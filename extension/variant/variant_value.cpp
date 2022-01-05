#include "duckdb.hpp"

#include "variant_value.hpp"

namespace duckdb {

typedef uint32_t list_index_type;
constexpr idx_t list_index_size = sizeof(list_index_type);

static Value BufferToBlob(LogicalTypeId type_id, const void *data, idx_t size) {
	Value result(LogicalType::BLOB);
	result.is_null = false;
	result.str_value.reserve(size + 1);
	result.str_value.push_back(static_cast<uint8_t>(type_id));
	result.str_value.append((const char *)data, size);
	return result;
}

#define FIXED_VARIANT(TYPE, TYPE_ID) \
template <> \
Value DUCKDB_API Variant(TYPE value) { \
	return BufferToBlob(TYPE_ID, &value, sizeof(value)); \
} \

FIXED_VARIANT(bool, LogicalTypeId::BOOLEAN)
FIXED_VARIANT(int8_t, LogicalTypeId::TINYINT)
FIXED_VARIANT(uint8_t, LogicalTypeId::UTINYINT)
FIXED_VARIANT(int16_t, LogicalTypeId::SMALLINT)
FIXED_VARIANT(uint16_t, LogicalTypeId::USMALLINT)
FIXED_VARIANT(int32_t, LogicalTypeId::INTEGER)
FIXED_VARIANT(uint32_t, LogicalTypeId::UINTEGER)
FIXED_VARIANT(int64_t, LogicalTypeId::BIGINT)
FIXED_VARIANT(uint64_t, LogicalTypeId::UBIGINT)
FIXED_VARIANT(hugeint_t, LogicalTypeId::HUGEINT)
FIXED_VARIANT(float, LogicalTypeId::FLOAT)
FIXED_VARIANT(double, LogicalTypeId::DOUBLE)
FIXED_VARIANT(date_t, LogicalTypeId::DATE)
FIXED_VARIANT(dtime_t, LogicalTypeId::TIME)
FIXED_VARIANT(timestamp_t, LogicalTypeId::TIMESTAMP)
FIXED_VARIANT(interval_t, LogicalTypeId::INTERVAL)

Value DUCKDB_API Variant(const char *value) {
	return BufferToBlob(LogicalTypeId::VARCHAR, value, strlen(value));
}

Value DUCKDB_API Variant(const string &value) {
	return BufferToBlob(LogicalTypeId::VARCHAR, value.data(), value.size());
}

static void TypeToBlob(const LogicalType &type, string &result) {
	LogicalTypeId type_id = type.id();
	result.push_back(static_cast<uint8_t>(type_id == LogicalTypeId::ENUM ? LogicalTypeId::VARCHAR : type_id));
	switch (type_id) {
	case LogicalTypeId::DECIMAL:
		result.push_back(DecimalType::GetWidth(type));
		result.push_back(DecimalType::GetScale(type));
		break;
	case LogicalTypeId::LIST:
		TypeToBlob(ListType::GetChildType(type), result);
		break;
	case LogicalTypeId::STRUCT: {
		auto &list = StructType::GetChildTypes(type);
		idx_t offsets = result.size();
		result.resize(offsets + MaxValue(list.size(), (idx_t)1) * list_index_size);
		for (idx_t i = 0; i < list.size(); ++i) {
			((list_index_type *)&result[offsets])[i] = list_index_type(result.size() - offsets);
			auto &v = list[i];
			list_index_type size = (list_index_type)v.first.size();
			result.append((const char *)&size, list_index_size);
			result += v.first;
			TypeToBlob(v.second, result);
		}
		break;
	}
	}
}

static idx_t TypeSize(const LogicalType &type) {
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
		return sizeof(bool);
	case PhysicalType::INT8:
	case PhysicalType::UINT8:
		return 1;
	case PhysicalType::INT16:
	case PhysicalType::UINT16:
		return 2;
	case PhysicalType::INT32:
	case PhysicalType::UINT32:
		return 4;
	case PhysicalType::INT64:
	case PhysicalType::UINT64:
		return 8;
	case PhysicalType::INT128:
		return sizeof(hugeint_t);
	case PhysicalType::FLOAT:
		return sizeof(float);
	case PhysicalType::DOUBLE:
		return sizeof(double);
	case PhysicalType::INTERVAL:
		return sizeof(interval_t);
	}
	return 0;
}

static void ValueToBlob(const Value &value, string &result) {
	auto &type = value.type();
	switch (type.InternalType()) {

	case PhysicalType::VARCHAR:
		result += value.str_value;
		return;

	case PhysicalType::LIST: {
		const auto &list = value.list_value;
		list_index_type list_size = (list_index_type)list.size();
		if (list_size == 0) {
			return;
		}
		result.append((const char *)&list_size, list_index_size);
		idx_t bitmap = result.size();
		result.resize(bitmap + (list.size() + 7) / 8);
		auto &child_type = ListType::GetChildType(type);
		idx_t child_size = TypeSize(child_type);
		if (child_size != 0 && child_type.id() != LogicalTypeId::ENUM) {
			result.reserve(result.size() + list.size() * child_size);
			for (idx_t i = 0; i < list.size(); ++i) {
				auto &v = list[i];
				if (v.is_null) {
					result[bitmap + i / 8] |= 1 << (i % 8);
				} else {
					D_ASSERT(v.type() == child_type);
				}
				result.append((const char *)&v.value_, child_size);
			}
			return;
		}
		idx_t offsets = result.size();
		idx_t offsets_size = (list.size() + 1) * list_index_size;
		result.resize(offsets + offsets_size);
		*(list_index_type *)&result[offsets] = (list_index_type)offsets_size;
		bool is_any = child_type.id() == LogicalTypeId::ANY;
		for (idx_t i = 0; i < list.size(); ++i) {
			auto &v = list[i];
			if (v.is_null) {
				result[bitmap + i / 8] |= 1 << i % 8;
			} else {
				if (is_any) {
					TypeToBlob(v.type(), result);
				} else {
					D_ASSERT(v.type() == child_type);
				}
				ValueToBlob(v, result);
			}
			((list_index_type *)&result[offsets])[i+1] = list_index_type(result.size() - offsets);
		}
		return;
	}

	case PhysicalType::STRUCT: {
		const auto &list = value.struct_value;
		if (list.empty()) {
			return;
		}
		idx_t bitmap = result.size();
		result.resize(bitmap + (list.size() + 7) / 8);
		idx_t offsets = result.size();
		idx_t offsets_size = (list.size() + 1) * list_index_size;
		result.resize(offsets + offsets_size);
		*(list_index_type *)&result[offsets] = (list_index_type)offsets_size;
		auto &child_types = StructType::GetChildTypes(type);
		for (idx_t i = 0; i < list.size(); ++i) {
			auto &v = list[i];
			if (v.is_null) {
				result[bitmap + i / 8] |= 1 << i % 8;
			} else {
				if (child_types[i].second.id() == LogicalTypeId::ANY) {
					TypeToBlob(v.type(), result);
				} else {
					D_ASSERT(v.type() == child_types[i].second);
				}
				ValueToBlob(v, result);
			}
			((list_index_type *)&result[offsets])[i+1] = list_index_type(result.size() - offsets);
		}
		return;
	}
	}

	if (type.id() == LogicalTypeId::ENUM) {
		result += value.ToString();
		return;
	}

	idx_t size = TypeSize(type);
	if (size == 0) {
		throw InvalidTypeException(type, "Cannot encode type as variant");
	}
	result.append((const char *)&value.value_, size);
}

Value DUCKDB_API Variant(const Value &value) {
	Value result(LogicalType::BLOB);
	if (value.is_null) {
		return result;
	}
	result.is_null = false;
	TypeToBlob(value.type(), result.str_value);
	ValueToBlob(value, result.str_value);
	return result;
}

[[noreturn]]
static void BadVariant() {
	throw InvalidInputException("Invalid Variant value");
}

static LogicalType BlobToType(const char *&begin, const char *end) {
	if (begin >= end) {
		BadVariant();
	}
	LogicalTypeId type_id = static_cast<LogicalTypeId>(*begin++);
	switch (type_id) {
	case LogicalTypeId::DECIMAL: {
		if (begin + 2 > end) {
			BadVariant();
		}
		uint8_t width = begin[0];
		uint8_t scale = begin[1];
		begin += 2;
		return LogicalType::DECIMAL(width, scale);
	}
	case LogicalTypeId::LIST:
		return LogicalType::LIST(BlobToType(begin, end));
	case LogicalTypeId::STRUCT: {
		if (begin + list_index_size > end) {
			BadVariant();
		}
		const list_index_type *offsets = (const list_index_type *)begin;
		const char *start = begin;
		idx_t list_size = *offsets / list_index_size;
		child_list_t<LogicalType> child_types;
		if (list_size == 0) {
			begin += 4;
		} else {
			child_types.reserve(list_size);
			for (idx_t i = 0; i < list_size; ++i) {
				begin = start + offsets[i];
				if (begin + list_index_size > end) {
					BadVariant();
				}
				list_index_type key_size = *(const list_index_type *)begin;
				begin += list_index_size;
				if (begin + key_size > end) {
					BadVariant();
				}
				string key = string(begin, key_size);
				begin += key_size;
				child_types.push_back({move(key), BlobToType(begin, end)});
			}
		}
		return LogicalType::STRUCT(move(child_types));
	}
	}
	return type_id;
}

static void BlobToValue(const char *begin, const char *end, Value &result) {
	auto &type = result.type();
	result.is_null = false;
	D_ASSERT(begin <= end);
	idx_t blob_size = end - begin;

	switch(type.InternalType()) {

	case PhysicalType::VARCHAR:
		result.str_value = string(begin, blob_size);
		return;

	case PhysicalType::LIST: {
		if (blob_size == 0) {
			return;
		}
		if (blob_size < list_index_size) {
			BadVariant();
		}
		idx_t list_size = *(const list_index_type *)begin;
		auto &list = result.list_value;
		list.reserve(list_size);
		auto &child_type = ListType::GetChildType(type);
		idx_t child_size = TypeSize(child_type);
		const char *bitmap = begin += list_index_size;
		begin += (list_size + 7) / 8;
		if (child_size != 0) {
			if (begin + list_size * child_size != end) {
				BadVariant();
			}
			for (idx_t i = 0; i < list_size; ++i) {
				list.push_back(Value(child_type));
				if (!(bitmap[i / 8] & (1 << i % 8))) {
					auto &v = list.back();
					v.is_null = false;
					memmove(&v.value_, begin, child_size);
				}
				begin += child_size;
			}
			return;
		}
		const list_index_type *offsets = (const list_index_type *)begin;
		const char *start = begin;
		if (start + (list_size + 1) * list_index_size > end) {
			BadVariant();
		}
		bool is_any = child_type.id() == LogicalTypeId::ANY;
		for (idx_t i = 0; i < list_size; ++i) {
			if (bitmap[i / 8] & (1 << i % 8)) {
				list.push_back(is_any ? Value() : Value(child_type));
			} else {
				begin = start + offsets[i];
				const char *v_end = start + offsets[i + 1];
				if (v_end > end || begin > end) {
					BadVariant();
				}
				if (is_any) {
					list.push_back(Value(BlobToType(begin, v_end)));
				} else {
					list.push_back(Value(child_type));
				}
				BlobToValue(begin, v_end, list.back());
			}
		}
		return;
	}

	case PhysicalType::STRUCT: {
		if (blob_size == 0) {
			return;
		}
		auto &child_types = StructType::GetChildTypes(type);
		idx_t list_size = child_types.size();
		auto &list = result.struct_value;
		list.reserve(list_size);
		const char *bitmap = begin;
		begin += (list_size + 7) / 8;
		const list_index_type *offsets = (const list_index_type *)begin;
		const char *start = begin;
		if (start + (list_size + 1) * list_index_size > end) {
			BadVariant();
		}
		for (idx_t i = 0; i < list_size; ++i) {
			auto &child_type = child_types[i].second;
			if (bitmap[i / 8] & (1 << i % 8)) {
				list.push_back(child_type.id() == LogicalTypeId::ANY ? Value() : Value(child_type));
			} else {
				begin = start + offsets[i];
				const char *v_end = start + offsets[i + 1];
				if (v_end > end || begin > end) {
					BadVariant();
				}
				if (child_type.id() == LogicalTypeId::ANY) {
					list.push_back(Value(BlobToType(begin, v_end)));
				} else {
					list.push_back(Value(child_type));
				}
				BlobToValue(begin, v_end, list.back());
			}
		}
		return;
	}

	default:
		if (blob_size != TypeSize(type) || blob_size == 0) {
			BadVariant();
		}
		memcpy(&result.value_, begin, blob_size);
		return;
	}
}

Value DUCKDB_API FromVariant(const Value &value) {
	if (value.is_null) {
		return Value();
	}
	if (value.type().id() != LogicalTypeId::BLOB) {
		throw InvalidTypeException(value.type(), "Variant requires BLOB type");
	}
	const char *begin = value.str_value.data();
	const char *end = begin + value.str_value.size();
	Value result = Value(BlobToType(begin, end));
	BlobToValue(begin, end, result);
	return result;
}

} // namespace duckdb
