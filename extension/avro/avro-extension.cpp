#include <iterator>

#include "duckdb.hpp"
#ifndef DUCKDB_AMALGAMATION
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#endif

#include "NodeImpl.hh"
#include "DataFile.hh"
#include "Generic.hh"
#include "avro-extension.hpp"

namespace duckdb {

namespace {

static vector<string> AvroGlob(FileSystem &fs, const string &glob) {
	auto files = fs.Glob(glob);
	if (files.empty()) {
		throw IOException("No files found that match the pattern \"%s\"", glob);
	}
	return files;
};

struct AvroReadBindData : public FunctionData {
	avro::DataFileReader<avro::GenericDatum> reader;
	const vector<string> files;
	bool top_record;

	explicit AvroReadBindData(vector<string> &&files)
	    : reader(files[0].data()), files(move(files)), top_record(false) {
	}

	static const avro::Node *NullableNode(const avro::Node *node)
	{
		if (node->leaves() == 2) {
			auto child0 = node->leafAt(0).get();
			auto child1 = node->leafAt(1).get();
			if (child0->type() == avro::AVRO_NULL) {
				return child1;
			}
			else if (child1->type() == avro::AVRO_NULL) {
				return child0;
			}
		}
		return nullptr;
	}

	static LogicalType ColumnType(const avro::Node *node) {
		switch (node->type()) {
		case avro::AVRO_INT:
			switch (node->logicalType().type()) {
			case avro::LogicalType::DATE:
				return LogicalType::DATE;
			case avro::LogicalType::TIME_MILLIS:
				return LogicalType::TIME;
			default:
				return LogicalType::INTEGER;
			}
		case avro::AVRO_LONG:
			switch (node->logicalType().type()) {
			case avro::LogicalType::TIME_MICROS:
				return LogicalType::TIME;
			case avro::LogicalType::TIMESTAMP_MILLIS:
				return LogicalType::TIMESTAMP_MS;
			case avro::LogicalType::TIMESTAMP_MICROS:
				return LogicalType::TIMESTAMP;
			default:
				return LogicalType::BIGINT;
			}
		case avro::AVRO_BOOL:
			return LogicalType::BOOLEAN;
		case avro::AVRO_FLOAT:
			return LogicalType::FLOAT;
		case avro::AVRO_DOUBLE:
			return LogicalType::DOUBLE;
		case avro::AVRO_STRING:
			return node->logicalType().type() == avro::LogicalType::UUID ? LogicalType::UUID : LogicalType::VARCHAR;
		case avro::AVRO_NULL:
			return LogicalType::SQLNULL;
		case avro::AVRO_BYTES:
		case avro::AVRO_FIXED:
			switch (auto ltype = node->logicalType(); ltype.type()) {
			case avro::LogicalType::DECIMAL:
				if (ltype.precision() < 1 || ltype.precision() > Decimal::MAX_WIDTH_DECIMAL ||
				    ltype.scale() < 0 || ltype.scale() > ltype.precision()) {
					return LogicalType::BLOB;
				}
				return LogicalType::DECIMAL(ltype.precision(), ltype.scale());
			case avro::LogicalType::DURATION:
				return LogicalType::INTERVAL;
			default:
				return LogicalType::BLOB;
			};
		case avro::AVRO_ENUM: {
			size_t count = node->names();
			Vector names(LogicalType::VARCHAR, count);
			auto p_names = FlatVector::GetData<string_t>(names);
			for (size_t i = 0; i < count; ++i) {
				p_names[i] = StringVector::AddStringOrBlob(names, node->nameAt(i));
			}
			return LogicalType::ENUM(node->name().simpleName(), names, count);
		}
		case avro::AVRO_ARRAY:
			return LogicalType::LIST(ColumnType(node->leafAt(0).get()));
		case avro::AVRO_MAP: {
			child_list_t<LogicalType> child_types;
			child_types.push_back({"key", LogicalType::LIST(LogicalType::VARCHAR)});
			child_types.push_back({"value", LogicalType::LIST(ColumnType(node->leafAt(1).get()))});
			return LogicalType::MAP(move(child_types));
		}
		case avro::AVRO_UNION: {
			auto pchild = NullableNode(node);
			return pchild ? ColumnType(pchild) : LogicalType::INVALID;
		}
		case avro::AVRO_RECORD: {
			child_list_t<LogicalType> child_types;
			size_t fields = node->leaves();
			for (size_t i = 0; i < fields; ++i) {
				child_types.push_back({node->nameAt(i), ColumnType(node->leafAt(i).get())});
			}
			return LogicalType::STRUCT(move(child_types));
		}
		case avro::AVRO_SYMBOLIC:
			return ColumnType(static_cast<const avro::NodeSymbolic *>(node)->getNode().get());
		default:
			return LogicalType::INVALID;
		}
	}

	void BindSchema(vector<LogicalType> &return_types, vector<string> &names) {
		const auto *root = reader.dataSchema().root().get();
		if (root->type() == avro::AVRO_UNION) {
			auto pchild = NullableNode(root);
			if (pchild) {
				root = pchild;
			}
		}
		if (root->type() == avro::AVRO_RECORD) {
			top_record = true;
			size_t fields = root->leaves();
			for (size_t i = 0; i < fields; ++i) {
				names.push_back(root->nameAt(i));
				return_types.push_back(ColumnType(root->leafAt(i).get()));
			}
		} else {
			if (root->hasName()) {
				names.push_back(root->name().simpleName());
			} else {
				names.push_back("data");
			}
			return_types.push_back(ColumnType(root));
		}
	}
};

struct AvroReadOperatorData : public FunctionOperatorData {
};

static unique_ptr<FunctionData> AvroScanBindInternal(ClientContext &context, vector<string> files,
                                                     vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_unique<AvroReadBindData>(move(files));
	result->BindSchema(return_types, names);
	return result;
}

static unique_ptr<FunctionData> AvroScanBind(ClientContext &context, vector<Value> &inputs,
                                             unordered_map<string, Value> &named_parameters,
                                             vector<LogicalType> &input_table_types,
                                             vector<string> &input_table_names,
                                             vector<LogicalType> &return_types, vector<string> &names) {
	FileSystem &fs = FileSystem::GetFileSystem(context);
	auto file_name = inputs[0].GetValue<string>();
	auto files = AvroGlob(fs, file_name);
	return AvroScanBindInternal(context, move(files), return_types, names);
}

static unique_ptr<FunctionData> AvroScanBindList(ClientContext &context, vector<Value> &inputs,
                                                 unordered_map<string, Value> &named_parameters,
                                                 vector<LogicalType> &input_table_types,
                                                 vector<string> &input_table_names,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	FileSystem &fs = FileSystem::GetFileSystem(context);
	vector<string> files;
	for (const auto &val : inputs[0].list_value) {
		auto glob_files = AvroGlob(fs, val.ToString());
		files.insert(files.end(), glob_files.begin(), glob_files.end());
	}
	if (files.empty()) {
		throw IOException("Avro reader needs at least one file to read");
	}
	return AvroScanBindInternal(context, move(files), return_types, names);
}

static unique_ptr<FunctionOperatorData> AvroScanInit(ClientContext &context, const FunctionData *bind_data_p,
                                                     const vector<column_t> &column_ids,
                                                     TableFilterCollection *filters) {
	auto &bind_data = (AvroReadBindData &)*bind_data_p;
	auto result = make_unique<AvroReadOperatorData>();
	return result;
}

static Value AvroReadField(const LogicalType &type, avro::GenericDatum &field) {
	auto field_type = field.type();
	switch(field_type) {
	case avro::AVRO_INT: {
		auto data = field.value<int32_t>();
		switch (field.logicalType().type()) {
		case avro::LogicalType::DATE:
			D_ASSERT(type.id() == LogicalTypeId::DATE);
			return Value::DATE(date_t(data));
		case avro::LogicalType::TIME_MILLIS:
			D_ASSERT(type.id() == LogicalTypeId::TIME);
			return Value::TIME(dtime_t((uint32_t)data * 1000LL));
		default:
			D_ASSERT(type.id() == LogicalTypeId::INTEGER);
			return data;
		}
	}
	case avro::AVRO_LONG: {
		auto data = field.value<int64_t>();
		switch (field.logicalType().type()) {
		case avro::LogicalType::TIME_MICROS:
			D_ASSERT(type.id() == LogicalTypeId::TIME);
			return Value::TIME(dtime_t(data));
		case avro::LogicalType::TIMESTAMP_MILLIS:
			D_ASSERT(type.id() == LogicalTypeId::TIMESTAMP_MS);
			return Value::TIMESTAMPMS(timestamp_t(data));
		case avro::LogicalType::TIMESTAMP_MICROS:
			D_ASSERT(type.id() == LogicalTypeId::TIMESTAMP);
			return Value::TIMESTAMP(timestamp_t(data));
		default:
			D_ASSERT(type.id() == LogicalTypeId::BIGINT);
			return data;
		}
	}
	case avro::AVRO_BOOL:
		D_ASSERT(type.id() == LogicalTypeId::BOOLEAN);
		return field.value<bool>();
	case avro::AVRO_FLOAT:
		D_ASSERT(type.id() == LogicalTypeId::FLOAT);
		return field.value<float>();
	case avro::AVRO_DOUBLE:
		D_ASSERT(type.id() == LogicalTypeId::DOUBLE);
		return field.value<double>();
	case avro::AVRO_STRING: {
		auto &data = field.value<string>();
		if (type.id() == LogicalTypeId::UUID)
			return Value::UUID(data);
		D_ASSERT(type.id() == LogicalTypeId::VARCHAR);
		return move(data);
	}
	case avro::AVRO_BYTES:
	case avro::AVRO_FIXED: {
		const auto &data = field_type == avro::AVRO_BYTES ? field.value<vector<uint8_t>>()
		                                                  : field.value<avro::GenericFixed>().value();
		switch (type.id()) {
		case LogicalTypeId::DECIMAL: {
			size_t upper_size = data.size();
			if (upper_size == 0) {
				return Value();
			}
			if (upper_size > 8) {
				upper_size -= 8;
			}
			int64_t result = (int8_t)data.front();
			for (size_t i = 1; i < upper_size; ++i) {
				result = result << 8 | data[i];
			}
			Value val(type);
			switch (type.InternalType()) {
			case PhysicalType::INT16:
				val.value_.smallint = (int16_t)result;
				break;
			case PhysicalType::INT32:
				val.value_.integer = (int32_t)result;
				break;
			case PhysicalType::INT64:
				val.value_.bigint = result;
				break;
			default:
				D_ASSERT(type.InternalType() == PhysicalType::INT128);
				val.value_.hugeint.upper = result;
				result = 0;
				for (size_t i = upper_size; i < data.size(); ++i) {
					result = result << 8 | data[i];
				}
				val.value_.hugeint.lower = (uint64_t)result;
				break;
			}
			val.is_null = false;
			return val;
		}
		case LogicalTypeId::INTERVAL:
			D_ASSERT(data.size() == 12);
			return Value::INTERVAL(*(int32_t*)data.data(), *(int32_t*)&data[4], *(uint32_t*)&data[8] * 1000LL);
		default:
			return Value::BLOB(data.data(), data.size());
		}
	}
	case avro::AVRO_ENUM:
		D_ASSERT(type.id() == LogicalTypeId::ENUM);
		return Value::ENUM(field.value<avro::GenericEnum>().value(), type);
	case avro::AVRO_ARRAY: {
		D_ASSERT(type.id() == LogicalTypeId::LIST);
		Value val(type);
		auto &child_type = ListType::GetChildType(type);
		for (auto &datum : field.value<avro::GenericArray>().value()) {
			val.list_value.push_back(AvroReadField(child_type, datum));
		}
		val.is_null = false;
		return val;
	}
	case avro::AVRO_MAP: {
		D_ASSERT(type.id() == LogicalTypeId::MAP);
		Value val(type);
		auto &child_types = StructType::GetChildTypes(type);
		auto &child_type = ListType::GetChildType(child_types[1].second);
		val.struct_value.emplace_back(child_types[0].second);
		val.struct_value.emplace_back(child_types[1].second);
		auto &keys = val.struct_value[0];
		auto &values = val.struct_value[1];
		for (auto &datum : field.value<avro::GenericMap>().value()) {
			keys.list_value.push_back(move(datum.first));
			values.list_value.push_back(AvroReadField(child_type, datum.second));
		}
		val.is_null = keys.is_null = values.is_null = false;
		return val;
	}
	case avro::AVRO_RECORD: {
		D_ASSERT(type.id() == LogicalTypeId::STRUCT);
		Value val(type);
		auto &child_types = StructType::GetChildTypes(type);
		auto &data = field.value<avro::GenericRecord>();
		D_ASSERT(data.fieldCount() == child_types.size());
		for (size_t i = 0; i < data.fieldCount(); ++i) {
			val.struct_value.push_back(AvroReadField(child_types[i].second, data.fieldAt(i)));
		}
		val.is_null = false;
		return val;
	}
	default:
		return Value();
	}
}

static void AvroScanImpl(ClientContext &context, const FunctionData *bind_data_p,
                         FunctionOperatorData *operator_state, DataChunk *input, DataChunk &output) {
	auto &bind_data = (AvroReadBindData &)*bind_data_p;
	auto &data = (AvroReadOperatorData &)*operator_state;

	auto &reader = bind_data.reader;
	avro::GenericDatum datum(reader.dataSchema());
	idx_t count = 0;
	while (reader.read(datum)) {
		if (datum.type() == avro::AVRO_NULL) {
			Value null;
			for (size_t i = 0; i < output.ColumnCount(); ++i) {
				output.SetValue(i, count, null);
			}
		} else if (bind_data.top_record) {
			D_ASSERT(datum.type() == avro::AVRO_RECORD);
			auto &r = datum.value<avro::GenericRecord>();
			D_ASSERT(output.ColumnCount() == r.fieldCount());
			for (size_t i = 0; i < output.ColumnCount(); ++i) {
				output.SetValue(i, count, AvroReadField(output.data[i].GetType(), r.fieldAt(i)));
			}
		} else {
			D_ASSERT(output.ColumnCount() == 1);
			output.SetValue(0, count, AvroReadField(output.data[0].GetType(), datum));
		}
		if (++count >= STANDARD_VECTOR_SIZE) {
			break;
		}
	}
	output.SetCardinality(count);
}

static unique_ptr<TableFunctionRef> AvroScanReplacement(const string &table_name, void *data) {
	if (!StringUtil::EndsWith(table_name, ".avro")) {
		return nullptr;
	}
	auto table_function = make_unique<TableFunctionRef>();
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(make_unique<ConstantExpression>(Value(table_name)));
	table_function->function = make_unique<FunctionExpression>("avro_scan", move(children));
	return table_function;
}


struct AvroSchemaBindData : public FunctionData {
	static constexpr const char *column_names[] = {
		"file_name",
		"id",
		"parent_id",
		"field",
		"namespace",
		"name",
		"type",
		"logical_type",
		"precision",
		"scale",
		"size",
		"doc"
	};
	const vector<LogicalType> column_types = {
		LogicalType::VARCHAR,   //  0 "file_name",
		LogicalType::UINTEGER,  //  1 "id",
		LogicalType::UINTEGER,  //  2 "parent_id",
		LogicalType::VARCHAR,   //  3 "field",
		LogicalType::VARCHAR,   //  4 "namespace",
		LogicalType::VARCHAR,   //  5 "name",
		LogicalType::VARCHAR,   //  6 "type",
		LogicalType::VARCHAR,   //  7 "logical_type",
		LogicalType::UINTEGER,  //  8 "precision",
		LogicalType::UINTEGER,  //  9 "scale",
		LogicalType::UBIGINT,   // 10 "size",
		LogicalType::VARCHAR    // 11 "doc",
	};
	vector<string> files;

	void BindSchema(vector<LogicalType> &return_types, vector<string> &names) {
		return_types.insert(return_types.end(), column_types.begin(), column_types.end());
		names.insert(names.end(), std::begin(column_names), std::end(column_names));
	}
};

struct AvroSchemaOperatorData : public FunctionOperatorData {
	idx_t file_index = 0;
	ChunkCollection collection;

	class FileLoader {
		static constexpr const char *logical_types[] = {
			"",
			"decimal",
			"date",
			"time-millis",
			"time-micros",
			"timestamp-millis",
			"timestamp-micros",
			"duration",
			"uuid"
		};

	public:
		FileLoader(const string &file_path, ChunkCollection &collection, const vector<LogicalType> &column_types)
		    : file_path(file_path), column_types(column_types), collection(collection),
		      chunk(make_unique<DataChunk>()), count(0), id(0) {
			chunk->Initialize(column_types);
		}

		void LoadFile() {
			avro::DataFileReader<avro::GenericDatum> reader(file_path.data());
			LoadNode(reader.dataSchema().root().get());
			if (count > 0) {
				chunk->SetCardinality(count);
				collection.Append(move(chunk));
			}
		}

	private:
		void LoadNode(const avro::Node *node, unsigned parent_id = 0, const string *field = nullptr) {
			if (count >= STANDARD_VECTOR_SIZE) {
				chunk->SetCardinality(count);
				collection.Append(move(chunk));
				chunk = make_unique<DataChunk>();
				chunk->Initialize(column_types);
				count = 0;
			}
			Value null;
			auto type = node->type();
			chunk->SetValue(0, count, file_path);  // file_name
			bool is_container = type == avro::AVRO_RECORD || type == avro::AVRO_UNION ||
			                    type == avro::AVRO_ARRAY || type == avro::AVRO_MAP;
			chunk->SetValue(1, count, is_container ? Value::UINTEGER(++id) : null);  // id
			chunk->SetValue(2, count, parent_id == 0 ? null : Value::UINTEGER(parent_id));  // parent_id
			chunk->SetValue(3, count, field ? Value(*field) : null);  // field
			bool has_name = node->hasName();
			chunk->SetValue(4, count, has_name ? Value(node->name().ns()) : null);  // namespace
			chunk->SetValue(5, count, has_name ? Value(node->name().simpleName()) : null);  // name
			chunk->SetValue(6, count, avro::toString(node->type()));  // type
			Value v_ltype, v_precision, v_scale;
			if (type <= avro::AVRO_NULL || type == avro::AVRO_FIXED) {
				auto ltype = node->logicalType();
				if (ltype.type() != avro::LogicalType::NONE) {
					v_ltype = logical_types[ltype.type()];  // logical_type
					if (ltype.type() == avro::LogicalType::DECIMAL) {
						v_precision = Value::UINTEGER(ltype.precision());
						v_scale = Value::UINTEGER(ltype.scale());
					}
				}
			}
			chunk->SetValue(7, count, move(v_ltype));  // logical_type
			chunk->SetValue(8, count, move(v_precision));  // precision
			chunk->SetValue(9, count, move(v_scale));  // scale
			chunk->SetValue(10, count, type == avro::AVRO_FIXED ? Value::UBIGINT(node->fixedSize()) : null); // size
			chunk->SetValue(11, count, node->getDoc());  // doc
			++count;

			switch (type) {
			case avro::AVRO_RECORD: {
				size_t fields = node->leaves();
				unsigned this_id = id;
				for (size_t i = 0; i < fields; ++i) {
					LoadNode(node->leafAt(i).get(), this_id, &node->nameAt(i));
				}
				break;
			}
			case avro::AVRO_UNION: {
				size_t fields = node->leaves();
				unsigned this_id = id;
				for (size_t i = 0; i < fields; ++i) {
					LoadNode(node->leafAt(i).get(), this_id);
				}
				break;
			}
			case avro::AVRO_ARRAY:
				LoadNode(node->leafAt(0).get(), id);
				break;
			case avro::AVRO_MAP:
				LoadNode(node->leafAt(1).get(), id);
				break;
			}
		}

		const string &file_path;
		const vector<LogicalType> &column_types;
		ChunkCollection &collection;
		unique_ptr<DataChunk> chunk;
		idx_t count;
		unsigned id;
	};

	bool LoadNextSchema(ClientContext &context, const AvroSchemaBindData &bind_data) {
		if (file_index >= bind_data.files.size()) {
			return false;
		}
		collection.Reset();
		FileLoader(bind_data.files[file_index], collection, bind_data.column_types).LoadFile();
		++file_index;
		return true;
	}
};

static unique_ptr<FunctionData> AvroSchemaBind(ClientContext &context, vector<Value> &inputs,
                                               unordered_map<string, Value> &named_parameters,
                                               vector<LogicalType> &input_table_types,
                                               vector<string> &input_table_names, vector<LogicalType> &return_types,
                                               vector<string> &names) {
	auto result = make_unique<AvroSchemaBindData>();
	result->BindSchema(return_types, names);
	FileSystem &fs = FileSystem::GetFileSystem(context);
	auto file_name = inputs[0].GetValue<string>();
	result->files = AvroGlob(fs, file_name);
	return result;
}

static unique_ptr<FunctionOperatorData> AvroSchemaInit(ClientContext &context, const FunctionData *bind_data_p,
                                                       const vector<column_t> &column_ids,
                                                       TableFilterCollection *filters) {
	auto &bind_data = (const AvroSchemaBindData &)*bind_data_p;
	D_ASSERT(!bind_data.files.empty());
	return make_unique<AvroSchemaOperatorData>();
}

static void AvroSchemaImpl(ClientContext &context, const FunctionData *bind_data_p,
                           FunctionOperatorData *operator_state, DataChunk *input, DataChunk &output) {
	auto &bind_data = (const AvroSchemaBindData &)*bind_data_p;
	auto &data = (AvroSchemaOperatorData &)*operator_state;
	auto chunk = data.collection.Fetch();
	while (!chunk) {
		if (!data.LoadNextSchema(context, bind_data)) {
			// no files remaining: done
			return;
		}
		chunk = data.collection.Fetch();
	}
	output.Move(*chunk);
}

} // namespace

void AvroExtension::Load(DuckDB &db) {
	Connection con(db);
	con.BeginTransaction();
	auto &context = *con.context;
	auto &catalog = Catalog::GetCatalog(context);

	TableFunctionSet scan_set("avro_scan");
	scan_set.AddFunction(
	    TableFunction({LogicalType::VARCHAR}, AvroScanImpl, AvroScanBind, AvroScanInit,
	                  nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
	                  nullptr, false, false, nullptr));
	scan_set.AddFunction(
	    TableFunction({LogicalType::LIST(LogicalType::VARCHAR)}, AvroScanImpl, AvroScanBindList, AvroScanInit,
	                  nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
	                  nullptr, false, false, nullptr));
	CreateTableFunctionInfo scan_info(move(scan_set));
	catalog.CreateTableFunction(context, &scan_info);

	CreateTableFunctionInfo schema_info(
	    TableFunction("avro_schema", {LogicalType::VARCHAR}, AvroSchemaImpl, AvroSchemaBind, AvroSchemaInit,
	                  nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
	                  nullptr, false, false, nullptr));
	catalog.CreateTableFunction(context, &schema_info);
	con.Commit();

	auto &config = DBConfig::GetConfig(*db.instance);
	config.replacement_scans.emplace_back(AvroScanReplacement);
}

string AvroExtension::Name() {
	return "avro";
}

} // namespace duckdb
