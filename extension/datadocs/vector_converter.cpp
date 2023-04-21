#include <cmath>
#include <unordered_map>

#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/operator/string_cast.hpp"

#include "vector_converter.hpp"
#include "utility.h"
#include "type_conv.h"

namespace duckdb {

struct VectorCnvWriter::Converters {
	struct Base {
		static int SetString(VectorCnvWriter &writer, string_t v) { return -1; }
		static int SetInt64(VectorCnvWriter &writer, int64_t v) { return -1; }
		static int SetBool(VectorCnvWriter &writer, bool v) { return -1; }
		static int SetDouble(VectorCnvWriter &writer, double v) { return -1; }
		static int SetExcelDate(VectorCnvWriter &writer, double v) { return -1; }
	};

	struct VARCHAR {
		static int SetString(VectorCnvWriter &writer, string_t v) {
			writer.SetString(v);
			return 0;
		}

		static int SetInt64(VectorCnvWriter &writer, int64_t v) {
			writer.SetVectorString(StringCast::Operation(v, writer.vec));
			return 0;
		}

		static int SetBool(VectorCnvWriter &writer, bool v) {
			writer.SetVectorString(StringCast::Operation(v, writer.vec));
			return 0;
		}

		static int SetDouble(VectorCnvWriter &writer, double v) {
			writer.SetVectorString(StringCast::Operation(v, writer.vec));
			return 0;
		}

		static int SetExcelDate(VectorCnvWriter &writer, double v) {
			int32_t date[3], time[4];
			idx_t date_length, year_length, time_length, length;
			bool add_bc;
			char micro_buffer[6];
			bool have_date = v >= 1.0;
			bool have_time = v != std::trunc(v);
			if (have_date)
			{
				Date::Convert(date_t((int)v - 25569), date[0], date[1], date[2]);
				length = date_length = have_time + DateToStringCast::Length(date, year_length, add_bc);
			} else {
				length = 0;
			}
			if (have_time)
			{
				long t = std::lround((v - int(v)) * 86400.0);
				time[0] = t / 3600;
				t %= 3600;
				time[1] = t / 60;
				time[2] = t % 60;
				time[3] = 0;
				time_length = TimeToStringCast::Length(time, micro_buffer);
				length += time_length;
			}
			string_t &result = writer.ReserveString(length);
			char *data = result.GetDataWriteable();
			if (have_date) {
				DateToStringCast::Format(data, date, year_length, add_bc);
				data += date_length;
				if (have_time) {
					data[-1] = ' ';
				}
			}
			if (have_time) {
				TimeToStringCast::Format(data, time_length, time, micro_buffer);
			}
			result.Finalize();
			return 0;
		}
	};

	struct BOOL : Base {
		static inline const std::unordered_map<string, bool> bool_dict {
			{"0", false}, {"1", true},
			{"false", false}, {"False", false}, {"FALSE", false}, {"true", true}, {"True", true}, {"TRUE", true},
			{"n", false}, {"N", false}, {"no", false}, {"No", false}, {"NO", false},
			{"y", true}, {"Y", true}, {"yes", true}, {"Yes", true}, {"YES", true}
		};

		static int SetString(VectorCnvWriter &writer, string_t v) {
			if (v.GetSize() == 0) {
				return 1;
			}
			auto it = bool_dict.find(string(v));
			if (it == bool_dict.end())
				return -1;
			writer.Set(it->second);
			return 0;
		}

		static int SetInt64(VectorCnvWriter &writer, int64_t v) {
			if (v != 0 && v != 1) {
				return -1;
			}
			writer.Set((bool)v);
			return 0;
		}

		static int SetBool(VectorCnvWriter &writer, bool v) {
			writer.Set(v);
			return 0;
		}
	};

	struct BIGINT : Base {
		static int SetString(VectorCnvWriter &writer, string_t v) {
			if (v.GetSize() == 0) {
				return 1;
			}
			int64_t result;
			if (!TryCast::Operation(v, result, true)) {
				return -1;
			}
			writer.Set(result);
			return 0;
		}

		static int SetInt64(VectorCnvWriter &writer, int64_t v) {
			writer.Set(v);
			return 0;
		}

		static int SetBool(VectorCnvWriter &writer, bool v) {
			writer.Set((int64_t)v);
			return 0;
		}

		static int SetDouble(VectorCnvWriter &writer, double v) {
			if (!Ingest::is_integer(v)) {
				return -1;
			}
			writer.Set((int64_t)v);
			return 0;
		}
	};

	struct DOUBLE : Base {
		static int SetString(VectorCnvWriter &writer, string_t v) {
			if (v.GetSize() == 0) {
				return 1;
			}
			double result;
			if (!TryCast::Operation(v, result, true)) {
				return -1;
			}
			writer.Set(result);
			return 0;
		}

		static int SetInt64(VectorCnvWriter &writer, int64_t v) {
			writer.Set((double)v);
			return 0;
		}

		static int SetBool(VectorCnvWriter &writer, bool v) {
			writer.Set((double)v);
			return 0;
		}

		static int SetDouble(VectorCnvWriter &writer, double v) {
			writer.Set(v);
			return 0;
		}
	};

	struct DATE : Base {
		static int SetString(VectorCnvWriter &writer, string_t v) {
			if (v.GetSize() == 0) {
				return 1;
			}
			double t;
			if (!Ingest::strptime(string(v), writer.cnv.format, t)) {
				return -1;
			}
			return SetExcelDate(writer, t);
		}

		static int SetExcelDate(VectorCnvWriter &writer, double v) {
			writer.Set((int32_t)v - 25569);
			return 0;
		}
	};

	struct TIME : Base {
		static int SetString(VectorCnvWriter &writer, string_t v) {
			if (v.GetSize() == 0) {
				return 1;
			}
			double t;
			if (!Ingest::strptime(string(v), writer.cnv.format, t)) {
				return -1;
			}
			return SetExcelDate(writer, t);
		}

		static int SetExcelDate(VectorCnvWriter &writer, double v) {
			writer.Set(int64_t(v * Interval::MICROS_PER_DAY));
			return 0;
		}
	};

	struct TIMESTAMP : Base {
		static int SetString(VectorCnvWriter &writer, string_t v) {
			if (v.GetSize() == 0) {
				return 1;
			}
			double t;
			if (!Ingest::strptime(string(v), writer.cnv.format, t)) {
				return -1;
			}
			return SetExcelDate(writer, t);
		}

		static int SetExcelDate(VectorCnvWriter &writer, double v) {
			writer.Set(int64_t((v - 25569) * Interval::MICROS_PER_DAY));
			return 0;
		}
	};

	struct BLOB : Base {
		static inline const uint8_t _base64tbl[256] = {
			 0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
			 0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
			 0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0, 62, 63, 62, 62, 63, // +,-./
			52, 53, 54, 55, 56, 57, 58, 59, 60, 61,  0,  0,  0,  0,  0,  0, // 0-9
			 0,  0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14, // a-o
			15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25,  0,  0,  0,  0, 63, // p-z _
			 0, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, // A-O
			41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51                      // P-Z
		};

		static int SetString(VectorCnvWriter &writer, string_t v) {
			idx_t size = v.GetSize();
			if (size == 0) {
				return 1;
			}
			if (writer.cnv.format == "base64") {
				const unsigned char *s = (const unsigned char*)v.GetDataUnsafe();
				while (s[size - 1] == '=')
					--size;
				size_t len_tail = size % 4;
				if (len_tail == 1)
					return -1;
				size -= len_tail;
				char *res = writer.ReserveString(size / 4 * 3 + (len_tail > 0 ? len_tail-1 : 0)).GetDataWriteable();
				size_t i_write = 0;
				for (size_t i_read = 0; i_read < size; i_read += 4)
				{
					uint32_t n = _base64tbl[s[i_read]] << 18 | _base64tbl[s[i_read + 1]] << 12 | _base64tbl[s[i_read + 2]] << 6 | _base64tbl[s[i_read + 3]];
					res[i_write++] = n >> 16;
					res[i_write++] = (n >> 8) & 0xFF;
					res[i_write++] = n & 0xFF;
				}
				if (len_tail > 0)
				{
					uint32_t n = _base64tbl[s[size]] << 18 | _base64tbl[s[size + 1]] << 12;
					res[i_write++] = n >> 16;
					if (len_tail == 3)
					{
						n |= _base64tbl[s[size + 2]] << 6;
						res[i_write] = (n >> 8) & 0xFF;
					}
				}
			}
			else
			{
				if (size % 2 != 0)
					return -1;
				const char *s = v.GetDataUnsafe();
				size_t i_read = (s[1] == 'x' || s[1] == 'X') && s[0] == '0' ? 2 : 0;
				char *res = writer.ReserveString((size - i_read) / 2).GetDataWriteable();
				if (!Ingest::string0x_to_bytes(s+i_read, s+size, res))
					return -1;
			}
			return 0;
		}
	};

	struct NUMERIC : Base {};
	struct GEO : Base {};
	struct STRUCT : Base {};
	struct VARIANT : Base {};
};

struct VectorConverter::VTable {
	int (*SetString)(VectorCnvWriter &writer, string_t v);
	int (*SetInt64)(VectorCnvWriter &writer, int64_t v);
	int (*SetBool)(VectorCnvWriter &writer, bool v);
	int (*SetDouble)(VectorCnvWriter &writer, double v);
	int (*SetExcelDate)(VectorCnvWriter &writer, double v);
};

#define IMPL_VTBL(TYPE)                                                                                                \
const VectorConverter::VTable VectorConverter::VTBL_##TYPE = {                                                         \
	VectorCnvWriter::Converters::TYPE::SetString,                                                                    \
	VectorCnvWriter::Converters::TYPE::SetInt64,                                                                     \
	VectorCnvWriter::Converters::TYPE::SetBool,                                                                      \
	VectorCnvWriter::Converters::TYPE::SetDouble,                                                                    \
	VectorCnvWriter::Converters::TYPE::SetExcelDate,                                                                 \
};

IMPL_VTBL(VARCHAR)
IMPL_VTBL(BOOL)
IMPL_VTBL(BIGINT)
IMPL_VTBL(DOUBLE)
IMPL_VTBL(DATE)
IMPL_VTBL(TIME)
IMPL_VTBL(TIMESTAMP)
IMPL_VTBL(BLOB)
IMPL_VTBL(NUMERIC)
IMPL_VTBL(GEO)
IMPL_VTBL(STRUCT)
IMPL_VTBL(VARIANT)

int VectorCnvWriter::Write(string_t v) { return cnv.vtbl.SetString(*this, v); }
int VectorCnvWriter::Write(int64_t v) { return cnv.vtbl.SetInt64(*this, v); }
int VectorCnvWriter::Write(bool v) { return cnv.vtbl.SetBool(*this, v); }
int VectorCnvWriter::Write(double v) { return cnv.vtbl.SetDouble(*this, v); }
int VectorCnvWriter::WriteExcelDate(double v) { return cnv.vtbl.SetExcelDate(*this, v); }

VectorCnvWriter VectorRow::operator[](size_t index) noexcept {
	D_ASSERT(index < columns.size());
	return {output.data[index], i_row, columns[index]};
}

void VectorRow::WriteError(size_t i_col, const string &msg) {
}

} // namespace duckdb
