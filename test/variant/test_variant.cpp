//#define GEN_VARIANT
#include <catch.hpp>
#include <test_helpers.hpp>

#include <duckdb/catalog/catalog_entry/type_catalog_entry.hpp>
#include <duckdb/common/types/date.hpp>
#include <duckdb/common/types/time.hpp>
#include <duckdb/common/types/timestamp.hpp>
#include <duckdb/common/types/hash.hpp>

#include <variant-extension.hpp>
#include <variant_value.hpp>

using namespace duckdb;

#ifdef GEN_VARIANT
#include <fstream>
#define GEN(v)  of << "blob = Value::BLOB(R\"'(" << v << ")'\");\nREQUIRE(" #v " == blob);\n"
#define GENV(v) of << "blob = Value::BLOB(R\"'(" << Variant(v) << ")'\");\nREQUIRE(Variant(" #v ") == blob);\n"
#endif

vector<Value> &struct_value(const Value &v) {
	return const_cast<vector<Value> &>(StructValue::GetChildren(v));
}

// clang-format off

TEST_CASE("Test variant", "[variant]") {
	DuckDB db(nullptr);
	db.LoadExtension<VariantExtension>();
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TYPE enum_type AS ENUM ('one', 'two', 'three');"));
	auto enum_type_catalog = (TypeCatalogEntry *)db.instance->GetCatalog().GetEntry(
	    *con.context, CatalogType::TYPE_ENTRY, "", "enum_type", true);
	const LogicalType &tp_enum = enum_type_catalog->user_type;

	hugeint_t huge_val;
	huge_val.upper = 0x8000000000000000;
	huge_val.lower = 1;
	timestamp_t ts = Timestamp::FromString("2021-12-31 16:54:59");
	vector<Value> vts {Value::TIMESTAMP(ts), nullptr, Value::TIMESTAMP(ts + 1e13)};

	Value st = Value::STRUCT({
		{"bool", Value::BOOLEAN(true)},
		{"string", "hello"},
		{"a list", Value::LIST(LogicalType::TINYINT, {0, nullptr, 127})},
		{"null", Value()},
		{"another list", Value::LIST(LogicalType::VARCHAR, {})},
		{"empty_nested", Value::STRUCT({})},
		{"bool or string", Value(LogicalTypeId::ANY)},
		{"nested", Value::STRUCT({{"child1", 123}, {"child2", "a string"}})}
	});
	struct_value(st)[6] = Value::BOOLEAN(true);
	Value st2 = st;
	struct_value(st2) = {
		Value::BOOLEAN(false),
		"goodbye",
		Value::LIST(LogicalType::TINYINT, {}),
		Value(),
		Value::LIST(LogicalType::VARCHAR, {"one", "two"}),
		Value::STRUCT({}),
		"not boolean",
		Value::STRUCT({{"child1", 456}, {"child2", "also a string"}})
	};
	Value st_null = st;
	struct_value(st_null).clear();
	struct_value(st_null).resize(struct_value(st).size());

	Value any_list = Value::EMPTYLIST(LogicalTypeId::ANY);
	const_cast<vector<Value> &>(ListValue::GetChildren(any_list)) = {1, "a string", nullptr, Value::DECIMAL(INT64_C(314159265), 9, 8), st, Value::STRUCT({}),
		Value::LIST(LogicalType::SMALLINT, {1, 2, 3, nullptr}),
		Value::LIST(LogicalType::VARCHAR, {"a", "bbb", "cdefg", nullptr}),
	};

	Value blob;

#ifndef GEN_VARIANT

	REQUIRE(Variant(Value()).IsNull());
#include "test_variant.inc"

#else
	std::ofstream of("test/variant/test_variant.inc");
	GEN(Variant(false));
	GEN(Variant<int8_t>(-120));
	GEN(Variant<uint8_t>(254));
	GEN(Variant<int16_t>(-32000));
	GEN(Variant<uint16_t>(65000));
	GEN(Variant<int32_t>(-65000));
	GEN(Variant<uint32_t>(0x80000001));
	GEN(Variant<int64_t>(-2));
	GEN(Variant<uint64_t>(0x8000FFFF0000F001));
	GEN(Variant(huge_val));
	GEN(Variant(3.14f));
	GEN(Variant(3.14));
	GEN(Variant(Date::FromDate(2021, 12, 31)));
	GEN(Variant(Time::FromTime(16, 54, 59)));
	GEN(Variant(ts));
	GEN(Variant(Interval::GetDifference(ts, Timestamp::FromString("1956-01-17 10:05:01"))));
	GEN(Variant(""));
	GEN(Variant("str"));
	GEN(Variant(string("Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. ")));

	GENV(Value::BOOLEAN(true));
	GENV(Value::TINYINT(-120));
	GENV(Value::UTINYINT(254));
	GENV(Value::SMALLINT(-32000));
	GENV(Value::USMALLINT(65000));
	GENV(Value::INTEGER(-65000));
	GENV(Value::UINTEGER(0x80000001));
	GENV(Value::BIGINT(-2));
	GENV(Value::UBIGINT(0x8000FFFF0000F001));
	GENV(Value::HUGEINT(huge_val));
	GENV(Value::FLOAT(3.14f));
	GENV(Value::DOUBLE(3.14));
	GENV(Value::DATE(2021, 12, 31));
	GENV(Value::TIME(16, 54, 59, 999999));
	GENV(Value::TIMETZ(Time::FromTime(16, 54, 59)));
	GENV(Value::TIMESTAMP(ts));
	GENV(Value::TIMESTAMPTZ(ts));
	GENV(Value::TIMESTAMPSEC(timestamp_t(1000)));
	GENV(Value::TIMESTAMPMS(timestamp_t(1000000)));
	GENV(Value::TIMESTAMPNS(timestamp_t(1000000000000)));
	GENV(Value::INTERVAL(13, 18, 10000000000));
	GENV(Value::HASH(Hash("Hash me")));
	GENV(Value::UUID("6b6542ea-863f-4b10-b5ff-ab9ed50f4291"));
	GENV(Value(""));
	GENV(Value("a string"));
	GENV(Value::BLOB((const uint8_t*)"a\0blob", 6));
	GENV(Value::DECIMAL(INT64_C(3141), 4, 3));
	GENV(Value::DECIMAL(INT64_C(314159265), 9, 8));
	GENV(Value::DECIMAL(INT64_C(314159265358979323), 18, 17));
	GENV(Value::DECIMAL(huge_val, 32, 31));
	GENV(Value::ENUM(2, tp_enum));

	GENV(Value::EMPTYLIST(LogicalType::SQLNULL));
	GENV(Value::LIST(LogicalType::SQLNULL, {nullptr}));
	GENV(Value::EMPTYLIST(LogicalType::BOOLEAN));
	GENV(Value::LIST(LogicalType::BOOLEAN, {true, false, nullptr, true}));
	GENV(Value::LIST(LogicalType::TINYINT, {nullptr, -128, 127, 0}));
	GENV(Value::LIST(LogicalType::UTINYINT, {0, 127, 128, 255, nullptr}));
	GENV(Value::LIST(LogicalType::SMALLINT, {-32768, nullptr, 32767}));
	GENV(Value::LIST(LogicalType::USMALLINT, {65535}));
	GENV(Value::LIST(LogicalType::INTEGER, {std::numeric_limits<int32_t>::max(), -1, 2, 3, 4, 5, 6, 7, 8, nullptr, std::numeric_limits<int32_t>::min()}));
	GENV(Value::LIST(LogicalType::UINTEGER, {INT64_C(0x80000001)}));
	GENV(Value::LIST(LogicalType::BIGINT, {std::numeric_limits<int64_t>::max(), nullptr, std::numeric_limits<int64_t>::min()}));
	GENV(Value::LIST(LogicalType::UBIGINT, {Value::UBIGINT(100), Value::UBIGINT(200)}));
	GENV(Value::LIST(LogicalType::HUGEINT, {Value::HUGEINT(huge_val), Value::HUGEINT(huge_val / 2), nullptr}));
	GENV(Value::LIST(LogicalType::FLOAT, {3.14f, 2.71f, 0.0f, nullptr, std::numeric_limits<float>::max()}));
	GENV(Value::LIST(LogicalType::DOUBLE, {3.14, 2.71, 0.0, nullptr, std::numeric_limits<double>::max()}));
	GENV(Value::LIST(LogicalType::DATE, {Value::DATE(2021, 12, 31), nullptr, Value::DATE(2022, 1, 1)}));
	GENV(Value::LIST(LogicalType::TIME, {Value::TIME(16, 54, 59, 999999), Value::TIME(0, 0, 0, 0), nullptr}));
	GENV(Value::LIST(LogicalType::TIME_TZ, {Value::TIME(16, 54, 59, 999999), Value::TIME(0, 0, 0, 0), nullptr}));
	GENV(Value::LIST(LogicalType::TIMESTAMP, vts));
	GENV(Value::LIST(LogicalType::TIMESTAMP_TZ, vts));
	GENV(Value::LIST(LogicalType::TIMESTAMP_S, vts));
	GENV(Value::LIST(LogicalType::TIMESTAMP_MS, vts));
	GENV(Value::LIST(LogicalType::TIMESTAMP_NS, vts));
	GENV(Value::LIST(LogicalType::INTERVAL, {Value::INTERVAL(13, 18, 10000000000), nullptr, Value::INTERVAL(130, 1, 10000000)}));
	GENV(Value::LIST(LogicalType::HASH, {Value::HASH(Hash("Hash me")), nullptr, Value::HASH(Hash("Hash me too"))}));
	GENV(Value::LIST(LogicalType::UUID, {Value::UUID("6b6542ea-863f-4b10-b5ff-ab9ed50f4291"), nullptr, Value::UUID("33b66900-6d7e-11ec-90d6-0242ac120003")}));
	GENV(Value::LIST(LogicalType::DECIMAL(4, 3), {Value::DECIMAL(INT64_C(3141), 4, 3), nullptr, Value::DECIMAL(INT64_C(65535), 4, 3)}));
	GENV(Value::LIST(LogicalType::DECIMAL(9, 8), {Value::DECIMAL(INT64_C(314159265), 9, 8), nullptr, Value::DECIMAL(INT64_C(1), 9, 8)}));
	GENV(Value::LIST(LogicalType::DECIMAL(18, 17), {Value::DECIMAL(INT64_C(314159265358979323), 18, 17), nullptr, Value::DECIMAL(INT64_C(1), 18, 17)}));
	GENV(Value::LIST(LogicalType::DECIMAL(32, 31), {Value::DECIMAL(huge_val, 32, 31), nullptr, Value::DECIMAL(INT64_C(1), 32, 31)}));
	GENV(Value::EMPTYLIST(LogicalType::VARCHAR));
	GENV(Value::LIST(LogicalType::VARCHAR, {"", nullptr, "a string"}));
	GENV(Value::LIST(LogicalType::BLOB, {Value::BLOB((const uint8_t*)"a\0blob", 6), nullptr, "", "a blob"}));
	GENV(Value::LIST(tp_enum, {Value::ENUM(0, tp_enum), nullptr, Value::ENUM(1, tp_enum), Value::ENUM(2, tp_enum)}));
	GENV(Value::LIST(LogicalType::LIST(LogicalType::SMALLINT), {
		Value::LIST(LogicalType::SMALLINT, {1, 2, 3, nullptr}),
		Value::LIST(LogicalType::SMALLINT, {4, 5, 6}),
		nullptr,
		Value::LIST(LogicalType::SMALLINT, {}),
		Value::LIST(LogicalType::SMALLINT, {1})}));
	GENV(Value::LIST(LogicalType::LIST(LogicalType::VARCHAR), {
		Value::LIST(LogicalType::VARCHAR, {"a", "bbb", "cdefg", nullptr}),
		Value::LIST(LogicalType::VARCHAR, {"h", "ij", "klm"}),
		nullptr,
		Value::LIST(LogicalType::VARCHAR, {}),
		Value::LIST(LogicalType::VARCHAR, {"", ""})}));

	GENV(any_list);
	GENV(Value::EMPTYLIST(LogicalType::STRUCT({})));
	GENV(Value::LIST(LogicalType::STRUCT({}), {Value::STRUCT({}), nullptr, Value::STRUCT({})}));
	GENV(Value::EMPTYLIST(st.type()));
	GENV(Value::LIST(st.type(), {st, Value(st.type()), st_null, st2}));
	GENV(Value::STRUCT({}));
	GENV(st);
	GENV(Value::MAP(Value::LIST({"key1", "key2"}), Value::LIST({1, 2})));
#endif
}

// clang-format on
