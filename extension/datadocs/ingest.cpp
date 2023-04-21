#include "duckdb.hpp"
#ifndef DUCKDB_AMALGAMATION
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#endif

#include "datadocs-extension.hpp"
#include "vector_converter.hpp"
#include "datadocs.hpp"
#include "inferrer.h"

namespace duckdb {

namespace {

struct TypeInfo {
	LogicalType type;
	const VectorConverter::VTable *vtbl;
};

static const TypeInfo inferrer_types[] = {
	{LogicalTypeId::VARCHAR, &VectorConverter::VTBL_VARCHAR},
	{LogicalTypeId::BOOLEAN, &VectorConverter::VTBL_BOOL},
	{LogicalTypeId::BIGINT, &VectorConverter::VTBL_BIGINT},
	{LogicalTypeId::DOUBLE, &VectorConverter::VTBL_DOUBLE},
	{LogicalTypeId::DATE, &VectorConverter::VTBL_DATE},
	{LogicalTypeId::TIME, &VectorConverter::VTBL_TIME},
	{LogicalTypeId::TIMESTAMP, &VectorConverter::VTBL_TIMESTAMP},
	{LogicalTypeId::BLOB, &VectorConverter::VTBL_BLOB},
	{DDNumericType, &VectorConverter::VTBL_NUMERIC},
	{LogicalTypeId::VARCHAR, &VectorConverter::VTBL_GEO},
	{LogicalTypeId::STRUCT, &VectorConverter::VTBL_STRUCT},
	{DDVariantType, &VectorConverter::VTBL_VARIANT},
};

struct IngestBindData : public TableFunctionData {
	explicit IngestBindData(const string &file_name)
	    : parser(Ingest::Parser::get_parser(file_name)) {
	}

	static LogicalType ProcessColumn(const Ingest::ColumnDefinition &col, vector<VectorConverter> &columns) {
		TypeInfo tp;
		if (col.column_type == Ingest::ColumnType::Struct) {
//			columns.emplace_back(ST);
			child_list_t<LogicalType> child_types;
			for (const auto &field : col.fields) {
				child_types.push_back({field.column_name, ProcessColumn(field, columns.back().children)});
			}
//			tp = LogicalType::STRUCT(std::move(child_types));
		} else {
			tp = inferrer_types[static_cast<size_t>(col.column_type)];
			columns.emplace_back(*tp.vtbl, col.format);
		}
		if (col.is_list) {
//			tp = LogicalType::LIST(tp);
//			columns.emplace_back(tp);
		}
		return tp.type;
	}

	void BindSchema(vector<LogicalType> &return_types, vector<string> &names) {
		if (!parser->infer_schema()) {
			throw InvalidInputException("Cannot ingest file");
		}
		Ingest::Schema *schema = parser->get_schema();
		for (const auto &col : schema->columns) {
			names.push_back(col.column_name);
			return_types.push_back(ProcessColumn(col, columns));
		}
	}

	std::unique_ptr<Ingest::Parser> parser;
	bool is_finished = false;
	vector<VectorConverter> columns;
};

static unique_ptr<FunctionData> IngestBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
	if (!DBConfig::GetConfig(context).options.enable_external_access) {
		throw PermissionException("Scanning external files is disabled through configuration");
	}
	const string &file_name = StringValue::Get(input.inputs[0]);
	auto result = make_unique<IngestBindData>(file_name);
	result->BindSchema(return_types, names);
	return result;
}

static unique_ptr<GlobalTableFunctionState> IngestInit(ClientContext &context, TableFunctionInitInput &input) {
	auto *bind_data = (IngestBindData *)input.bind_data;
	bind_data->parser->open();
	return nullptr;
}

static void IngestImpl(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto *bind_data = (IngestBindData *)data_p.bind_data;
	if (bind_data->is_finished) {
		return;
	}
	auto &parser = *bind_data->parser;
	VectorRow row(output, bind_data->columns);
	while (row.i_row < STANDARD_VECTOR_SIZE) {
		if (!parser.GetNextRow(row)) {
			parser.close();
			bind_data->is_finished = true;
			break;
		}
		++row.i_row;
	}
	output.SetCardinality(row.i_row);
}

} // namespace

void DataDocsExtension::LoadIngest(Connection &con) {
	auto &context = *con.context;
	auto &catalog = Catalog::GetSystemCatalog(context);
	CreateTableFunctionInfo schema_info(
	    TableFunction("ingest_file", {LogicalType::VARCHAR}, IngestImpl, IngestBind, IngestInit));
	catalog.CreateTableFunction(context, &schema_info);
}

} // namespace duckdb
