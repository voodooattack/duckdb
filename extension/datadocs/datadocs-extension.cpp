#define DUCKDB_EXTENSION_MAIN

#include "duckdb.hpp"

#include "datadocs-extension.hpp"

namespace duckdb {

void DataDocsExtension::Load(DuckDB &db) {
	Connection con(db);
	con.BeginTransaction();
	LoadVariant(con);
	con.Commit();
}

string DataDocsExtension::Name() {
	return "datadocs";
}

} // namespace duckdb
