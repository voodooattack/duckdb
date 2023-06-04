#define DUCKDB_EXTENSION_MAIN

#include "datadocs-extension.hpp"

namespace duckdb {

void DataDocsExtension::Load(DuckDB &db) {
	auto &inst = *db.instance;
	LoadGeo(inst);
	LoadVariant(inst);
	LoadIngest(inst);
}

string DataDocsExtension::Name() {
	return "datadocs";
}

} // namespace duckdb
