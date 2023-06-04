#pragma once

#include "duckdb.hpp"

namespace duckdb {

class DataDocsExtension : public Extension {
public:
	void Load(DuckDB &db) override;
	string Name() override;

private:
	void LoadGeo(DatabaseInstance &inst);
	void LoadVariant(DatabaseInstance &inst);
	void LoadIngest(DatabaseInstance &inst);
};

} // namespace duckdb
