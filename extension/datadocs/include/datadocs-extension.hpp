#pragma once

#include "duckdb.hpp"

namespace duckdb {

class DataDocsExtension : public Extension {
public:
	void Load(DuckDB &db) override;
	string Name() override;

private:
	void LoadVariant(Connection &con);
};

} // namespace duckdb
