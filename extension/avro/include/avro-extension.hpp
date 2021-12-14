#pragma once

#include "duckdb.hpp"

namespace duckdb {

class AvroExtension : public Extension {
public:
	void Load(DuckDB &db) override;
	string Name() override;
};

} // namespace duckdb
