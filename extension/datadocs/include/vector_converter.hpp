#pragma once
#include <stddef.h>
#include <utility>

#include "duckdb.hpp"

#include "vector_proxy.hpp"

namespace duckdb {

class VectorConverter {
public:
	struct VTable;
	static const VTable
		VTBL_VARCHAR,
		VTBL_BOOL,
		VTBL_BIGINT,
		VTBL_DOUBLE,
		VTBL_DATE,
		VTBL_TIME,
		VTBL_TIMESTAMP,
		VTBL_BLOB,
		VTBL_NUMERIC,
		VTBL_GEO,
		VTBL_STRUCT,
		VTBL_VARIANT;

	VectorConverter(const VTable &vtbl, string format) noexcept : vtbl(vtbl), format(std::move(format)) {
	}

	vector<VectorConverter> children;
	const string format;
	const VTable &vtbl;
};

class VectorCnvWriter : private VectorWriter {
public:
	struct Converters;

	VectorCnvWriter(Vector &vec, idx_t i_row, VectorConverter &cnv) noexcept : VectorWriter(vec, i_row), cnv(cnv) {
	}

	using VectorWriter::SetNull;
	int Write(string_t v);
	int Write(int64_t v);
	int Write(bool v);
	int Write(double v);
	int WriteExcelDate(double v);

private:
	VectorConverter &cnv;
};

class VectorRow
{
public:
	VectorRow(DataChunk &output, vector<VectorConverter> &columns) noexcept
	    : output(output), columns(columns), i_row(0) {
	}

	VectorCnvWriter operator[](size_t index) noexcept;
	void WriteError(size_t i_col, const string &msg);

	idx_t i_row;

private:
	duckdb::DataChunk &output;
	vector<VectorConverter> &columns;
};

} // namespace duckdb
