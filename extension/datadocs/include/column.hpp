#pragma once
#include <utility>
#include <memory>

#include "duckdb.hpp"

#include "vector_proxy.hpp"

namespace duckdb {

class IngestColBase {
public:
	IngestColBase(string name, idx_t &cur_row) noexcept : vec(nullptr), cur_row(cur_row), name(std::move(name)) {
	}
	virtual ~IngestColBase() = default;

	void WriteNull() {
		Writer().SetNull();
	}
	virtual bool Write(string_t v) {
		return false;
	}
	virtual bool Write(int64_t v) {
		return false;
	}
	virtual bool Write(bool v) {
		return false;
	}
	virtual bool Write(double v) {
		return false;
	}
	virtual bool WriteExcelDate(double v) {
		return false;
	}
	virtual void SetVector(Vector *new_vec) noexcept {
		D_ASSERT(new_vec->GetType() == GetType());
		vec = new_vec;
	}

	virtual LogicalType GetType() const {
		return LogicalType::SQLNULL;
	};
	const string &GetName() const noexcept {
		return name;
	}

protected:
	Vector &GetVector() noexcept {
		D_ASSERT(vec);
		return *vec;
	}
	VectorWriter Writer() noexcept {
		return {GetVector(), cur_row};
	}

private:
	Vector *vec;
	idx_t &cur_row;
	string name;
};

class IngestColVARCHAR : public IngestColBase {
public:
	using IngestColBase::IngestColBase, IngestColBase::Write;

	LogicalType GetType() const override {
		return LogicalType::VARCHAR;
	};
	bool Write(string_t v) override;
	bool Write(int64_t v) override;
	bool Write(bool v) override;
	bool Write(double v) override;
	bool WriteExcelDate(double v) override;
};

class IngestColBOOLEAN : public IngestColBase {
public:
	using IngestColBase::IngestColBase, IngestColBase::Write;

	LogicalType GetType() const override {
		return LogicalType::BOOLEAN;
	};
	bool Write(string_t v) override;
	bool Write(int64_t v) override;
	bool Write(bool v) override;
};

class IngestColBIGINT : public IngestColBase {
public:
	using IngestColBase::IngestColBase, IngestColBase::Write;

	LogicalType GetType() const override {
		return LogicalType::BIGINT;
	};
	bool Write(string_t v) override;
	bool Write(int64_t v) override;
	bool Write(bool v) override;
	bool Write(double v) override;
};

class IngestColDOUBLE : public IngestColBase {
public:
	using IngestColBase::IngestColBase, IngestColBase::Write;

	LogicalType GetType() const override {
		return LogicalType::DOUBLE;
	};
	bool Write(string_t v) override;
	bool Write(int64_t v) override;
	bool Write(bool v) override;
	bool Write(double v) override;
};

class IngestColDateBase : public IngestColBase {
public:
	using IngestColBase::Write;

	IngestColDateBase(string name, idx_t &cur_row, string format) noexcept
	    : IngestColBase(std::move(name), cur_row), format(std::move(format)) {
	}

protected:
	string format;
};

class IngestColDATE : public IngestColDateBase {
public:
	using IngestColDateBase::IngestColDateBase, IngestColDateBase::Write;

	LogicalType GetType() const override {
		return LogicalType::DATE;
	};
	bool Write(string_t v) override;
	bool WriteExcelDate(double v) override;
};

class IngestColTIME : public IngestColDateBase {
public:
	using IngestColDateBase::IngestColDateBase, IngestColDateBase::Write;

	LogicalType GetType() const override {
		return LogicalType::TIME;
	};
	bool Write(string_t v) override;
	bool WriteExcelDate(double v) override;
};

class IngestColTIMESTAMP : public IngestColDateBase {
public:
	using IngestColDateBase::IngestColDateBase, IngestColDateBase::Write;

	LogicalType GetType() const override {
		return LogicalType::TIMESTAMP;
	};
	bool Write(string_t v) override;
	bool WriteExcelDate(double v) override;
};

class IngestColBLOBBase64 : public IngestColBase {
public:
	using IngestColBase::IngestColBase, IngestColBase::Write;

	LogicalType GetType() const override {
		return LogicalType::BLOB;
	};
	bool Write(string_t v) override;
};

class IngestColBLOBHex : public IngestColBase {
public:
	using IngestColBase::IngestColBase, IngestColBase::Write;

	LogicalType GetType() const override {
		return LogicalType::BLOB;
	};
	bool Write(string_t v) override;
};

class IngestColNUMERIC : public IngestColBase {
public:
	using IngestColBase::IngestColBase, IngestColBase::Write;
};

class IngestColGEO : public IngestColBase {
public:
	using IngestColBase::IngestColBase, IngestColBase::Write;
};

class IngestColVARIANT : public IngestColBase {
public:
	using IngestColBase::IngestColBase, IngestColBase::Write;
};

struct IngestColChildrenMap {
	void Clear() {
		valid.assign(keys.size(), false);
		cnt_valid = 0;
	}
	size_t GetIndex(const string &s) {
		auto it = keys.find(s);
		if (it == keys.end())
			return -1;
		if (!valid[it->second]) {
			valid[it->second] = true;
			++cnt_valid;
		}
		return it->second;
	}

	std::unordered_map<string, size_t> keys;
	size_t cnt_valid;
	vector<bool> valid;
};

} // namespace duckdb