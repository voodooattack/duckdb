#include <expat.h>

#include "xml_reader.h"
#include "utility.h"

using namespace std::literals;

namespace duckdb {

XMLHandlerBase::~XMLHandlerBase()
{
	if (m_parser)
		XML_ParserFree(m_parser);
}

bool XMLHandlerBase::parse_string(const std::string& input)
{
	create_parser();
	bool res = XML_Parse(m_parser, input.data(), input.size(), 0) == XML_STATUS_OK;
	XML_ParserFree(m_parser); m_parser = nullptr;
	return res;
}

XML_ParserStruct* XMLHandlerBase::create_parser()
{
	if (m_parser)
		XML_ParserFree(m_parser);
	m_parser = XML_ParserCreate(nullptr);
	XML_SetUserData(m_parser, this);
	XML_SetElementHandler(m_parser, p_start, p_end);
	XML_SetCharacterDataHandler(m_parser, do_text);
	return m_parser;
}

void XMLHandlerBase::do_text(void* userdata, const char* buf, int buflen)
{
	static_cast<XMLHandlerBase*>(userdata)->m_cur_text.append(buf, buflen);
}

bool XMLHandlerBase::check_new_text()
{
	size_t i = m_cur_text.find_first_not_of("\t\n\v\f\r ");
	if (i != std::string::npos)
	{
		rtrim(m_cur_text);
		if (i > 0)
			m_cur_text.erase(0, i);
		return true;
	}
	return false;
}

void XMLHandlerBase::abort()
{
	XML_StopParser(m_parser, 0);
}

class XMLParseHandler : public XMLHandler<XMLParseHandler>
{
public:
	XMLParseHandler(XMLBase* top = nullptr) : m_top(top) {}

	bool start_tag(const char* name, const char** atts)
	{
		XMLBase* new_top = m_top->new_tag(name, atts);
		if (!new_top)
			return false;
		m_stack.push_back(m_top);
		m_top = new_top;
		return true;
	}

	void end_tag()
	{
		m_top->end_tag();
		m_top = m_stack.back();
		m_stack.pop_back();
	}

	bool new_text()
	{
		bool res = m_top->new_text(std::move(m_cur_text));
		m_cur_text.clear();
		return res;
	};

	bool convert_cell(CellRaw& src, XMLBase* root)
	{
		const std::string* sp = std::get_if<std::string>(&src);
		if (!sp)
			return false;
		if (sp->empty())
			return true;
		m_top = root;
		m_stack.clear();
		m_cur_text.clear();
		return parse_string(*sp);
	}
private:
	XMLBase* m_top;
	std::vector<XMLBase*> m_stack;
};

class XMLValueBase : public XMLBase
{
public:
	XMLValueBase(IngestColBase *column) : column(*column) {}
	IngestColBase &column;
};

static void XMLBuildColumns(const vector<IngestColumnDefinition> &fields, std::unordered_map<string, size_t> &keys,
    vector<std::unique_ptr<XMLValueBase>> &columns, idx_t &cur_row);

class XMLValue : public XMLValueBase
{
public:
	using XMLValueBase::XMLValueBase;
	bool new_text(std::string&& s) override
	{
		if (s.empty() || !column.Write(s)) {
			column.WriteNull();
		}
		return true;
	}
};

template <class Col>
class XMLCol : public XMLValue, public Col {
public:
	template<typename... Args>
	XMLCol(Args&&... args) : XMLValue(this), Col(std::forward<Args>(args)...) {
	}
};

class XMLList : public XMLValueBase, public IngestColBase
{
public:
	XMLList(const IngestColumnDefinition &col, idx_t &parent_cur_row)
	    : XMLValueBase(this), IngestColBase(col.column_name, parent_cur_row), buffer(nullptr), column(BuildColumn(col, cur_row)) {
	}

	virtual void SetVector(Vector *new_vec) noexcept {
		IngestColBase::SetVector(new_vec);
		buffer = (VectorListBuffer *)(new_vec->GetAuxiliary().get());
		column->SetVector(&buffer->GetChild());
	}

	virtual LogicalType GetType() const {
		return LogicalType::LIST(column->GetType());
	};

	virtual void first_tag() override {
		auto &entry = Writer().GetList();
		entry.offset = buffer->size;
		entry.length = 0;
		length = &entry.length;
	}
	virtual bool second_tag() override { return true; }
	virtual bool new_text(std::string&& s) override
	{
		if (buffer->size + 1 > buffer->capacity) {
			buffer->GetChild().Resize(buffer->capacity, buffer->capacity * 2);
			buffer->capacity *= 2;
		}
		cur_row = buffer->size;
		++buffer->size;
		++(*length);

		if (s.empty() || !column->Write(s)) {
			column->WriteNull();
		}
		return true;
	}

private:
	VectorListBuffer *buffer;
	std::unique_ptr<IngestColBase> column;
	idx_t cur_row;
	uint64_t *length;
};

class XMLStruct : public XMLValueBase, public IngestColBase
{
public:
	XMLStruct(const IngestColumnDefinition &col, idx_t &cur_row) : XMLValueBase(this), IngestColBase(col.column_name, cur_row) {
		XMLBuildColumns(col.fields, m_children.keys, m_columns, cur_row);
	}

	LogicalType GetType() const override {
		child_list_t<LogicalType> child_types;
		for (const auto &field : m_columns) {
			child_types.push_back({field->column.GetName(), field->column.GetType()});
		}
		return LogicalType::STRUCT(std::move(child_types));
	};

	void SetVector(Vector *new_vec) noexcept override {
		IngestColBase::SetVector(new_vec);
		const auto &entries = StructVector::GetEntries(*new_vec);
		size_t n_columns = m_columns.size();
		for (size_t i_col = 0; i_col < n_columns; ++i_col) {
			m_columns[i_col]->column.SetVector(entries[i_col].get());
		}
	}

	virtual void first_tag() override
	{
		m_children.Clear();
		for (auto& v : m_columns)
			v->m_saw_tag = false;
	}
	virtual XMLBase* new_tag(const char* name, const char** atts) override
	{
		size_t i = m_children.GetIndex(std::string(name));
		if (i == -1)
			return nullptr;
		XMLBase* obj = m_columns[i].get();
		if (!obj->m_saw_tag)
		{
			obj->first_tag();
			obj->m_saw_tag = true;
		}
		else if (!obj->second_tag())
			return nullptr;
		if (atts)
			for (; *atts; atts += 2)
			{
				XMLBase* attr_obj = obj->new_tag(*atts, nullptr);
				if (attr_obj && !attr_obj->new_text(std::string(atts[1])))
					return nullptr;
			}
		return obj;
	}
	virtual bool new_text(std::string&& s) override
	{
		XMLBase* obj = new_tag("#text", nullptr);
		return !obj || obj->new_text(std::move(s));
	}

	void end_tag() override {
		if (m_children.cnt_valid == 0) {
			WriteNull();
			return;
		}
		size_t n_columns = m_columns.size();
		if (m_children.cnt_valid < n_columns) {
			for (size_t i_col = 0; i_col < n_columns; ++i_col)
			{
				if (!m_children.valid[i_col]) {
					m_columns[i_col]->column.WriteNull();
				}
			}
		}
	}

protected:
	XMLStruct(idx_t &cur_row) : XMLValueBase(this), IngestColBase("__root__", cur_row) {}

	vector<std::unique_ptr<XMLValueBase>> m_columns;
	IngestColChildrenMap m_children;
};

class XMLListStruct : public XMLValueBase, public IngestColBase
{
public:
	XMLListStruct(const IngestColumnDefinition &col, idx_t &parent_cur_row)
	    : XMLValueBase(this), IngestColBase(col.column_name, parent_cur_row), buffer(nullptr), column(col, cur_row) {
	}

	virtual void SetVector(Vector *new_vec) noexcept {
		IngestColBase::SetVector(new_vec);
		buffer = (VectorListBuffer *)(new_vec->GetAuxiliary().get());
		column.SetVector(&buffer->GetChild());
	}

	virtual LogicalType GetType() const {
		return LogicalType::LIST(column.GetType());
	};

	virtual void first_tag() override {
		auto &entry = Writer().GetList();
		entry.offset = buffer->size;
		entry.length = 0;
		length = &entry.length;
		second_tag();
	}
	virtual bool second_tag() override { 
		if (buffer->size + 1 > buffer->capacity) {
			buffer->GetChild().Resize(buffer->capacity, buffer->capacity * 2);
			buffer->capacity *= 2;
		}
		cur_row = buffer->size;
		++buffer->size;
		++(*length);
		column.first_tag();
		return true;
	}

	virtual XMLBase* new_tag(const char* name, const char** atts) override {
		return column.new_tag(name, atts);
	}

	virtual bool new_text(std::string&& s) override {
		return column.new_text(std::move(s));
	}

	void end_tag() override {
		column.end_tag();
	}

private:
	VectorListBuffer *buffer;
	XMLStruct column;
	idx_t cur_row;
	uint64_t *length;
};

class XMLTopStruct : public XMLStruct {
public:
	XMLTopStruct(XMLRoot *root) : XMLStruct(cur_row), raw_parser(nullptr), col_row_number("__rownum__", cur_row), handler(root) {}

	void BuildColumns(const Schema &schema) {
		XMLBuildColumns(schema.columns, m_children.keys, m_columns, cur_row);
	}

	void BindSchema(vector<LogicalType> &return_types, vector<string> &names) {
		for (auto &col : m_columns) {
			names.push_back(col->column.GetName());
			return_types.push_back(col->column.GetType());
		}
		names.push_back(col_row_number.GetName());
		return_types.push_back(col_row_number.GetType());
		m_row_number = 0;
	}

	bool NewChunk(BaseReader &reader, DataChunk &output) {
		cur_row = 0;
		size_t n_columns = m_columns.size();
		D_ASSERT(output.data.size() == n_columns + 1);
		for (size_t i = 0; i < n_columns; ++i) {
			m_columns[i]->column.SetVector(&output.data[i]);
		}
		col_row_number.SetVector(&output.data[n_columns]);

		const int BUFF_SIZE = 4096;
		if (!raw_parser) {
			raw_parser = handler.create_parser();
		} else {
			auto status = XML_ResumeParser(raw_parser);
			if (status == XML_STATUS_SUSPENDED) {
				return true;
			}
			if (!status) {
				D_ASSERT(status != XML_ERROR_NOT_SUSPENDED);
				return false;
			}
		}
		while (true)
		{
			void* buff = XML_GetBuffer(raw_parser, BUFF_SIZE);
			if (!buff)
				return false;
			size_t bytes_read = reader.read((char*)buff, BUFF_SIZE);
			auto status = XML_ParseBuffer(raw_parser, bytes_read, bytes_read == 0);
			if (status == XML_STATUS_SUSPENDED)
				return true;
			if (!status || bytes_read == 0)
				return false;
		}
		//enum XML_Error status = XML_GetErrorCode(raw_parser);
		//return status != XML_ERROR_ABORTED;
	}

	void end_tag() override {
		if (m_children.cnt_valid == 0) {
			++m_row_number;
			return;
		}
		XMLStruct::end_tag();
		col_row_number.Write(m_row_number++);
		if (++cur_row >= STANDARD_VECTOR_SIZE) {
			XML_StopParser(raw_parser, XML_TRUE);
		}
	}
	idx_t cur_row;

private:
	int64_t m_row_number;
	IngestColBIGINT col_row_number;
	XML_Parser raw_parser;
	XMLParseHandler handler;
};

class XMLTopListStruct : public XMLBase
{
public:
	XMLTopListStruct(XMLRoot *root) : m_struct(root) {};
	virtual XMLBase* new_tag(const char* name, const char** atts) override
	{
		m_struct.first_tag();
		if (atts)
			for (; *atts; atts += 2)
			{
				XMLBase* attr_obj = m_struct.new_tag(*atts, nullptr);
				if (attr_obj && !attr_obj->new_text(std::string(atts[1])))
					return nullptr;
			}
		return &m_struct;
	}
	XMLTopStruct m_struct;
};

static void XMLBuildColumns(const vector<IngestColumnDefinition> &fields, std::unordered_map<string, size_t> &keys,
    vector<std::unique_ptr<XMLValueBase>> &columns, idx_t &cur_row) {
	XMLValueBase *column;
	for (const auto &col : fields) {
		if (col.index < 0) {
			continue;
		}
		if (col.is_list) {
			if (col.column_type == ColumnType::Struct) {
				column = new XMLListStruct(col, cur_row);
			} else {
				column = new XMLList(col, cur_row);
			}
		} else {
			switch(col.column_type) {
			case ColumnType::String: column = new XMLCol<IngestColVARCHAR>(col.column_name, cur_row); break;
			case ColumnType::Boolean: column = new XMLCol<IngestColBOOLEAN>(col.column_name, cur_row); break;
			case ColumnType::Integer: column = new XMLCol<IngestColBIGINT>(col.column_name, cur_row); break;
			case ColumnType::Decimal: column = new XMLCol<IngestColDOUBLE>(col.column_name, cur_row); break;
			case ColumnType::Date: column = new XMLCol<IngestColDATE>(col.column_name, cur_row, col.format); break;
			case ColumnType::Time: column = new XMLCol<IngestColTIME>(col.column_name, cur_row, col.format); break;
			case ColumnType::Datetime: column = new XMLCol<IngestColTIMESTAMP>(col.column_name, cur_row, col.format); break;
			case ColumnType::Bytes:
				if (col.format == "base64") {
					column = new XMLCol<IngestColBLOBBase64>(col.column_name, cur_row);
				} else {
					column = new XMLCol<IngestColBLOBHex>(col.column_name, cur_row);
				}
				break;
			case ColumnType::Numeric: column = new XMLCol<IngestColNUMERIC>(col.column_name, cur_row); break;
			case ColumnType::Geography: column = new XMLCol<IngestColGEO>(col.column_name, cur_row); break;
			case ColumnType::Struct:
				column = new XMLStruct(col, cur_row);
				break;
			case ColumnType::Variant: column = new XMLCol<IngestColVARIANT>(col.column_name, cur_row); break;
			default:
				D_ASSERT(false);
				column = new XMLCol<IngestColBase>(col.column_name, cur_row);
				break;
			}
		}
		keys.emplace(col.column_name, columns.size());
		columns.push_back(std::unique_ptr<XMLValueBase>(column));
	}
}

XMLParser::XMLParser(std::shared_ptr<BaseReader> reader) :
	m_reader(reader)
{
	root.assign(new XMLTopListStruct(&root));
}

XMLParser::~XMLParser()
{
	close();
}

bool XMLParser::open()
{
	if (m_reader->is_file())
		return m_reader->open();
	return false;
}

void XMLParser::close()
{
	m_reader->close();
}

int XMLParser::get_percent_complete()
{
	return m_reader->pos_percent();
}

void XMLParser::BuildColumns() {
	((XMLTopListStruct*)root.m_root.get())->m_struct.BuildColumns(m_schema);
}

void XMLParser::BindSchema(vector<LogicalType> &return_types, vector<string> &names) {
	((XMLTopListStruct*)root.m_root.get())->m_struct.BindSchema(return_types, names);
}

idx_t XMLParser::FillChunk(DataChunk &output)
{
	XMLTopStruct &top = ((XMLTopListStruct*)root.m_root.get())->m_struct;
	if (top.NewChunk(*m_reader, output)) {
		return STANDARD_VECTOR_SIZE;
	}
	close();
	is_finished = true;
	return top.cur_row;
}

}