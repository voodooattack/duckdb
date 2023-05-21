#include <string_view>

#include "json_reader.h"
#include "utility.h"

using namespace std::literals;

namespace duckdb {

bool JSONHandler::EndObject(JSONDispatcher* dispatcher) { dispatcher->pop(); return true; }
bool JSONHandler::EndArray(JSONDispatcher* dispatcher) { dispatcher->pop(); return true; }

class JSONRoot : public JSONHandler
{
public:
	JSONRoot(const JSONSchema& schema) : m_level(0), m_start_path(schema.start_path) {}
	virtual ~JSONRoot() = default;
	virtual bool Null() override { return false; }
	virtual bool StartObject(JSONDispatcher* dispatcher) override { return m_level < m_start_path.size(); }
	virtual bool Key(const char* s, int length, bool copy, JSONDispatcher* dispatcher) override
	{
		if (m_start_path[m_level] == std::string_view(s, length))
		{
			++m_level;
			dispatcher->m_value = this;
		}
		else
			dispatcher->skip();
		return true;
	}
	virtual bool EndObject(JSONDispatcher* dispatcher) override { return false; }
	virtual bool StartArray(JSONDispatcher* dispatcher) override
	{
		if (m_level < m_start_path.size())
			return false;
		dispatcher->m_suspended = true;
		return true;
	}
	virtual bool EndArray(JSONDispatcher* dispatcher) override { return false; }
protected:
	size_t m_level;
	const vector<string>& m_start_path;
};

bool JSONDispatcher::parse_string(const string& input, JSONHandler* handler)
{
	m_stack.clear();
	m_top = nullptr;
	m_value = handler;
	rj::StringStream ss(input.data());
	return rj::Reader().Parse(ss, *this);
}

void JSONDispatcher::init(JSONHandler* root)
{
	m_stack.clear();
	m_top = m_value = root;
	m_suspended = false;
}

static void JSONBuildColumns(const vector<IngestColumnDefinition> &fields, std::unordered_map<string, size_t> &keys,
    vector<std::unique_ptr<JSONValue>> &columns, idx_t &cur_row);

template <class Col>
class JSONCol : public JSONValue, public Col {
public:
	template<typename... Args>
	JSONCol(Args&&... args) : JSONValue(this), Col(std::forward<Args>(args)...) {
	}
	virtual ~JSONCol() = default;
};

class JSONStruct : public JSONValue, public IngestColBase {
public:
	JSONStruct(const IngestColumnDefinition &col, idx_t &cur_row) : JSONValue(this), IngestColBase(col.column_name, cur_row) {
		JSONBuildColumns(col.fields, m_children.keys, m_columns, cur_row);
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

	virtual bool StartObject(JSONDispatcher* dispatcher) override
	{
		m_children.Clear();
		dispatcher->push(this);
		return true;
	}
	virtual bool Key(const char* s, int length, bool copy, JSONDispatcher* dispatcher) override
	{
		size_t i = m_children.GetIndex(std::string(s, length));
		if (i == -1)
			return false;
		dispatcher->m_value = m_columns[i].get();
		return true;
	}
	virtual bool EndObject(JSONDispatcher* dispatcher) override {
		size_t n_columns = m_columns.size();
		if (m_children.cnt_valid < n_columns) {
			for (size_t i_col = 0; i_col < n_columns; ++i_col) {
				if (!m_children.valid[i_col]) {
					m_columns[i_col]->column.WriteNull();
				}
			}
		}
		dispatcher->pop();
		return true;
	}

public:
	vector<std::unique_ptr<JSONValue>> m_columns;
	IngestColChildrenMap m_children;
};

class JSONList : public JSONHandler
{
public:
	JSONList(const IngestColumnDefinition &col) : buffer(nullptr), column(BuildColumn(col, cur_row)) {
	}

	virtual bool Null() { Append(); column->WriteNull(); return true; }
	virtual bool Bool(bool b) override { return new_value(b); }
	virtual bool Int(int i) override { return new_value((int64_t)i); }
	virtual bool Uint(unsigned i) override { return new_value((int64_t)i); }
	virtual bool Int64(int64_t i) override { return new_value(i); }
	virtual bool Uint64(uint64_t i) override { return new_value(int64_t(i < INT64_MAX ? i : INT64_MAX)); }
	virtual bool Double(double d) override { return new_value(d); }
	virtual bool String(const char* s, int length, bool copy) override { return new_value(string_t(s, length)); }

	void SetVector(Vector *new_vec) noexcept {
		buffer = (VectorListBuffer *)(new_vec->GetAuxiliary().get());
		column->SetVector(&buffer->GetChild());
	}
	LogicalType GetType() const {
		return column->GetType();
	}
	void Init(list_entry_t &entry) {
		entry.offset = buffer->size;
		entry.length = 0;
		length = &entry.length;
	}

protected:
	void Append() {
		if (buffer->size + 1 > buffer->capacity) {
			buffer->GetChild().Resize(buffer->capacity, buffer->capacity * 2);
			buffer->capacity *= 2;
		}
		cur_row = buffer->size;
		++buffer->size;
		++(*length);
	}

	template <typename T>
	bool new_value(T value) {
		Append();
		if (!column->Write(value))
			column->WriteNull();
		return true;
	}
private:
	VectorListBuffer *buffer;
	std::unique_ptr<IngestColBase> column;
	idx_t cur_row;
	uint64_t *length;
};

class JSONListStruct : public JSONHandler
{
public:
	JSONListStruct(const IngestColumnDefinition &col) :  buffer(nullptr), column(col, cur_row) {
	}

	virtual bool Null() { Append(); column.WriteNull(); return true; }
	virtual bool Bool(bool b) override { return new_value(b); }
	virtual bool Int(int i) override { return new_value((int64_t)i); }
	virtual bool Uint(unsigned i) override { return new_value((int64_t)i); }
	virtual bool Int64(int64_t i) override { return new_value(i); }
	virtual bool Uint64(uint64_t i) override { return new_value(int64_t(i < INT64_MAX ? i : INT64_MAX)); }
	virtual bool Double(double d) override { return new_value(d); }
	virtual bool String(const char* s, int length, bool copy) override { return new_value(string_t(s, length)); }
	virtual bool StartObject(JSONDispatcher* dispatcher) override
	{
		Append();
		return column.StartObject(dispatcher);
	}

	void SetVector(Vector *new_vec) noexcept {
		buffer = (VectorListBuffer *)(new_vec->GetAuxiliary().get());
		column.SetVector(&buffer->GetChild());
	}
	LogicalType GetType() const {
		return column.GetType();
	}
	void Init(list_entry_t &entry) {
		entry.offset = buffer->size;
		entry.length = 0;
		length = &entry.length;
	}

protected:
	void Append() {
		if (buffer->size + 1 > buffer->capacity) {
			buffer->GetChild().Resize(buffer->capacity, buffer->capacity * 2);
			buffer->capacity *= 2;
		}
		cur_row = buffer->size;
		++buffer->size;
		++(*length);
	}

	template <typename T>
	bool new_value(T value) {
		Append();
		if (!column.Write(value))
			column.WriteNull();
		return true;
	}
private:
	VectorListBuffer *buffer;
	JSONStruct column;
	idx_t cur_row;
	uint64_t *length;
};

template <class T>
class JSONListWrapper : public JSONValue, public IngestColBase {
public:
	JSONListWrapper(const IngestColumnDefinition &col, idx_t &cur_row)
	    : JSONValue(this), IngestColBase(col.column_name, cur_row), child(col) {
	}
	virtual bool StartArray(JSONDispatcher* dispatcher) override {
		child.Init(Writer().GetList());
		dispatcher->push(&child);
		dispatcher->m_value = &child;
		return true;
	}

	virtual void SetVector(Vector *new_vec) noexcept {
		IngestColBase::SetVector(new_vec);
		child.SetVector(new_vec);
	}
	virtual LogicalType GetType() const {
		return LogicalType::LIST(child.GetType());
	};

private:
	T child;
};

/*
class JSONGeoStruct : public JSONHandler
{
public:
	JSONGeoStruct(Row::ColumnAdapter* adapter) :
		m_adapter(adapter),
		m_cell(std::in_place_type<std::string>),
		m_data(std::get<std::string>(m_cell)),
		m_type(m_data),
		m_coord(m_data),
		m_collection(*this)
	{}
	virtual ~JSONGeoStruct() = default;
	virtual bool StartObject(JSONDispatcher* dispatcher) override
	{
		dispatcher->push(this);
		DoStartObject();
		return true;
	}
	void DoStartObject()
	{
		m_data.clear();
		initialize();
		m_data_kind = 0;
	}
	virtual bool Key(const char* s, int length, bool copy, JSONDispatcher* dispatcher) override
	{
		if (std::strncmp(s, "type", length) == 0)
			dispatcher->m_value = &m_type;
		else if (std::strncmp(s, "coordinates", length) == 0)
		{
			if (m_type.m_index == 1) // top level
			{
				if (m_data_kind != 0)
					return false;
				m_data_kind = 1;
			}
			dispatcher->m_value = &m_coord;
		}
		else if (m_data_kind == 0 && std::strncmp(s, "geometries", length) == 0)
		{
			m_data_kind = 2;
			m_data.append(4, '\0');
			dispatcher->m_value = &m_collection;
		}
		else
			return false;
		return true;
	}
	virtual bool EndObject(JSONDispatcher* dispatcher) override
	{
		dispatcher->pop();
		return DoEndObject();
	}
	bool DoEndObject()
	{
		int res = finalize();
		if (res < 0)
			return false;
		if (res > 0)
			m_adapter->set_value(m_cell);
		return true;
	}
	void initialize()
	{
		m_data.push_back(JSONGeoCoord::endianness);
		m_type.m_index = m_data.size();
		m_data.append(4, '\0');
		m_coord.initialize();
	}
	int finalize()
	{
		size_t i_start = m_type.m_index;
		uint32_t type = *(uint32_t*)&m_data[i_start];
		if (type == 0)
			return m_data.size() == 5 ? 0 : -1;
		if (type == 7) // collection
			return (i_start == 1 && m_data_kind == 2) ? 1 : -1;
		if (i_start == 1 && m_data_kind != 1)
			return -1;
		static const int levels[] = { 1, 2, 3, 2, 3, 4 };
		if (m_coord.m_level != levels[type-1])
			return -1;
		if (type >= 4 && type <= 6) // Multi... type
		{
			type -= 3;
			size_t old_size = m_data.size();
			size_t cnt = *(uint32_t*)&m_data[i_start+4]; // count of base elements, each needs to be prefixed with 5 bytes
			m_data.resize(old_size + cnt * 5);
			m_insert_pos.clear();
			m_insert_pos.reserve(cnt);
			char* s = &m_data[i_start + 8];
			m_insert_pos.push_back(s);
			switch (type)
			{
			case 1:
				for (size_t i = 1; i < cnt; ++i)
					m_insert_pos.push_back(s += 16);
				break;
			case 2:
				for (size_t i = 1; i < cnt; ++i)
					m_insert_pos.push_back(s += *(uint32_t*)s * 16 + 4);
				break;
			case 3:
				for (size_t i = 1; i < cnt; ++i)
				{
					size_t base_cnt = *(uint32_t*)s;
					s += 4;
					for (size_t j = 0; j < base_cnt; ++j)
						s += *(uint32_t*)s * 16 + 4;
					m_insert_pos.push_back(s);
				}
				break;
			}
			char* s_end = &m_data[old_size];
			char* dst = &m_data[m_data.size()];
			while (cnt-->0)
			{
				s = m_insert_pos[cnt];
				dst = std::move_backward(s, s_end, dst);
				dst -= 5;
				*dst = JSONGeoCoord::endianness;
				*(uint32_t*)(dst+1) = type;
				s_end = s;
			}
		}
		return 1;
	}

protected:
	class JSONGeoType : public JSONHandler
	{
	public:
		JSONGeoType(std::string& data) : m_data(data) {}
		virtual ~JSONGeoType() = default;
		virtual bool String(const char* s, int length, bool copy) override
		{
			auto it = type_map.find(std::string(s, length));
			if (it == type_map.end())
				return false;
			*(uint32_t*)&m_data[m_index] = it->second;
			return true;
		}
		inline static const std::unordered_map<std::string, uint32_t> type_map {
			{"Point", 1}, {"LineString", 2}, {"Polygon", 3}, {"MultiPoint", 4},
			{"MultiLineString", 5}, {"MultiPolygon", 6}, {"GeometryCollection", 7}
		};

		std::string& m_data;
		size_t m_index;
	};

	class JSONGeoCoord : public JSONHandler
	{
	public:
		JSONGeoCoord(std::string& data) : m_data(data) {}
		virtual ~JSONGeoCoord() = default;
		virtual bool StartArray(JSONDispatcher* dispatcher) override
		{
			if (m_cur_level == 0)
				dispatcher->push(this);
			else if (m_cur_level >= 4)
				return false;
			else if (m_cnt_pos[m_cur_level-1] == 0)
			{
				m_cnt_pos[m_cur_level-1] = m_data.size();
				m_data.append(4, '\0');
			}
			++m_cur_level;
			m_cnt = 0;
			return true;
		}
		virtual bool EndArray(JSONDispatcher* dispatcher) override
		{
			if (m_cur_level >= m_level && m_cnt < 2)
				return false;
			--m_cur_level;
			if (m_cur_level == 0)
				dispatcher->pop();
			else
			{
				++*(uint32_t*)&m_data[m_cnt_pos[m_cur_level-1]];
				m_cnt_pos[m_cur_level] = 0;
			}
			return true;
		}
		virtual bool Null() override { return m_cur_level == 0; }
		virtual bool Int(int i) override { return Double(i); }
		virtual bool Uint(unsigned i) override { return Double(i); }
		virtual bool Int64(int64_t i) override { return Double(i); }
		virtual bool Uint64(uint64_t i) override { return Double(i); }
		virtual bool Double(double d) override
		{
			if (m_level == 0)
			{
				if (m_cur_level == 0)
					return false;
				m_level = m_cur_level;
			}
			else if (m_cur_level != m_level)
				return false;
			if (m_cnt < 2)
			{
				++m_cnt;
				m_data.append((char*)(&d), 8);
			}
			return true;
		}
		void initialize()
		{
			m_cnt = m_level = m_cur_level = m_cnt_pos[0] = m_cnt_pos[1] = m_cnt_pos[2] = 0;
		}

		static constexpr char endianness = 1;
		int m_cnt;
		int m_level;
		int m_cur_level;
		size_t m_cnt_pos[4]; // index 3 is a dummy
		std::string& m_data;
	};

	class JSONGeoStructCollection : public JSONHandler
	{
	public:
		JSONGeoStructCollection(JSONGeoStruct& geo) : m_item(geo) {}
		virtual ~JSONGeoStructCollection() = default;
		virtual bool StartArray(JSONDispatcher* dispatcher) override
		{
			dispatcher->push(&m_item);
			dispatcher->m_value = &m_item;
			return true;
		}
		virtual bool EndArray(JSONDispatcher* dispatcher) override
		{
			m_item.m_geo.m_type.m_index = 1;
			dispatcher->pop();
			return true;
		}

	protected:
		class JSONGeoStructCollectionItem : public JSONHandler
		{
		public:
			JSONGeoStructCollectionItem(JSONGeoStruct& geo) : m_geo(geo) {}
			virtual ~JSONGeoStructCollectionItem() = default;
			virtual bool StartObject(JSONDispatcher* dispatcher) override
			{
				m_geo.initialize();
				return true;
			}
			virtual bool Key(const char* s, int length, bool copy, JSONDispatcher* dispatcher) override
			{
				return m_geo.Key(s, length, copy, dispatcher);
			}
			virtual bool EndObject(JSONDispatcher* dispatcher) override
			{
				dispatcher->m_value = this;
				if (m_geo.finalize() <= 0)
					return false;
				++*(uint32_t*)&m_geo.m_data[5];
				return true;
			}

			JSONGeoStruct& m_geo;
		};

		JSONGeoStructCollectionItem m_item;
	};

	Row::ColumnAdapter* m_adapter;
	Cell m_cell;
	std::string& m_data;
	int m_data_kind; // 1 - coordinates, 2 - geometries
	JSONGeoType m_type;
	JSONGeoCoord m_coord;
	JSONGeoStructCollection m_collection;
	std::vector<char*> m_insert_pos;
};

class JSONListGeoStruct : public JSONGeoStruct
{
public:
	using JSONGeoStruct::JSONGeoStruct;
	virtual bool StartObject(JSONDispatcher* dispatcher) override { DoStartObject(); return true; }
	virtual bool EndObject(JSONDispatcher* dispatcher) override { return DoEndObject(); }
	void init_list_value(JSONDispatcher* dispatcher)
	{
		m_adapter->set_notnull();
		dispatcher->push(this);
		dispatcher->m_value = this;
	}
};

class JSONVariant : public JSONHandler, public Row::ColumnAdapter
{
public:
	JSONVariant(Row::ColumnAdapter* adapter) :
		m_adapter(adapter),
		m_cell(std::in_place_type<VariantCell>),
		m_var(std::get<VariantCell>(m_cell))
	{}
	virtual bool Null() override { m_var.assign<VariantCell::Null>(0); m_adapter->set_value(m_cell); return true; }
	virtual bool Bool(bool b) override { m_var.assign<VariantCell::Boolean>(b); m_adapter->set_value(m_cell); return true; }
	virtual bool Int (int i) override { m_var.assign<VariantCell::Integer>(i); m_adapter->set_value(m_cell); return true; }
	virtual bool Uint(unsigned i) override { m_var.assign<VariantCell::Unsigned>(i); m_adapter->set_value(m_cell); return true; }
	virtual bool Int64(int64_t i) override { m_var.assign<VariantCell::Integer64>(i); m_adapter->set_value(m_cell); return true; }
	virtual bool Uint64(uint64_t i) override { m_var.assign<VariantCell::Unsigned64>(i); m_adapter->set_value(m_cell); return true; }
	virtual bool Double(double d) override { m_var.assign<VariantCell::Float>(d); m_adapter->set_value(m_cell); return true; }
	virtual bool String(const char* s, int length, bool copy) override
	{
		string_to_variant(s, length, m_var);
		m_adapter->set_value(m_cell);
		return true;
	}

	virtual bool StartArray(JSONDispatcher* dispatcher) override { return StartObject(dispatcher); }
	virtual bool EndArray(JSONDispatcher* dispatcher) override { finalize(dispatcher, VariantCell::List); return true; }
	virtual bool StartObject(JSONDispatcher* dispatcher) override
	{
		if (!m_nested)
			m_nested.reset(new JSONVariant(this));
		dispatcher->push(this);
		dispatcher->m_value = m_nested.get();
		m_keys.clear();
		m_values.clear();
		return true;
	}
	virtual bool Key(const char* s, int length, bool copy, JSONDispatcher* dispatcher) override
	{
		m_keys.emplace_back(s, length);
		return true;
	}
	virtual bool EndObject(JSONDispatcher* dispatcher) override { finalize(dispatcher, VariantCell::Struct); return true; }

	virtual ColumnAdapter* column_adapter(size_t i_col) { return nullptr; };
	virtual void append(bool not_null) {};
	virtual void set_notnull() {};
	virtual void set_value(Cell& value)
	{
		m_values.push_back(std::move(m_nested->m_var));
	};

	void init_list_value(JSONDispatcher* dispatcher)
	{
		m_adapter->set_notnull();
		dispatcher->push(dispatcher->m_value);
		dispatcher->m_value = this;
	}
	
private:
	void finalize(JSONDispatcher* dispatcher, VariantCell::VariantTypeId type)
	{
		m_var.type = type;
		size_t size = (2 + m_keys.size()) * sizeof(unsigned) + m_values.size() * (sizeof(unsigned) + sizeof(uint8_t));
		size_t data_start = size;
		for (const std::string& s : m_keys)
			size += s.size();
		for (const VariantCell& cell : m_values)
			size += cell.data.size();
		std::string& data = m_var.data;
		data.reserve(size);
		data.resize(data_start);
		unsigned* pu = (unsigned*)data.data();
		*pu++ = (unsigned)m_values.size();
		*pu++ = (unsigned)data_start;
		for (const std::string& s : m_keys)
		{
			data.append(s);
			*pu++ = (unsigned)data.size();
		}
		for (const VariantCell& cell : m_values)
		{
			data.append(cell.data);
			*pu++ = (unsigned)data.size();
		}
		uint8_t* pc = (uint8_t*)pu;
		for (const VariantCell& cell : m_values)
			*pc++ = (uint8_t)cell.type;
		m_adapter->set_value(m_cell);
		dispatcher->pop();
		dispatcher->m_value = this;
	}

	Row::ColumnAdapter* m_adapter;
	Cell m_cell;
	VariantCell& m_var;
	std::unique_ptr<JSONVariant> m_nested;
	std::vector<std::string> m_keys;
	std::vector<VariantCell> m_values;
};
*/

//JSONValue* JSONValue::create(IngestColBase &col)
//{
	//switch(col.column_type)
	//{
	//case ColumnType::Struct:
	//	if (col.is_list)
	//		return new JSONListWrapper<JSONListStruct>(i_col, col.fields);
	//	else
	//		return new JSONStruct(i_col, col.fields);
	//case ColumnType::Geography:
	//	if (col.is_list)
	//		return new JSONListWrapper<JSONListGeoStruct>(i_col);
	//	else
	//		return new JSONGeoStruct(i_col);
	//case ColumnType::Variant:
	//	if (col.is_list)
	//		return new JSONListWrapper<JSONVariant>(i_col);
	//	else
	//		return new JSONVariant(i_col);
	//default:
		//if (col.is_list)
		//	return new JSONListWrapper<JSONList>(i_col, col);
		//else
		//	return nullptr;//new JSONValue();
	//}
//}

static void JSONBuildColumns(const vector<IngestColumnDefinition> &fields, std::unordered_map<string, size_t> &keys,
    vector<std::unique_ptr<JSONValue>> &columns, idx_t &cur_row) {
	JSONValue *column;
	for (const auto &col : fields) {
		if (col.index < 0) {
			continue;
		}
		if (col.is_list) {
			if (col.column_type == ColumnType::Struct) {
				column = new JSONListWrapper<JSONListStruct>(col, cur_row);
			} else {
				column = new JSONListWrapper<JSONList>(col, cur_row);
			}
		} else {
			switch(col.column_type) {
			case ColumnType::String: column = new JSONCol<IngestColVARCHAR>(col.column_name, cur_row); break;
			case ColumnType::Boolean: column = new JSONCol<IngestColBOOLEAN>(col.column_name, cur_row); break;
			case ColumnType::Integer: column = new JSONCol<IngestColBIGINT>(col.column_name, cur_row); break;
			case ColumnType::Decimal: column = new JSONCol<IngestColDOUBLE>(col.column_name, cur_row); break;
			case ColumnType::Date: column = new JSONCol<IngestColDATE>(col.column_name, cur_row, col.format); break;
			case ColumnType::Time: column = new JSONCol<IngestColTIME>(col.column_name, cur_row, col.format); break;
			case ColumnType::Datetime: column = new JSONCol<IngestColTIMESTAMP>(col.column_name, cur_row, col.format); break;
			case ColumnType::Bytes:
				if (col.format == "base64") {
					column = new JSONCol<IngestColBLOBBase64>(col.column_name, cur_row);
				} else {
					column = new JSONCol<IngestColBLOBHex>(col.column_name, cur_row);
				}
				break;
			case ColumnType::Numeric: column = new JSONCol<IngestColNUMERIC>(col.column_name, cur_row); break;
			case ColumnType::Geography: column = new JSONCol<IngestColGEO>(col.column_name, cur_row); break;
			case ColumnType::Struct:
				column = new JSONStruct(col, cur_row);
				break;
			case ColumnType::Variant: column = new JSONCol<IngestColVARIANT>(col.column_name, cur_row); break;
			default:
				D_ASSERT(false);
				column = new JSONCol<IngestColBase>(col.column_name, cur_row);
				break;
			}
		}
		keys.emplace(col.column_name, columns.size());
		columns.push_back(std::unique_ptr<JSONValue>(column));
	}
}

JSONParser::JSONParser(std::shared_ptr<BaseReader> reader) :
	m_reader(reader), m_stream(*m_reader), m_top()//m_schema)
{}

JSONParser::~JSONParser()
{
	close();
}

bool JSONParser::open()
{
	if (!m_reader->is_file() || !m_reader->open()) {
		return false;
	}
	rj_reader.IterativeParseInit();
	JSONRoot root(m_schema);
	m_dispatcher.init(&root);
	if (!do_parse())
		return false;
	m_dispatcher.init(&m_top);
	return true;
}

void JSONParser::close()
{
	m_reader->close();
}

int JSONParser::get_percent_complete()
{
	return m_reader->pos_percent();
}

bool JSONParser::do_parse()
{
	m_dispatcher.m_suspended = false;
	while (!rj_reader.IterativeParseComplete()) {
		if (!rj_reader.IterativeParseNext<rj::kParseDefaultFlags>(m_stream, m_dispatcher))
			return false;
		if (m_dispatcher.m_suspended) {
			return true;
		}
	}
	return false;
}

void JSONParser::BuildColumns() {
	m_top.BuildColumns(m_schema);
}

void JSONTopListStruct::BuildColumns(JSONSchema &schema) {
	JSONBuildColumns(schema.columns, m_children.keys, m_columns, cur_row);
}

void JSONParser::BindSchema(vector<LogicalType> &return_types, vector<string> &names) {
	m_top.BindSchema(return_types, names);
}

void JSONTopListStruct::BindSchema(vector<LogicalType> &return_types, vector<string> &names) {
	for (auto &col : m_columns) {
		names.push_back(col->column.GetName());
		return_types.push_back(col->column.GetType());
	}
	names.push_back(col_row_number.GetName());
	return_types.push_back(col_row_number.GetType());
	m_row_number = 0;
}

idx_t JSONParser::FillChunk(DataChunk &output)
{
	m_top.NewChunk(output);
	if (do_parse()) {
		return STANDARD_VECTOR_SIZE;
	}
	close();
	is_finished = true;
	return m_top.cur_row;
}

void JSONTopListStruct::NewChunk(DataChunk &output) {
	cur_row = 0;
	size_t n_columns = m_columns.size();
	D_ASSERT(output.data.size() == n_columns + 1);
	for (size_t i = 0; i < n_columns; ++i) {
		m_columns[i]->column.SetVector(&output.data[i]);
	}
	col_row_number.SetVector(&output.data[n_columns]);
}

}
