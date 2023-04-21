#include <string_view>

#include "rapidjson/reader.h"

#include "json_reader.h"
#include "utility.h"

using namespace std::literals;
namespace rj = rapidjson;

namespace Ingest {

class BaseReaderStream
{
public:
	typedef char Ch;
	BaseReaderStream(BaseReader& reader) : m_reader(reader) {}
	Ch Peek() { Ch c; if (!m_reader.peek(c)) return '\0'; return c; }
	Ch Take() { Ch c; if (!m_reader.next_char(c)) return '\0'; return c; }
	size_t Tell() const { return m_reader.tell(); }

	// Not implemented
	void Put(Ch) { RAPIDJSON_ASSERT(false); }
	void Flush() { RAPIDJSON_ASSERT(false); } 
	Ch* PutBegin() { RAPIDJSON_ASSERT(false); return 0; }
	size_t PutEnd(Ch*) { RAPIDJSON_ASSERT(false); return 0; }

	// For encoding detection only.
	const Ch* Peek4() { return m_reader.peek_start(4); }

private:
	BaseReader& m_reader;
};

bool JSONHandler::EndObject(JSONDispatcher* dispatcher) { dispatcher->pop(); return true; }
bool JSONHandler::EndArray(JSONDispatcher* dispatcher) { dispatcher->pop(); return true; }

class JSONRoot : public JSONHandler
{
public:
	JSONRoot(JSONHandler* top, const JSONSchema& schema) : m_level(0), m_start_path(schema.start_path), m_top(top) {}
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
		dispatcher->push(m_top);
		dispatcher->m_value = m_top;
		return true;
	}
	virtual bool EndArray(JSONDispatcher* dispatcher) override { return false; }
protected:
	size_t m_level;
	const std::vector<std::string>& m_start_path;
	JSONHandler* m_top;
};

bool JSONDispatcher::parse_string(const std::string& input, JSONHandler* handler)
{
	m_stack.clear();
	m_top = nullptr;
	m_value = handler;
	rj::StringStream ss(input.data());
	return rj::Reader().Parse(ss, *this);
}

/*void JSONDispatcher::parse_file(BaseReader* reader, JSONHandler* top, const JSONSchema& schema)
{
	JSONRoot root(top, schema);
	m_stack.clear();
	m_top = m_value = &root;
	BaseReaderStream stream(*reader);
	rj::Reader().Parse(stream, *this);
}*/

JSONParser::JSONParser(std::shared_ptr<BaseReader> reader) :
	m_reader(reader)
{}

JSONParser::~JSONParser()
{
	close();
}

bool JSONParser::open()
{
	if (m_reader->is_file())
		return m_reader->open();
	return false;
}

void JSONParser::close()
{
	m_reader->close();
}

int JSONParser::get_percent_complete()
{
	return m_reader->pos_percent();
}

void JSONParser::do_parse(JSONDispatcher& dispatcher)
{
	BaseReaderStream stream(*m_reader);
	rj::Reader reader;
	reader.Parse(stream, dispatcher);
}

}
