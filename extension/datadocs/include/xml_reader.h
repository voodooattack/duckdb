#ifndef XML_READER_H
#define XML_READER_H

#include <string>

#include "inferrer_impl.h"
#include "file_reader.h"

struct XML_ParserStruct;

namespace duckdb {

class XMLHandlerBase
{
public:
	~XMLHandlerBase();
	bool parse_string(const std::string& input);
	XML_ParserStruct* create_parser();
protected:
	static void do_text(void* userdata, const char* buf, int buflen);
	bool check_new_text();
	void abort();
	XML_ParserStruct* m_parser = nullptr;
	std::string m_cur_text;
	void (*p_start)(void*, const char*, const char**) = nullptr;
	void (*p_end)(void*, const char*) = nullptr;
};

template <class Handler>
class XMLHandler : public XMLHandlerBase
{
protected:
	XMLHandler()
	{
		p_start = do_start;
		p_end = do_end;
	}
private:
	static void do_start(void* userdata, const char* name, const char** atts)
	{
		Handler* handler = static_cast<Handler*>(userdata);
		if (handler->check_new_text() && !handler->new_text())
			handler->abort();
		else if (!handler->start_tag(name, atts))
			handler->abort();
	}
	static void do_end(void* userdata, const char*)
	{
		Handler* handler = static_cast<Handler*>(userdata);
		if (handler->check_new_text() && !handler->new_text())
			handler->abort();
		else
			handler->end_tag();
	}
};

class XMLBase
{
public:
	//static XMLBase* create(const IngestColumnDefinition& col, Row::ColumnAdapter* adapter);
	virtual ~XMLBase() = default;
	virtual XMLBase* new_tag(const char* name, const char** atts) { return nullptr; }
	virtual void first_tag() {}
	virtual bool second_tag() { return false; }
	virtual void end_tag() {}
	virtual bool new_text(std::string&& s) { return true; }
	bool m_saw_tag;
};

class XMLRoot : public XMLBase
{
public:
	void assign(XMLBase* xml) { m_root.reset(xml); }
	virtual ~XMLRoot() = default;
	virtual XMLBase* new_tag(const char* name, const char** atts) override
	{
		m_root->first_tag();
		return m_root.get();
	}
//protected:
	std::unique_ptr<XMLBase> m_root;
};

class XMLParser : public ParserImpl
{
public:
	XMLParser(std::shared_ptr<BaseReader> reader);
	virtual ~XMLParser() override;
	virtual bool do_infer_schema() override;
	virtual Schema* get_schema() override { return &m_schema; }
	virtual bool open() override;
	virtual void close() override;
	virtual void BuildColumns() override;
	virtual void BindSchema(vector<LogicalType> &return_types, vector<string> &names) override;
	virtual idx_t FillChunk(DataChunk &output) override;
	virtual int get_percent_complete() override;

protected:
	Schema m_schema;
	std::shared_ptr<BaseReader> m_reader;
	XMLRoot root;
};

}

#endif