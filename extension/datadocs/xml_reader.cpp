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

XMLParser::XMLParser(std::shared_ptr<BaseReader> reader) :
	m_reader(reader)
{}

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

bool XMLParser::do_parse(XMLHandlerBase& handler)
{
	XML_Parser parser = handler.create_parser();
	const int BUFF_SIZE = 4096;
	while (true)
	{
		void* buff = XML_GetBuffer(parser, BUFF_SIZE);
		if (!buff)
			break;
		size_t bytes_read = m_reader->read((char*)buff, BUFF_SIZE);
		if (!XML_ParseBuffer(parser, bytes_read, bytes_read == 0))
			break;
		if (bytes_read == 0)
			break;
	}
	enum XML_Error status = XML_GetErrorCode(parser);
	return status != XML_ERROR_ABORTED;
}

}
