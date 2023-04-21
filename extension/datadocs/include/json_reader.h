#ifndef JSON_READER_H
#define JSON_READER_H

#include "json.h"
#include "inferrer_impl.h"
#include "file_reader.h"

namespace Ingest {

class JSONParser : public ParserImpl
{
public:
	JSONParser(std::shared_ptr<BaseReader> reader);
	virtual ~JSONParser() override;
	virtual bool do_infer_schema() override;
	virtual Schema* get_schema() override { return &m_schema; }
	virtual bool open() override;
	virtual void close() override;
	virtual bool GetNextRow(Row &row) override;
	virtual int get_percent_complete() override;

protected:
	void do_parse(JSONDispatcher& dispatcher);

	JSONSchema m_schema;
	std::shared_ptr<BaseReader> m_reader;
};

}

#endif
