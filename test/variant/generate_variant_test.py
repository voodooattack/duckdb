import re
import duckdb

data = r'''
NULL
FALSE
TRUE
-120::TINYINT
254::UTINYINT
-32000::SMALLINT
65000::USMALLINT
-65000::INTEGER
2147483649::UINTEGER
-2::BIGINT
9223653507536580609::UBIGINT
-170141183460469231731687303715884105727::HUGEINT
3.14::FLOAT
3.14::DOUBLE
'2021-12-31'::DATE
'16:54:59.999999'::TIME
'16:54:59'::TIMETZ
'2021-12-31 16:54:59'::TIMESTAMP
'2021-12-31 16:54:59'::TIMESTAMPTZ
'1970-01-01 00:16:40'::TIMESTAMP_S
'1970-01-01 00:16:40'::TIMESTAMP_MS
'1970-01-01 00:16:40'::TIMESTAMP_NS
'1 year 1 month 18 days 02:46:40'::INTERVAL
'24090 days 06:49:58'::INTERVAL
''
'a string'
'Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua'
'6b6542ea-863f-4b10-b5ff-ab9ed50f4291'::UUID
'a\x00blob'::BLOB
3.141::DECIMAL(4,3)
3.14159265::DECIMAL(9,8)
3.14159265358979323::DECIMAL(18,17)
'-1.7014118346046923173168730371588'::DECIMAL(32,31)
'two'::ENUM_TYPE
[]
[NULL]
[]::BOOLEAN[]
[TRUE, FALSE, NULL, TRUE]
[NULL, -128, 127, 0]::TINYINT[]
[0, 127, 128, 255, NULL]::UTINYINT[]
[-32768, NULL, 32767]::SMALLINT[]
[65535]::USMALLINT[]
[2147483647, -1, 2, 3, 4, 5, 6, 7, 8, NULL, -2147483648]::INTEGER[]
[2147483649]::UINTEGER[]
[9223372036854775807, NULL, -9223372036854775808]::BIGINT[]
[100, 200]::UBIGINT[]
[-170141183460469231731687303715884105727, -85070591730234615865843651857942052863, NULL]::HUGEINT[]
[3.14, 2.71, 0.0, NULL, 340282346638528859811704183484516925440]::FLOAT[]
[3.14, 2.71, 0.0, NULL, 1.7976931348623157e+308]::DOUBLE[]
['2021-12-31', NULL, '2022-1-1']::DATE[]
['16:54:59.999999', '0:00:00', NULL]::TIME[]
['16:54:59.999999', '0:00:00', NULL]::TIMETZ[]
['2021-12-31 16:54:59', NULL, '2022-04-26 10:41:39']::TIMESTAMP[]
['2021-12-31 16:54:59', NULL, '2022-04-26 10:41:39']::TIMESTAMPTZ[]
['2021-12-31 16:54:59', NULL, '2022-04-26 10:41:39']::TIMESTAMP_S[]
['2021-12-31 16:54:59', NULL, '2022-04-26 10:41:39']::TIMESTAMP_MS[]
['2021-12-31 16:54:59', NULL, '2022-04-26 10:41:39']::TIMESTAMP_NS[]
['1 year 1 month 18 days 02:46:40', NULL, '10 years 10 months 1 day 00:00:10']::INTERVAL[]
['6b6542ea-863f-4b10-b5ff-ab9ed50f4291', NULL, '33b66900-6d7e-11ec-90d6-0242ac120003']::UUID[]
[3.141, NULL, -0.001]::DECIMAL(4,3)[]
[3.14159265, NULL, 0.00000001]::DECIMAL(9,8)[]
[3.14159265358979323, NULL, 0.00000000000000001]::DECIMAL(18,17)[]
[-1.70141183460469231731687303715884105727, NULL, 0.0000000000000000000000000000001]::DECIMAL(32,31)[]
[]::VARCHAR[]
['', NULL, 'a string']
['a\x00blob', NULL, '', 'a blob']::BLOB[]
['one', NULL, 'two', 'three']::ENUM_TYPE[]
[[1, 2, 3, NULL], [4, 5, 6], NULL, [], [1]]::SMALLINT[][]
[['a', 'bbb', 'cdefg', NULL], ['h', 'ij', 'klm'], NULL, [], ['', '']]
[{'bool': TRUE, 'string': 'hello', 'a list': [0, NULL, 127]::TINYINT[], 'null': NULL, 'another list': []::VARCHAR[], 'nested': {'child1': 123, 'child2': 'a string'}}, NULL, {'bool': FALSE, 'string': 'goodbye', 'a list': []::TINYINT[], 'null': NULL, 'another list': ['one', 'two'], 'nested': {'child1': 456, 'child2': 'also a string'}}]
{'bool': TRUE, 'string': 'hello', 'a list': [0, NULL, 127]::TINYINT[], 'null': NULL, 'another list': []::VARCHAR[], 'nested': {'child1': 123, 'child2': 'a string'}}
map(['key1', 'key2'], [1, 2])
'''

def cnv(s):
	if s is None:
		return 'NULL'
	elif isinstance(s, str) and not s:
		return '(empty)'
	#elif isinstance(s, bytes):
	#	return str(s)[2:-1]
	return s

def res_to_string(res):
	return '\n'.join('\t'.join(map(cnv, row)) for row in res)

def main():
	con = duckdb.connect(database=':memory:')
	con.execute("CREATE TYPE enum_type AS ENUM ('one', 'two', 'three');")
	f_name = 'test_variant.test'
	last_line = '# generated data\n'
	with open(f_name) as f:
		lines = list(iter(f.readline, last_line))
	with open(f_name, 'w') as f:
		f.writelines(lines)
		f.write(last_line)
		for d in data.strip().splitlines():
			q = f'SELECT VARIANT({d})'
			res = con.query(q+'::VARCHAR;').fetchall()
			blob = res_to_string(res)
			f.write(f'\nquery I\n{q};\n----\n{blob}\n')

			if blob != 'NULL':
				blob = blob.replace("'", r'\x27')
				blob = f"'{blob}'::BLOB"

			res = con.query(f'SELECT TYPEOF({d});').fetchall()
			tp = res[0][0]
			if tp.startswith('DECIMAL', 'LIST', 'STRUCT', 'MAP'):
				continue
			tp = {
				'NULL': 'VARCHAR',
				'TIME WITH TIME ZONE': 'TIMETZ',
				'TIMESTAMP WITH TIME ZONE': 'TIMESTAMPTZ',
				'TIMESTAMP (SEC)': 'TIMESTAMP_S',
				'TIMESTAMP (MS)': 'TIMESTAMP_MS',
				'TIMESTAMP (NS)': 'TIMESTAMP_NS',
				'enum_type': 'VARCHAR',
			}.get(tp, tp)
			old = con.query(f'SELECT {d};').fetchall()[0][0]
			q = f"SELECT FROM_VARIANT('{tp}', {blob})"
			res = con.query(q+';').fetchall()[0][0]
			assert res == old, f'{old} != {res}'
			res = con.query(q+'::VARCHAR;').fetchall()
			res = res_to_string(res)
			f.write(f'\nquery I\n{q};\n----\n{res}\n')

if __name__ == '__main__':
	main()
