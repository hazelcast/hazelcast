# Example user defined function queries

```sql
create function myfunjs(x varchar) RETURNS varchar
LANGUAGE 'js'
AS `x + '/' + x`

SELECT x, myfunjs(x) as c
FROM (VALUES ('a'), ('b')) AS t (x)

create function factorial(x BIGINT) RETURNS BIGINT
LANGUAGE 'js' AS `
function factorial_impl(v) {
    var result = 1;
    for(var i=2;i<=v;i++)
        result *= i;
    return result;
}
factorial_impl(x);`

Select v, factorial(v)
From table(generate_stream(1))

create function is_prime(n BIGINT) RETURNS varchar
LANGUAGE 'groovy'
AS `
def isPrime(i) { i <=2 || (2..Math.sqrt(i)).every { i % it != 0 } }
isPrime(n) ? "prime" : "composite"
`

Select v as "v", is_prime(v) as "v prime?", factorial(v) as "v!",
       factorial(v)-1 as "v!-1", is_prime(factorial(v) - 1) as "v!-1 prime?"
From table(generate_stream(1))

create or replace function sqlUpperJoin(x VARCHAR) RETURNS varchar
LANGUAGE 'js'
AS `sql.execute('select UPPER(?) || ?', x, x).scalar()`

SELECT sqlUpperJoin(x) as c FROM (VALUES ('a'), ('b')) AS t (x)

create or replace function now() RETURNS bigint
LANGUAGE 'js'
AS `java.lang.System.currentTimeMillis()`

SELECT now() from table(generate_stream(1))


create or replace function https_get(server varchar, url varchar) RETURNS varchar
LANGUAGE 'python'
AS
`
import httplib, urllib
conn = httplib.HTTPSConnection(server)
conn.request("GET", url)
response = conn.getresponse()
html = response.read()
conn.close()
result = html
`

SELECT now() as "now",
    https_get('www.uuidtools.com', '/api/generate/v1') as uuid,
    https_get('raw.githubusercontent.com', '/hazelcast/hazelcast/master/README.md') as hz


CREATE or replace MAPPING sqlCatalog
EXTERNAL NAME "__sql.catalog"
TYPE IMap
OPTIONS (
'keyFormat'='java',
'keyJavaClass' = 'java.lang.String',
'valueFormat' = 'java',
'valueJavaClass' = 'java.lang.Object'
)

SELECT * FROM sqlCatalog

SELECT __key, get_ddl('relation', __key) FROM sqlCatalog

```