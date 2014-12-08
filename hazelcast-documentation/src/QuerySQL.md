

### Distributed SQL Query

`com.hazelcast.query.SqlPredicate` takes the regular SQL `where` clause. Here is an example:

```java
IMap<Employee> map = hazelcastInstance.getMap( "employee" );
Set<Employee> employees = map.values( new SqlPredicate( "active AND age < 30" ) );
```

#### Supported SQL syntax:

**AND/OR:** `<expression> AND <expression> AND <expression>... `

- `active AND age>30`
- `active=false OR age = 45 OR name = 'Joe' `
- `active AND ( age > 20 OR salary < 60000 ) `

**Equality:** `=, !=, <, <=, >, >=`

- `<expression> = value`
- `age <= 30`
- `name = "Joe"`
- `salary != 50000`

**BETWEEN: ** `<attribute> [NOT] BETWEEN <value1> AND <value2>`

- `age BETWEEN 20 AND 33 ( same as age >= 20  AND age <= 33 )`
- `age NOT BETWEEN 30 AND 40 ( same as age < 30 OR age > 40 )`

**LIKE:** `<attribute> [NOT] LIKE 'expression'`

The `%` (percentage sign) is placeholder for multiple characters, an `_` (underscore) is placeholder for only one character.

- `name LIKE 'Jo%'` (true for 'Joe', 'Josh', 'Joseph' etc.)
- `name LIKE 'Jo_'` (true for 'Joe'; false for 'Josh')
- `name NOT LIKE 'Jo_'` (true for 'Josh'; false for 'Joe')
- `name LIKE 'J_s%'` (true for 'Josh', 'Joseph'; false 'John', 'Joe')

**IN:** `<attribute> [NOT] IN (val1, val2,...)`

- `age IN ( 20, 30, 40 )`
- `age NOT IN ( 60, 70 )`
- `active AND ( salary >= 50000 OR ( age NOT BETWEEN 20 AND 30 ) )`
- `age IN ( 20, 30, 40 ) AND salary BETWEEN ( 50000, 80000 )`


