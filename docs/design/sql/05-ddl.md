# SQL DDL

## Overview

This part contains reference information for the DDL commands supported by Hazelcast SQL subsystem.

## 1 CREATE EXTERNAL TABLE

### 1.1 Name

`CREATE EXTERNAL TABLE` -- define a new external table

### 1.2 Synopsis

```sql
CREATE [OR REPLACE] EXTERNAL TABLE [IF NOT EXISTS] table_name [ (
 column_name data_type [ EXTERNAL NAME external_name ] [, ... ]
) ]
TYPE table_type
[ OPTIONS ( table_parameter table_parameter_value [, ... ] ) ]
```

### 1.3 Description

`CREATE EXTERNAL TABLE` registers a virtual table that references an external storage system. For some storage systems,
`CREATE EXTERNAL TABLE` does not create a physical entity until a write occurs.

The table is created in the `public` schema. The name of the table must be distinct from the name of any other table in
the schema.

Column list is optional, for some storage systems and serialization formats, it is possible to derive it.

### 1.4 Parameters

**table_name**

The unqualified name of the table to be created.

**column_name**

The name of a column in the table.

**data_type**

The data type of the column.

| Name | Aliases | Description |
|---|---|---|
| BOOLEAN | | logical Boolean (true/false) |
| TINYINT | | signed one-byte integer |
| SMALLINT | | signed two-byte integer |
| INT | INTEGER | signed four-byte integer |
| BIGINT | | signed eight-byte integer |
| FLOAT | REAL | single precision floating-point number (4 bytes) |
| DOUBLE [PRECISION] | | double precision floating-point number (8 bytes) |
| DECIMAL [ (_p_, 0) ] | DEC [ (_p_, 0) ], NUMERIC [ (_p_, 0) ] | arbitrary-precision integer |
| DECIMAL [ (_p_, _s_) ] | DEC [ (_p_, _s_) ], NUMERIC [ (_p_, s) ] | arbitrary-precision signed decimal number |
| CHARACTER | CHAR | single character |
| CHARACTER VARYING | VARCHAR | variable-length character string |
| TIME | | time (no time zone) |
| DATE | | date (no time zone) |
| TIMESTAMP WITHOUT TIME ZONE | TIMESTAMP | date-time (no time zone) |
| TIMESTAMP WITH TIME ZONE | TIMESTAMP WITH TIME ZONE (OFFSET_DATE_TIME) | date-time with an offset from UTC/Greenwich in the ISO-8601 calendar system |
| TIMESTAMP WITH TIME ZONE (ZONED_DATE_TIME) | | date-time with a time-zone in the ISO-8601 calendar system |
| TIMESTAMP WITH TIME ZONE (CALENDAR) | | specific instant in time with a time-zone, with millisecond precision |
| TIMESTAMP WITH LOCAL TIME ZONE | TIMESTAMP WITH LOCAL TIME ZONE (INSTANT) | instantaneous point on the time-line |
| TIMESTAMP WITH LOCAL TIME ZONE (DATE) | | specific instant in time, with millisecond precision |

For more information on the supported data types, refer to [type system document](https://github.com/hazelcast/hazelcast/blob/master/docs/design/sql/01-type-system.md).

**external_name**

The path to the field in the external storage system.

**table_type**

The external storage system identifier.

**table_parameter**

Parameter specific to the external storage system - connection string, serialization format etc.

**table_parameter_value**

A string literal.

### 1.5 Examples

- Create an external table referencing an `IMap` with inferred schema.

  ```sql
  CREATE EXTERNAL TABLE persons
  TYPE "com.hazelcast.IMap"
  OPTIONS (
    "serialization.key.format" 'java',
    "serialization.key.java.class" 'java.lang.Integer',
    "serialization.value.format" 'java',
    "serialization.value.java.class" 'java.lang.String'
  )
  ```

  Column names and types are inferred as:

  ```sql
  (
    __key INT,
    this VARCHAR
  )
  ```

- Create an external table referencing an `IMap` with explicit schema.

  ```sql
  CREATE EXTERNAL TABLE persons (
    id INT EXTERNAL NAME __key,
    name VARCHAR
  )
  TYPE "com.hazelcast.IMap"
  OPTIONS (
    "serialization.key.format" 'java',
    "serialization.key.java.class" 'java.lang.Integer',
    "serialization.value.format" 'java',
    "serialization.value.java.class" 'java.lang.String'
  )
  ```

- Create an external table referencing an `IMap` with explicit schema and column external names.

  ```sql
  CREATE EXTERNAL TABLE persons (
    key_id INT EXTERNAL NAME "__key.id",
    value_id INT EXTERNAL NAME "this.id"
  )
  TYPE "com.hazelcast.IMap"
  OPTIONS (
    "serialization.key.format" 'java',
    "serialization.key.java.class" 'com.hazelcast.Person',
    "serialization.value.format" 'java',
    "serialization.value.java.class" 'com.hazelcast.Person'
  )
  ```

- Create an external table referencing an `IMap` with schema inferred from `Portable` class definitions.

  ```sql
  CREATE EXTERNAL TABLE persons
  TYPE "com.hazelcast.IMap"
  OPTIONS (
    "serialization.key.format" 'portable',
    "serialization.key.portable.factoryId" '1',
    "serialization.key.portable.classId" '2',
    "serialization.key.portable.classVersion" '3',
    "serialization.value.format" 'portable',
    "serialization.value.portable.factoryId" '4',
    "serialization.value.portable.classId" '5',
    "serialization.value.portable.classVersion" '6'
  )
  ```

- Create an external table referencing an `IMap` with JSON as the serialization format.

  ```sql
  CREATE EXTERNAL TABLE persons (
    id INT EXTERNAL NAME "__key.id",
    name VARCHAR EXTERNAL NAME "this.name"
  )
  TYPE "com.hazelcast.IMap"
  OPTIONS (
    "serialization.key.format" 'json',
    "serialization.value.format" 'json'
  )
  ```

## 2 CREATE EXTERNAL TABLE AS

### 2.1 Name

`CREATE EXTERNAL TABLE AS` -- define a new external table from the results of a query

### 2.2 Synopsis

```
CREATE [OR REPLACE] EXTERNAL TABLE [IF NOT EXISTS] table_name [ (
 column_name data_type [ EXTERNAL NAME external_name ] [, ... ]
) ]
TYPE table_type
[ OPTIONS ( table_parameter table_parameter_value [, ... ] ) ]
AS query
```

### 2.3 Description

`CREATE EXTERNAL TABLE AS` registers a virtual table that references an external storage system. It fills the table with
data computed by `SELECT` statement. The table columns have the names and data types associated with the output columns
of the `SELECT` (except that column names can overridden by an explicit list of column names).

The table is created in the `public` schema. The name of the table must be distinct from the name of any other table in
the schema.

### 2.4 Parameters

**table_name**

The unqualified name of the table to be created.

**column_name**

The name of a column in the table.

**data_type**

The data type of the column.

| Name | Aliases | Description |
|---|---|---|
| BOOLEAN | | logical Boolean (true/false) |
| TINYINT | | signed one-byte integer |
| SMALLINT | | signed two-byte integer |
| INT | INTEGER | signed four-byte integer |
| BIGINT | | signed eight-byte integer |
| FLOAT | REAL | single precision floating-point number (4 bytes) |
| DOUBLE [PRECISION] | | double precision floating-point number (8 bytes) |
| DECIMAL [ (_p_, 0) ] | DEC [ (_p_, 0) ], NUMERIC [ (_p_, 0) ] | arbitrary-precision integer |
| DECIMAL [ (_p_, _s_) ] | DEC [ (_p_, _s_) ], NUMERIC [ (_p_, s) ] | arbitrary-precision signed decimal number |
| CHARACTER | CHAR | single character |
| CHARACTER VARYING | VARCHAR | variable-length character string |
| TIME | | time (no time zone) |
| DATE | | date (no time zone) |
| TIMESTAMP WITHOUT TIME ZONE | TIMESTAMP | date-time (no time zone) |
| TIMESTAMP WITH TIME ZONE | TIMESTAMP WITH TIME ZONE (OFFSET_DATE_TIME) | date-time with an offset from UTC/Greenwich in the ISO-8601 calendar system |
| TIMESTAMP WITH TIME ZONE (ZONED_DATE_TIME) | | date-time with a time-zone in the ISO-8601 calendar system |
| TIMESTAMP WITH TIME ZONE (CALENDAR) | | specific instant in time with a time-zone, with millisecond precision |
| TIMESTAMP WITH LOCAL TIME ZONE | TIMESTAMP WITH LOCAL TIME ZONE (INSTANT) | instantaneous point on the time-line |
| TIMESTAMP WITH LOCAL TIME ZONE (DATE) | | specific instant in time, with millisecond precision |

For more information on the supported data types, refer to [type system document](https://github.com/hazelcast/hazelcast/blob/master/docs/design/sql/01-type-system.md).

**external_name**

The path to the field in the external storage system.

**table_type**

The external storage system identifier.

**table_parameter**

Parameter specific to the external storage system - connection string, serialization format etc.

**table_parameter_value**

A string literal.

**query**

A query statement (that is, a `SELECT` statement).

### 2.5 Examples

- Create an external table referencing an `IMap` and filling it with data from files in the given directory.

  ```sql
  CREATE EXTERNAL TABLE persons (
    id INT EXTERNAL NAME "__key"
  )
  TYPE "com.hazelcast.IMap"
  OPTIONS (
    "serialization.key.format" 'java',
    "serialization.key.java.class" 'java.lang.Integer',
    "serialization.value.format" 'java',
    "serialization.value.java.class" 'java.lang.String'
  )
  AS SELECT id, name FROM TABLE (
    FILE ('avro', '/path/to/directory')
  )
  ```

- Create an external table referencing an `IMap` and filling it with data from existing table.

  ```sql
  CREATE EXTERNAL TABLE persons (
    id INT EXTERNAL NAME "__key"
    name VARCHAR EXTERNAL NAME "this"
  )
  TYPE "com.hazelcast.IMap"
  OPTIONS (
    "serialization.key.format" 'java',
    "serialization.key.java.class" 'java.lang.Integer',
    "serialization.value.format" 'java',
    "serialization.value.java.class" 'java.lang.String'
  )
  AS SELECT __key, this FROM employees
  ```

## 3 DROP EXTERNAL TABLE

### 3.1 Name

`DROP EXTERNAL TABLE` -- remove external table

### 3.2 Synopsis

```sql
DROP EXTERNAL TABLE [IF EXISTS] table_name
```

### 3.3 Description

`DROP EXTERNAL TABLE` removes the table from the catalog. It removes just a reference to the external storage system, it
does NOT delete any physical entity nor data.

### 3.4 Parameters

**table_name**

The unqualified name of the table to be removed.

### 3.5 Examples

```sql
DROP EXTERNAL TABLE orders
```