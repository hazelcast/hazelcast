---
title: File Connector
description: Description of the SQL File connector
id: version-4.4-file-connector
original_id: file-connector
---

The File connector supports reading from both local and remote files.

To work with files you must specify location and serialization.
Any options not recognized by Jet are passed, in case of remote files,
directly to Hadoop client. For local files they are simply ignored. To
find out more about additional options consult [file source documentation](
../api/sources-sinks#supported-storage-systems).

## Location Options

`path` is an absolute path to the directory containing your data. These
are the supported schemes:

* `hdfs`: HDFS
* `s3a`: Amazon S3
* `wasbs`: Azure Cloud Storage
* `adl`: Azure Data Lake Generation 1
* `abfs`: Azure Data Lake Generation 2
* `gs`: Google Cloud Storage

So for instance, path to data residing in a Hadoop cluster could look
like `hdfs://path/to/directory/`. Any path not starting with any of the
above is considered local (i.e. files residing on Jet members), e.g.
`/path/to/directory/`.

`glob` is a pattern to filter the files in the specified directory.
The default value is `*`, matching all files.

### Ignore File Not Found

When you create a mapping without a column list, the location specified
by the `path` option is expected to contain at least one file matching
the `glob`, otherwise an exception is thrown. This is to avoid hard to
catch mistakes, such as typos. If you simply want to return zero
results, set the `ignoreFileNotFound` option to `true`. Note that in
this case you must specify the column list.

This option is not valid for [file table
functions](#file-table-functions), because they always need at least one
record to derive the column list from.

The default value is `false`.

### Shared File System option

In case the `path` points to a location which is shared between the
members, e.g. network mounted filesystem, you should set the
`sharedFileSystem` option to `true`. The files will be assigned among
the members and reading the files multiple times (once on each member)
will be avoided.

The default value is `false`.

## Serialization Options

`format` defines the serialization used to read the files. We assume all
records in files have the same format. These are the supported `format`
values:

* `csv`
* `json`
* `avro`
* `parquet`: remote files only

If you omit a file list from the `CREATE MAPPING` command, Jet will read
a sample file and try to determine column names and types from it. In
some cases you can use a different type if you specify the columns
explicitly. For example, the CSV format uses `VARCHAR` for all fields -
if you specify `DATE` manually, the behavior would be as if `CAST(column
AS DATE)` was used, using the same rules for conversion from `VARCHAR`
to `DATE` as `CAST` uses.

Also if you don't specify the columns, the directory needs to be
available at the time you execute the `CREATE MAPPING` and it must not
be empty. In case of local files, every cluster member must have some
file. If you specify the columns, an empty directory is OK.

See the examples for individual serialization options below.

### CSV Serialization

The `csv` files are expected to be comma-separated and `UTF-8` encoded.
Each file must have a header on the first line. If you omit the column
list from the mapping declaration, Jet will try to infer the column
names from the file header. All columns will have `VARCHAR` type.

```sql
CREATE MAPPING my_files
TYPE File
OPTIONS (
    'path' = '/path/to/directory',
    'format' = 'csv'
)
```

### JSON serialization

The `json` files are expected to contain one valid json document per
line and be `UTF-8` encoded. If you skip mapping columns from the
declaration, we infer names and types based on a sample.

```sql
CREATE MAPPING my_files
TYPE File
OPTIONS (
    'path' = '/path/to/directory',
    'format' = 'json'
)
```

#### Mapping Between JSON and SQL Types

| JSON type | SQL Type  |
| - | - |
| `BOOLEAN` | `BOOLEAN` |
| `NUMBER` | `DOUBLE` |
| `STRING` | `VARCHAR` |
| all other types | `OBJECT` |

### Avro & Parquet Serialization

The `avro` & `parquet` files are expected to contain Avro records.

```sql
CREATE MAPPING my_files
TYPE File
OPTIONS (
    'path' = '/path/to/directory',
    'format' = 'avro'
)
```

```sql
CREATE MAPPING my_files
TYPE File
OPTIONS (
    'path' = 'hdfs://path/to/directory',
    'format' = 'parquet'
    /* more Hadoop options ... */
)
```

#### Mapping Between Avro and SQL Types

| Avro Type | SQL Type |
| - | - |
| `BOOLEAN` | `BOOLEAN` |
| `INT` | `INT` |
| `LONG` | `BIGINT` |
| `FLOAT` | `REAL` |
| `DOUBLE` | `DOUBLE` |
| `STRING` | `VARCHAR` |
| all other types | `OBJECT` |

## External Column Name

You rarely need to specify the columns in DDL. If you do, you might want
to specify the external name. We don't support nested fields, hence the
external name should refer to the top-level field - not containing any
`.`.

## File Table Functions

To execute an ad hoc query against data in files you can use one of the
predefined table functions:

* `csv_file`
* `json_file`
* `avro_file`
* `parquet_file`

Table functions will create a temporary mapping, valid for the duration
of the statement. They accept the same options as `CREATE MAPPING`
statements.

You can use positional arguments:

```sql
SELECT * FROM TABLE(
  CSV_FILE('/path/to/directory', '*.csv', MAP['key', 'value'])
)
```

Or named arguments:

```sql
SELECT * FROM TABLE(
  CSV_FILE(path => '/path/to/directory', options => MAP['key', 'value'])
)
```

## Installation

Depending on what formats you want to work with you need different
modules on the classpath. Consult [file source documentation](
../api/sources-sinks#the-format) for details.
