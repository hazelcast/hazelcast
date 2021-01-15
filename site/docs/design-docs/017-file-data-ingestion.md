---
title: 017 - File Data Ingestion
description: Unified API for reading files and improved packaging for cloud sources
---

*Since*: 4.4

## 1. Support for common formats and data sources

Format and source of the data are seemingly orthogonal. You can read
plain text files/csv/avro/.. etc from a local filesystem, S3 etc.

However, to take an advantage of some properties of certain formats
(e.g. parallel reading/deserialization of Avro files, selecting columns
in parquet …) requires combining these two steps (read Avro file header
to find out the beginning positions of data blocks).

This is already implemented in the Hadoop connector.

Required Formats

* CSV
We don’t have a connector
There is not official Hadoop connector (= InputFormat), open-source
implementations exist, with limitations (same as with Spark)

* JSON
We have a connector
There is not official Hadoop connector (= InputFormat), open-source
implementations exist

* Avro
We have a connector
There is official Hadoop connector

* Parquet
We don’t have a connector
There is official Hadoop connector

* ORC low priority

** [Reading ORC files](https://orc.apache.org/docs/core-java.html#reading-orc-files)

### Parquet

Parquet is a special case between the formats listed above - it defines
representation in the file (as others do), but doesn't have own schema
(class?) definition for deserialization into objects. Uses one of the
other serialization formats, most commonly thrift, avro.

The PRD doesn't specify the following commonly used formats:

* plain text,

* binary (e.g. images for ML)

* Protobuf

* Thrift

Sources

* local filesystem

* Amazon S3

* Azure Blob Storage

* Azure Data Lake Storage

* Google Cloud Storage.

* HDFS ? not listed in the PRD

## Unified approach for file data ingestion

We have the following possibilities

1. enforce naming convention, e.g.
`com.hazelcast.jet.x.XSources.x(...., formatY)`
`com.hazelcast.jet.s3.S3Sources.s3(..., Avro)`

2. new File API as a single entry point e.g.
`FileSources.s3(&quot;...&quot;).withFormat(AVRO)`

3. URL/ resource description style with auto detection, e.g.
`FileSource.read(&quot;s3://...../file.avro&quot;);`

We think we should go with 2., potentially write a parser for 3. to
delegate to 2. - would be used from SQL (there already is similar logic)

We decided to use Hadoop libraries to access all systems apart from
local filesystem, and Hadoop can detect the source we implemented 3.

### Using Hadoop libraries

Using Hadoop libraries is a must for reading data from HDFS.

To read data from other sources, like S3 it is possible to use custom
connectors (e.g. we have a connector for S3). Both approaches have some
advantages and disadvantages.

Using Hadoop libs

* `-` Complicated to setup, lots of dependencies, possible dependency
  conflicts.

* `+` Supports advanced features - can take advantage of the structure
  of the formats - e.g. read avro file in parallel, or read specific
columns in parquet.  +-? The hadoop api leaks (see
`com.hazelcast.jet.hadoop.HadoopSources#inputFormat(org.apache.hadoop.conf.Configuration,
com.hazelcast.function.BiFunctionEx<K,V,E>`) It is questionable if this
is an issue, exposing the hadoop API gives users more power.  For
performance difference see the benchmark below

Using specific s3/... connector

* `-` we would need to implement a connector for each source (currently we
    have local filesystem and S3), the S3 source connector is ~300 lines

* `-` we would miss the advanced features, reimplementing those would be
  a huge effort

* `+` Simpler packaging

* `+` Nicer API

* `+-`? Unknown performance compared to Hadoop libs

Benchmark S3 client based vs Hadoop based connectors

Benchmark summary: S3 is generally faster, apart from 1 large file case,
where Hadoop can split the file. The difference is not massive though
(18211 fastest for s3, vs 21306 for Hadoop, slowest 75752 for S3, 90071
for Hadoop)

We decided to use Hadoop to access all sources, with option without
hadoop for local filesystems

## 2. Loading data from a file Cookbook

There is a section in the manual describing the new API and each module.

There are examples how to read:

* binary files
* text files by lines
* avro files

## 3. Any other concerns

### Compression

Hadoop supports various compression formats, which work with built-in
`TextInputFormat`, used in our `FileFormat#lines()`. Other input formats
implemented by us do not support compression, but it is possible to
implement. It should be straightforward for e.g. the binary files, a bit
more complicated for splittable files without clear boundaries (e.g.
json).

Avro format has its own compression, we are able to read such compressed
files. See [Avro documentation](https://avro.apache.org/docs/1.10.1/spec.html#Required+Codecs).

Parquet format has its own compression, we are able to read such
compressed files.

### Selecting subset of columns

Possible for parquet only. Available with hadoop connector via option.

## 4. Overlap with Jet SQL

The main difference in how SQL would use the connector is that it
expects `Object[]` as return type.

We need to provide a way to configure each format as such.

## Design

Entry point - `com.hazelcast.jet.pipeline.file.FileSources`. Returns
`com.hazelcast.jet.pipeline.file.FileSourceBuilder`

There are 2 required parameters - path and format.

Path specifies the file location (accepts globs).
The format describes the format of the data in the file (text, csv,
json ..).

The files are either on a local filesystem, hdfs, or on one of
supported cloud storage systems, implemented using Hadoop
infrastructure.

User must provide correct module on CP.

Additionally, local files can be read using Hadoop infrastructure by
setting `useHadoopForLocalFiles` flag on the builder.

It is also possible to pass key/value String pairs as options. These
are passed to the Hadoop MR job configuration - needed for
authentication and available to use for any options for the format or
other needs.

### Local files

Reading from local file system is implemented in
`com.hazelcast.jet.pipeline.file.impl.LocalFileSourceFactory`.
The current infrastructure is reused -
`com.hazelcast.jet.core.processor.SourceProcessors.readFilesP
(java.lang.String, java.lang.String, boolean,
 com.hazelcast.function.FunctionEx<? super java.nio.file.Path,?
extends java.util.stream.Stream<I>>)`

Supporting a file format in LocalFileSourceFactory means implementing a
`ReadFileFnProvider` interface:

```java
public interface ReadFileFnProvider {

    <T> FunctionEx<Path, Stream<T>> createReadFileFn(FileFormat<T> format);

    String format();
}
```

The `createReadFileFn` creates, for a given file format, a function
that reads from a Path (a file on a local filesystem) and returns
a stream of items, which are emitted from the source.

### Cloud

Cloud storage systems are supported via Hadoop. Each storage system is
supported by a given module, which includes all dependencies.
The concrete storage is detected from the path prefix by the Hadoop
infrastructure.

Supporting a file format in HadoopFileSourceFactory means implementing
`JobConfigurer` interface:

```java
public interface JobConfigurer {

    <T> void configure(Job job, FileFormat<T> format);

    BiFunctionEx<?, ?, ?> projectionFn();
}
```

The `configure` configured the MR job with given `FileFormat`. This
typically means setting the InputFormat class for the MR job and its
parameters.

The `projectionFn` function converts `InputFormat`'s key-value result
to the item emitted from the source.

## Testing

Tests for each format are part of the Hadoop module, which runs them in
two modes:

* local mode

* using Hadoop local filesystem

This means that the tests for the local filesystem are not part of the
hazelcast-jet-core module where the implementation is.
On the other hand we reuse the same set of tests, so we have the same
coverage for both implementations.

Integrations tests are part of the hazelcast-qe pipeline, where the jobs
run in a cluster inside a docker container, with the connector fat jars.
This ensures that the fat jars contain correct dependencies.

## Licensing

We had to add couple of aliases for apache 2, BSD, new/revised BSD,
nothing new

What's new is:

* "The Go license" - this is permissive BSD style license,
[link](https://golang.org/LICENSE)

* CDDL (1.0) - Common Development and Distribution License - this is
a weak copyleft license, based on mozilla public license and its
variants

** CDDL 1.1 (minor update, something with patents and EU law)
** CDDL + GPLv2 with classpath exception - this is just dual CDDL
1.0 + GPLv2 license

All the CDDL libs are transitive dependencies of hadoop-common, which
we plan to package in the all deps included file connectors jars for
s3/gcp/azure.

Many commercial applications use CDDL dependencies (e.g. Spring
framework has many modules with transitive dependencies under CDDL)
