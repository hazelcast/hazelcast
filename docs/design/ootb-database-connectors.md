# Relational database connector

### Table of Contents

+ [Background](#background)
  - [Description](#description)
  - [Terminology](#terminology)
  - [Actors and Scenarios](#actors-and-scenarios)
+ [Functional Design](#functional-design)
  * [Summary of Functionality](#summary-of-functionality)
  * [Additional Functional Design Topics](#additional-functional-design-topics)
    + [Notes/Questions/Issues](#notesquestionsissues)
+ [User Interaction](#user-interaction)
  - [API design and/or Prototypes](#api-design-andor-prototypes)
+ [Technical Design](#technical-design)
+ [Testing Criteria](#testing-criteria)
+ [Other Artifacts](#other-artifacts)

|||
|---|---|
|Related Jira|https://hazelcast.atlassian.net/browse/HZ-926|
|Related Github issues|_GH issue list_|
|Document Status / Completeness|DRAFT|
|Requirement owner|Jiri Holusa|
|Developer(s)|Frantisek Hartman|
|Quality Engineer|_Quality Engineer_|
|Support Engineer|_Support Engineer_|
|Technical Reviewers|_Technical Reviewers_|
|Simulator or Soak Test PR(s) |_Link to Simulator or Soak test PR_|

### Background
#### Description

The main goals of this task are:
- Provide zero-code way to setup read-through/write-through caching,
- Without any requirement for Java code (or any other code, only configuration),
- Primary use in Cloud with Amazon RDS.

The motivation and goals are described in detail as part of the epic in
[HZ-926](https://hazelcast.atlassian.net/browse/HZ-926).

The minimal solution consists of two main implementation tasks:
- implementing a ready-made MapStore/Loader for JDBC
- implementing MapStore/Loader operation offloading

In addition to that, the following areas of improvements have been 
identified and researched:

- external data store configuration - configuration of the data store and
  runtime resource management
- reuse of code for MapStore/Loader and Jet Source/Sink
- Using Hazelcast SQL to implement the MapStore/Loader
- Using a Jet job for the write-behind functionality

#### Terminology

|Term|Definition|
|---|---|
|External Data Store|3rd party data management system, usually some kind of database, e.g. PostgreSQL, MySQL, Cassandra, Mongodb|
|||

### Functional Design

#### External data store configuration

External data stores are used for several different use cases. Currently, 
the configuration depends on the use case, e.g. if I use JDBC Jet source 
I provide a JDBC URL in the Sources.jdbc(..) API, if JDBC 
MapStore/Loader is used the url is provided as part of the MapStore 
configuration.

New configuration should be introduced. This would decouple the 
configuration of the data store and it's use.

```xml
<hazelcast>

  <external-data-store name="my-mysql-database">
    <class-name>com.hazelcast.datastore.JdbcDataStoreFactory</class-name>
    <properties>
      <property name="jdbcUrl">jdbc:mysql://mysql.example.org:3306</property>
      <property name="username">my_user</property>
      <property name="password">my_password</property>
    </properties>
    <shared>true</shared>
  </external-data-store>

  <map name="my-map">
    <map-store enabled="true">
      <class-name>com.hazelcast.mapstore.GenericMapStore</class-name>
      <properties>
        <property name="external-data-store-ref">my-mysql-database</property>
      </properties>
    </map-store>
  </map>
</hazelcast>
```

It could be used also in a Jet job:

```java

class MyClass {
    public static void main(String[] args) {
        Sources.jdbc(externalDataStoreRef("mysql-database"), "SELECT * ..", (rs) -> ...)
        
    }
}

```

To support cloud effort this configuration should be dynamic - meaning
that it should be possible to add new `external-data-store` at runtime,
possibly also update it when not used anywhere.

Alternative: SQL could be used to add/modify the data store configuration.

### Resource reuse

Optionally, also runtime resources, could be shared for particular configuration.
This would typically be a connection pool shared across different jobs and/or map loaders.

This item is not required for JDBC based MapStore/Loader. It would be highly
beneficial for Jet JDBC source/sink (which is a prerequisite for SQL based
MapStore).

We also have seen requirements in the past to share resources between jobs 
for e.g. Kafka producer/consumer or for a client accessing remote map (tenths or
hundredths of jobs accessing 

### Code reuse

MapStore and Jet Source/Sink for a particular data store share code for
init, connection handling, error handling.

The idea is to reduce the complexity of implementing support for a new data
store across all Hazelcast features - namely MapStore, Jet Source/Sinks and SQL.

#### External DataStore abstraction

First option considered was to create an abstraction for a generic data store
interface, which could be used to implement MapStore and Jet Source/Sink.

The interface would need to support lookups/stores/deletes by key (or set of
keys) as well as lazily loading any data by native query - to support Jet
Source/Sink. 

The current Jdbc Jet Source leaks the JDBC API, and also uses JTA transactions,
which we would probably need to expose as well in this interface.

Based on these this solution was ruled out as too complex and over-engineered.

#### Using Hazelcast SQL to implement the MapStore/Loader

It is possible to implement the MapStore using our Hazelcast SQL engine, if we
already support the datastore in SQL. Currently, this is not the case for JDBC
(or any other data store), but it would give us huge incentive to implement
these. 

Supporting new data store across whole platform would then mean
- write Jet source/sink
- write SQL support for the source sink
- enabling the data store in the SQL base MapStore - there would be a single
  MapStore implementation with a set of supported datastores.

SQL currently returns a tuple (`SqlRow`) according to the mapping, we want to
convert it to compact. To avoid double conversion it would be best if SQL
allowed to return _native_ format - whatever the connector returns.

#### Alternative - Using light jobs to implement the MapStore/Loader

If there are any (temporary) blockers on SQL side we could alternatively use Jet
light jobs to implement the MapStore.

The drawback is that we need to implement the MapStore separately for each data
store (however we don't need to reimplement the init, error handling code
etc..).

It might also be easier to implement and maintain the MapStore on top of the
native data store client/driver.

#### Using a Jet job for the write-behind functionality

The write-behind functionality could be re-implemented using a Jet job,
which would provide better guarantees during cluster changes. However, 
it's not clear if there is demand for such improvement and such rewrite 
is risky.

There are several technical obstacles with unclear solution

- managing a lifecycle of such Jet job
- special handling of entry expirations

Due to these factors this wasn't investigated further.

##### Notes/Questions/Issues

Each step of adding a new MapStore configuration to a map should be
independently verifiable
- data store configuration (e.g. jdbc url & auth) - can we connecto to the
  database?
This could be part of the resource managing service, to make it available in all
clients the easiest is probably to expose the configuration via SQL

### User interaction

#### API design and/or Prototypes

The API / configuration for adding a map loader to a map exists.
MapLoader can be triggered by IMap.loadAll()

Configuration for a datastore is proposed in [External data store configuration](#External data store configuration)

#### Client Related Changes

The usability of this feature from other languages depends on the
availability of

- Adding map configuration dynamically (currently there is no public
  API in any client)
- Using IMap.loadAll methods (available in some clients)

### Development flow

The configuration can be re-created in development
(either new run of unit test or local Hz instance can be restarted).

This doesn't work well if cloud is used for development.

### Production flow

#### Enabling MapStore on an existing map

It's questionable if this is actually needed. This is not planned in scope.

#### Adding a new Map with MapStore configuration

A map config with MapStore configured can be added dynamically either 
via
- Java client
- REST interface to update config
- Update configuration file & reload

#### Changes in the schema (adding a column)

If the underlying schema changes, e.g. a column is added, it's likely 
that the added column is needed in the cached records as well, implying 
a complete reload of all cached data, roughly equivalent to replacing 
the map with a new map.

The problem is that the configuration can't be updated even when the 
map is removed. We should allow to update config for a destroyed map.

This wasn't implemented in 5.2, it is planned in 5.3 scope.

### Testing Criteria

Integration tests using Testcontainer will run as part of regular test suite.

Additional integration tests with Amazon RDS will run as part of either nightly
test suite or separate job run at regular interval.

A soak test or simulator test will be added to prevent any memory leaks.

Simple smoke test for K8s environment will be considered - will the JDBC URL
configuration work well in K8s network environment?

### Other Artifacts

Configuration Draft

SQL based DataStore POC
[SQL based MapLoader POC](https://github.com/frant-hartm/hazelcast/tree/poc/sql-based-maploader)

