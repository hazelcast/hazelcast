---
title: Compatibility of XA Support
description: How to check the compatibility of XA Support for JMS brokers and JDBC databases.
id: version-4.1.1-xa
original_id: xa
---

Jet supports exactly-once processing internally, but in order to have
end-to-end exactly-once processing sources and sinks need to support
it too. Jet introduced XA transaction support, by which source and sink
will actively participate to achieve this goal. Basically Jet starts
transactions for the source, sink and any stage between these two,
prepares them and commit them only after snapshot completed on all
nodes.

We've found that many resources we use as either sinks or
sources, claiming to support XA have a common flaw: the transaction
doesn't survive a client disconnect. Their behavior is sufficient to
ensure atomicity (that is all distributed participants either commit or
all rollback), but not to achieve fault-tolerance.

## Testing

We've introduced a set of tests to check compatibility of the XA
support in your JMS broker or JDBC database with Jet's fault tolerance.
The tests check only one feature, namely that a prepared transaction
can be committed after the client reconnects.

1. Clone the [contrib repo](https://github.com/hazelcast/hazelcast-jet-contrib)

```bash
git clone git@github.com:hazelcast/hazelcast-jet-contrib.git
```

2. Head to `xa-tests` module.

```bash
cd hazelcast-jet-contrib/xa-tests
```

The module contains two tests:

- [JDBC Test](https://github.com/hazelcast/hazelcast-jet-contrib/blob/master/xa-test/src/main/java/com/hazelcast/jet/contrib/xatests/JdbcXaTest.java)
- [JMS Test](https://github.com/hazelcast/hazelcast-jet-contrib/blob/master/xa-test/src/main/java/com/hazelcast/jet/contrib/xatests/JmsXaTest.java)

### JDBC

Let's test compatibility of XA support for `PostgreSQL`.

1. Add the database connector dependency to `build.gradle`:

```groovy
  compile group: 'org.postgresql', name: 'postgresql', version: '42.2.9'
```

2. Edit `JdbcXaTest.java`

Create a `PGXADataSource` as the XA connection factory and configure it
with the URL of the database, username, password and database name:

```java
...
private static XADataSource getXADataSource() {
    PGXADataSource factory = new PGXADataSource();
    factory.setUrl("jdbc:postgresql://localhost:32773/test-database");
    factory.setUser("the-user");
    factory.setPassword("the-pass");
    factory.setDatabaseName("test-database");
    return factory;
}
...
```

3. Run the class, you should see `"Success!"` in the output.

### JMS

Testing compatibility of XA support for JMS brokers is similar to JDBC.

1. Add the dependency to `build.gradle`:

```groovy
  compile group: 'org.apache.activemq', name:'activemq-all', version:'5.15.11'
```

2. Edit `JmsXaTest.java`

Create an `ActiveMQXAConnectionFactory` with the broker URL as the XA
connection factory.

```java
...
private static XAConnectionFactory getXAConnectionFactory() {
    ActiveMQXAConnectionFactory factory = new ActiveMQXAConnectionFactory("broker:(tcp://localhost:61616)");
    return factory;
}
...
```

3. Run the class, you should see `"Success!"` in the output.
