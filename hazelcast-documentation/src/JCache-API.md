
## Introduction to the JCache API

This section explains the JCache API by providing simple examples and use cases. While walking through the examples, we will have
a look at a couple of the standard API classes and see how these classes are used.

### JCache API Walk-through

The code in this subsection creates a small account application by providing a caching layer over an imagined database abstraction. The
database layer will be simulated using single demo data in a simple DAO interface. To show the difference between the "database"
access and retrieving values from the cache, a small waiting time is used in the DAO implementation to simulate network and
database latency.

#### Basic User Class

Before we implement the JCache caching layer, let's have a quick look at some basic
classes we need for this example.

The `User` class is the representation of a user table in the database. To keep it simple, it has just two properties:
`userId` and `username`.

```java
public class User {
  private int userId;
  private String username;

  // Getters and setters
}
```
#### DAO Interface Example

The DAO interface is also kept easy in this example. It provides a simple method to retrieve (find) a user by its `userId`.

```java
public interface UserDAO {
  User findUserById( int userId );
  boolean storeUser( int userId, User user );
  boolean removeUser( int userId );
  Collection<Integer> allUserIds();
}
```
#### Configuration Example

To show most of the standard features, the configuration example is a little more complex.

```java
// Create javax.cache.configuration.CompleteConfiguration subclass
CompleteConfiguration<Integer, User> config =
    new MutableConfiguration<Integer, User>()
        // Configure the cache to be typesafe
        .setTypes( Integer.class, User.class )
        // Configure to expire entries 30 secs after creation in the cache
        .setExpiryPolicyFactory( FactoryBuilder.factoryOf(
            new AccessedExpiryPolicy( new Duration( TimeUnit.SECONDS, 30 ) )
        ) )
        // Configure read-through of the underlying store
        .setReadThrough( true )
        // Configure write-through to the underlying store
        .setWriteThrough( true )
        // Configure the javax.cache.integration.CacheLoader
        .setCacheLoaderFactory( FactoryBuilder.factoryOf(
            new UserCacheLoader( userDao )
        ) )
        // Configure the javax.cache.integration.CacheWriter
        .setCacheWriterFactory( FactoryBuilder.factoryOf(
            new UserCacheWriter( userDao )
        ) )
        // Configure the javax.cache.event.CacheEntryListener with no
        // javax.cache.event.CacheEntryEventFilter, to include old value
        // and to be executed synchronously
        .addCacheEntryListenerConfiguration(
            new MutableCacheEntryListenerConfiguration<Integer, User>(
                new UserCacheEntryListenerFactory(),
                null, true, true
            )
        );
```

Let's go through this configuration line by line.

#### Setting the Cache Type and Expire Policy

First, we set the expected types for the cache, which is already known from the previous example. On the next line, an
`javax.cache.expiry.ExpirePolicy` is configured. Almost all integration `ExpirePolicy` implementations are configured using
`javax.cache.configuration.Factory` instances. `Factory` and `FactoryBuilder` are explained later in this chapter.

#### Configuring Read-Through and Write-Through

The next two lines configure the thread that will be read-through and write-through to the underlying backend resource that is configured
over the next few lines. The JCache API offers `javax.cache.integration.CacheLoader` and `javax.cache.integration.CacheWriter` to
implement adapter classes to any kind of backend resource, e.g. JPA, JDBC, or any other backend technology implementable in Java.
The interfaces provides the typical CRUD operations like `create`, `get`, `update`, `delete` and some bulk operation versions of those
common operations. We will look into the implementation of those implementations later.

#### Configuring Entry Listeners

The last configuration setting defines entry listeners based on sub-interfaces of `javax.cache.event.CacheEntryListener`. This
config does not use a `javax.cache.event.CacheEntryEventFilter` since the listener is meant to be fired on every change that
happens on the cache. Again we will look in the implementation of the listener in later in this chapter.

#### Full Example Code

A full running example that is presented in this
subsection is available in the
[code samples repository](https://github.com/hazelcast/hazelcast-code-samples/tree/master/jcache/src/main/java/com/hazelcast/examples/application).
The application is built to be a command line app. It offers a small shell to accept different commands. After startup, you can
enter `help` to see all available commands and their descriptions.

