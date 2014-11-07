
# Hazelcast JCache

This chapter describes the basics of JCache: the standardized Java caching layer API. The JCache
caching API is specified by the Java Community Process (JCP) as Java Specification Request (JSR) 107.

Caching keeps data in memory that either are slow to calculate/process or originate from another underlying backend system
whereas caching is used to prevent additional request round trips for frequently used data. In both cases, caching could be used to
gain performance or decrease application latencies.

## JCache Overview

Starting with Hazelcast release 3.3.1, a specification compliant JCache implementation is offered. To show our commitment to this
important specification the Java world was waiting for over a decade, we do not just provide a simple wrapper around our existing
APIs but implemented a caching structure from ground up to optimize the behavior to the needs of JCache. As mentioned before,
the Hazelcast JCache implementation is 100% TCK (Technology Compatibility Kit) compliant and therefore passes all specification
requirements.

In addition to the given specification, we added some features like asynchronous versions of almost all
operations to give the user extra power.  

This chapter gives a basic understanding of how to configure your application and how to setup Hazelcast to be your JCache
provider. It also shows examples of basic JCache usage as well as the additionally offered features that are not part of JSR-107.
To gain a full understanding of the JCache functionality and provided guarantees of different operations, read
the specification document (which is also the main documentation for functionality) at the specification page of JSR-107:

[https://www.jcp.org/en/jsr/detail?id=107](https://www.jcp.org/en/jsr/detail?id=107)

## Setup and Configuration

This sub-chapter shows what is necessary to provide the JCache API and the Hazelcast JCache implementation for your application. In
addition, it demonstrates the different configuration options as well as a description of the configuration properties.

### Application Setup

To provide your application with this JCache functionality, your application needs the JCache API inside its classpath. This API is the bridge between the specified JCache standard and the implementation provided by Hazelcast.

The way to integrate the JCache API JAR into the application classpath depends on the build system used. For Maven, Gradle, SBT,
Ivy and many other build systems, all using Maven based dependency repositories, perform the integration by adding
the Maven coordinates to the build descriptor.

As already mentioned, next to the default Hazelcast coordinates that might be already part of the application, you have to add JCache
coordinates.

For Maven users, the coordinates look like the following code:

```xml
<dependency>
  <groupId>javax.cache</groupId>
  <artifactId>cache-api</artifactId>
  <version>1.0.0</version>
</dependency>
```
With other build systems, you might need to describe the coordinates in a different way.

#### Activating Hazelcast as JCache Provider

To activate Hazelcast as the JCache provider implementation, add either `hazelcast-all.jar` or
`hazelcast.jar` to the classpath (if not already available) by either one of the following Maven snippets.

If you use `hazelcast-all.jar`:

```xml
<dependency>
  <groupId>com.hazelcast</groupId>
  <artifactId>hazelcast-all</artifactId>
  <version>3.4</version>
</dependency>
```

If you use `hazelcast.jar`:

```xml
<dependency>
  <groupId>com.hazelcast</groupId>
  <artifactId>hazelcast</artifactId>
  <version>3.4</version>
</dependency>
```
The users of other build systems have to adjust the way of
defining the dependency to their needs.

#### Connecting Clients to Remote Server

When the users want to use Hazelcast clients to connect to a remote cluster, the `hazelcast-client.jar` dependency is also required
on the client side applications. This JAR is already included in `hazelcast-all.jar`. Or, you can add it to the classpath using the following
Maven snippet:

```xml
<dependency>
  <groupId>com.hazelcast</groupId>
  <artifactId>hazelcast</artifactId>
  <version>3.4</version>
</dependency>
```

For other build systems, e.g. ANT, the users have to download these dependencies from either the JSR-107 specification and
Hazelcast community website ([http://www.hazelcast.org](http://www.hazelcast.org)) or from the Maven repository search page
([http://search.maven.org](http://search.maven.org)).

### Quick Example

Before moving on to configuration, let's have a look at a basic introductory example. The following code shows how to use the Hazelcast JCache integration
inside an application in an easy but typesafe way.

```java
// Retrieve the CachingProvider which is automatically backed by
// the chosen Hazelcast server or client provider
CachingProvider cachingProvider = Caching.getCachingProvider();

// Create a CacheManager
CacheManager cacheManager = cachingProvider.getCacheManager();

// Create a simple but typesafe configuration for the cache
CompleteConfiguration<String, String> config =
    new MutableConfiguration<String, String>()
        .setTypes( String.class, String.class );

// Create and get the cache
Cache<String, String> cache = cacheManager.createCache( "example", config );
// Alternatively to request an already existing cache
// Cache<String, String> cache = cacheManager
//     .getCache( name, String.class, String.class );

// Put a value into the cache
cache.put( "world", "Hello World" );

// Retrieve the value again from the cache
String value = cache.get( "world" );

// Print the value 'Hello World'
System.out.println( value );
```

Although the example is simple, let's go through the code lines one by one.

#### Getting the Hazelcast JCache Implementation

First of all, we retrieve the `javax.cache.spi.CachingProvider` using the static method from
`javax.cache.Caching::getCachingManager` which automatically picks up Hazelcast as the underlying JCache implementation, if
available in the classpath. This way the Hazelcast implementation of a `CachingProvider` will automatically start a new Hazelcast
node or client (depending on the chosen provider type) and pick up the configuration from either the command line parameter
or from the classpath. We will show how to use an existing `HazelcastInstance` later in this chapter, for now we keep it simple.

#### Setting up the JCache Entry Point

In the next line, we ask the `CachingProvider` to return a `javax.cache.CacheManager`. This is the general application's entry
point into JCache. The `CachingProvider` creates and manages named caches.

#### Configuring the Cache Before Creating It

The next few lines create a simple `javax.cache.configuration.MutableConfiguration` to configure the cache before actually
creating it. In this case, we only configure the key and value types to make the cache typesafe which is highly recommended and
checked on retrieval of the cache.

#### Creating the Cache

To create the cache, we call `javax.cache.CacheManager::createCache` with a name for the cache and the previously created
configuration; the call returns the created cache. If you need to retrieve a previously created cache, you can use the corresponding method overload `javax.cache.CacheManager::getCache`. If the cache was created using type parameters, you must retrieve the cache afterward using the type checking version of `getCache`.

#### get, put, and getAndPut

The following lines are simple `put` and `get` calls from the `java.util.Map` interface. The
`javax.cache.Cache::put` has a `void` return type and does not return the previously assigned value of the key. To imitate the
`java.util.Map::put` method, the JCache cache has a method called `getAndPut`.

### JCache Configuration

Hazelcast JCache provides two different ways of cache configuration:

- programmatically: the typical Hazelcast way, using the Config API seen above),
- and declaratively: using `hazelcast.xml` or `hazelcast-client.xml`.

#### JCache Declarative Configuration

You can declare your JCache cache configuration using the `hazelcast.xml` or `hazelcast-client.xml` configuration files. Using this declarative configuration makes the creation of the `javax.cache.Cache` fully transparent and automatically ensures internal thread safety. You do not need a call to `javax.cache.Cache::createCache` in this case: you can retrieve the cache using
`javax.cache.Cache::getCache` overloads and by passing in the name defined in the configuration for the cache.

To retrieve the cache defined in the declaration files, you need only perform a simple call (example below) because the cache is created automatically by the implementation.

```java
CachingProvider cachingProvider = Caching.getCachingProvider();
CacheManager cacheManager = cachingProvider.getCacheManager();
Cache<Object, Object> cache = cacheManager
    .getCache( "default", Object.class, Object.class );
```

Note that this section only describes the JCache provided standard properties. For the Hazelcast specific properties, please see the
[ICache Configuration](#icache-configuration) section.

```xml
<cache name="default">
  <key-type class-name="java.lang.Object" />
  <value-type class-name="java.lang.Object" />
  <statistics-enabled>false</statistics-enabled>
  <management-enabled>false</management-enabled>

  <read-through>true</read-through>
  <write-through>true</write-through>
  <cache-loader-factory
     class-name="com.example.cache.MyCacheLoaderFactory" />
  <cache-writer-factory
     class-name="com.example.cache.MyCacheWriterFactory" />
  <expiry-policy-factory
     class-name="com.example.cache.MyExpirePolicyFactory" />

  <entry-listeners>
    <entry-listener old-value-required="false" synchronous="false">
      <entry-listener-factory
         class-name="com.example.cache.MyEntryListenerFactory" />
      <entry-event-filter-factory
         class-name="com.example.cache.MyEntryEventFilterFactory" />
    </entry-listener>
    ...
  </entry-listeners>
</cache>
```

- `key-type#class-name`: The fully qualified class name of the cache key type, defaults to `java.lang.Object`.
- `value-type#class-name`: The fully qualified class name of the cache value type, defaults to `java.lang.Object`.
- `statistics-enabled`: If set to true, statistics like cache hits and misses are collected. Its default value is false.
- `management-enabled`: If set to true, JMX beans are enabled and collected statistics are provided - It doesn't automatically enables statistics collection, defaults to false.
- `read-through`: If set to true, enables read-through behavior of the cache to an underlying configured `javax.cache.integration.CacheLoader` which is also known as lazy-loading, defaults to false.
- `write-through`: If set to true, enables write-through behavior of the cache to an underlying configured `javax.cache.integration.CacheWriter` which passes any changed value to the external backend resource, defaults to false.
- `cache-loader-factory#class-name`: The fully qualified class name of the `javax.cache.configuration.Factory` implementation providing a `javax.cache.integration.CacheLoader` instance to the cache.
- `cache-writer-factory#class-name`: The fully qualified class name of the `javax.cache.configuration.Factory` implementation providing a `javax.cache.integration.CacheWriter` instance to the cache.
- `expiry-policy-factory#-class-name`: The fully qualified class name of the `javax.cache.configuration.Factory` implementation providing a `javax.cache.expiry.ExpiryPolicy` instance to the cache.
- `entry-listener`: A set of attributes and elements, explained below, to describe a `javax.cache.event.CacheEntryListener`.
  - `entry-listener#old-value-required`: If set to true, previously assigned values for the affected keys will be sent to the `javax.cache.event.CacheEntryListener` implementation. Setting this attribute to true creates additional traffic, defaults to false.
  - `entry-listener#synchronous`: If set to true, the `javax.cache.event.CacheEntryListener` implementation will be called in a synchronous manner, defaults to false.
  - `entry-listener/entry-listener-factory#class-name`: The fully qualified class name of the `javax.cache.configuration.Factory` implementation providing a `javax.cache.event.CacheEntryListener` instance.
  - `entry-listener/entry-event-filter-factory#class-name`: The fully qualified class name of the `javax.cache.configuration.Factory` implementation providing a `javax.cache.event.CacheEntryEventFilter` instance.

<br></br>
![image](images/NoteSmall.jpg) ***NOTE:*** *The JMX MBeans provided by Hazelcast JCache show statistics of the local node only.
To show the cluster-wide statistics, the user should collect statistic information from all nodes and accumulate them to
the overall statistics.*
<br></br>

#### JCache Programmatic Configuration

To configure the JCache programmatically:

- either instantiate `javax.cache.configuration.MutableConfiguration` if you will use
only the JCache standard configuration,
- or instantiate `com.hazelcast.config.CacheConfig` for a deeper Hazelcast integration.

`com.hazelcast.config.CacheConfig` offers additional options that are specific to Hazelcast like asynchronous and synchronous backup counts.
Both classes share the same supertype interface `javax.cache.configuration.CompleteConfiguration` which is part of the JCache
standard.

<br></br>
![image](images/NoteSmall.jpg) ***NOTE:*** *To stay vendor independent, try to keep your code as near as possible to the standard JCache API. We recommend you to use declarative configuration
and use the `javax.cache.configuration.Configuration` or `javax.cache.configuration.CompleteConfiguration` interfaces in
your code only when you need to pass the configuration instance throughout your code.*
<br></br>

If you don't need to configure Hazelcast specific properties, it is recommended that you instantiate
`javax.cache.configuration.MutableConfiguration` and that you use the setters to configure Hazelcast as shown in the example in
[Quick Example](#quick-example). Since the configurable properties are the same as the ones explained in
[JCache Declarative Configuration](#jcache-declarative-configuration), they are not mentioned here. For Hazelcast specific
properties, please read the [ICache Configuration](#icache-configuration) section.

## JCache Providers

Use JCache providers to create caches for a specification compliant implementation. Those providers abstract the platform
specific behavior and bindings, and provide the different JCache required features.

Hazelcast has two types of providers. Depending on your application setup and the cluster topology,
you can use the Client Provider (used from Hazelcast clients) or the Server Provider (used by cluster nodes).

### Provider Configuration

You configure the JCache `javax.cache.spi.CachingProvider` by either specifying the provider at the command line or by declaring the provider inside the Hazelcast configuration XML file. For more information on setting properties in this XML
configuration file, please see [JCache Declarative Configuration](#jcache-declarative-configuration).

Hazelcast implements a delegating `CachingProvider` that can automatically be configured for either client or server mode and that
delegates to the real underlying implementation based on the user's choice. It is recommended that you use this `CachingProvider`
implementation.

The delegating `CachingProvider`s fully qualified class name is:

```plain
com.hazelcast.cache.HazelcastCachingProvider
```

To configure the delegating provider at the command line, add the following parameter to the Java startup call, depending on the chosen provider:

```plain
-Dhazelcast.jcache.provider.type=[client|server]
```

By default, the delegating `CachingProvider` is automatically picked up by the JCache SPI and provided as shown above. In cases where multiple `javax.cache.spi.CachingProvider` implementations reside on the classpath (like in some Application
Server scenarios), you can select a `CachingProvider` by explicitly calling `Caching::getCachingProvider`
overloads and providing them using the canonical class name of the provider to be used. The class names of server and client providers
provided by Hazelcast are mentioned in the following two subsections.

<br></br>
![image](images/NoteSmall.jpg) ***NOTE:*** *Hazelcast advises that you use the `Caching::getCachingProvider` overloads to select a
`CachingProvider` explicitly. This ensures that uploading to later environments or Application Server versions doesn't result in
unexpected behavior like choosing a wrong `CachingProvider`.*
<br></br>

For more information on cluster topologies and Hazelcast clients, please see  [Hazelcast Topology](#hazelcast-topology).

### JCache Client Provider

For cluster topologies where Hazelcast light clients are used to connect to a remote Hazelcast cluster, use the Client Provider to configure JCache.

The Client Provider provides the same features as the Server Provider. However, it does not hold data on its own but instead delegates requests and calls to the remotely connected cluster.

The Client Provider can connect to multiple clusters at the same time. This can be achieved by scoping the client side
`CacheManager` with different Hazelcast configuration files. For more information, please see
[Scopes and Namespaces](#scopes-and-namespaces).

For requesting this `CachingProvider` using `Caching#getCachingProvider( String )` or
`Caching#getCachingProvider( String, ClassLoader )`, use the following fully qualified class name:

```plain
com.hazelcast.client.cache.impl.HazelcastClientCachingProvider
```

### JCache Server Provider

If a Hazelcast node is embedded into an application directly and the Hazelcast client is not used, the Server Provider is
required. In this case, the node itself becomes a part of the distributed cache and requests and operations are distributed
directly across the cluster by its given key.

The Server Provider provides the same features as the Client provider, but it keeps data in the local Hazelcast node and also distributes
non-owned keys to other direct cluster members.

Like the Client Provider, the Server Provider is able to connect to multiple clusters at the same time. This can be achieved by scoping the client side `CacheManager` with different Hazelcast configuration files. For more
information please see [Scopes and Namespaces](#scopes-and-namespaces).

To request this `CachingProvider` using `Caching#getCachingProvider( String )` or
`Caching#getCachingProvider( String, ClassLoader )`, use the following fully qualified class name:

```plain
com.hazelcast.cache.impl.HazelcastServerCachingProvider
```

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

### Roundup of Basics

In the section [Quick Example](#quick-example), we have already seen a couple of the base classes and explained how those work. Following are quick descriptions of them.

**`javax.cache.Caching`**:

The access point into the JCache API. It retrieves the general `CachingProvider` backed by any compliant JCache
implementation, such as Hazelcast JCache.

**`javax.cache.spi.CachingProvider`**:

The SPI that is implemented to bridge between the JCache API and the implementation itself. Hazelcast nodes and clients use different
providers chosen as seen in the subsection for [Provider Configuration](#provider-configuration) which enable the JCache API to
interact with Hazelcast clusters.

When a `javax.cache.spi.CachingProvider::getCacheManager` overload is used that takes a `java.lang.ClassLoader` argument, this
classloader will be part of the scope of the created `java.cache.Cache` and it is not possible to retrieve it on other nodes.
We advise not to use those overloads, those are not meant to be used in distributed environments!

**`javax.cache.CacheManager`**:

The `CacheManager` provides the capability to create new and manage existing JCache caches.

<br></br>
![image](images/NoteSmall.jpg) ***NOTE:*** *A `javax.cache.Cache` instance created with key and value types in the configuration
provides a type checking of those types at retrieval of the cache. For that reason, all non-types retrieval methods like
`getCache` throw an exception because types cannot be checked.*
<br></br>

**`javax.cache.configuration.Configuration`**, **`javax.cache.configuration.MutableConfiguration`**:

These two classes are used to configure a cache prior to retrieving it from a `CacheManager`. The `Configuration` interface,
therefore, acts as a common super type for all compatible configuration classes such as `MutableConfiguration`.

Hazelcast itself offers a special implementation (`com.hazelcast.config.CacheConfig`) of the `Configuration` interface which
offers more options on the specific Hazelcast properties that can be set to configure features like synchronous and asynchronous
backups counts or selecting the underlying [In Memory Format](#in-memory-format) of the cache. For more information on this
configuration class, please see the reference in [JCache Programmatic Configuration](#jcache-programmatic-configuration).

**`javax.cache.Cache`**:

This interface represents the cache instance itself. It is comparable to `java.util.Map` but offers special operations dedicated
to the caching use case. Therefore, for example `javax.cache.Cache::put`, unlike `java.util.Map::put`, does not return the old
value previously assigned to the given key.

<br></br>
![image](images/NoteSmall.jpg) ***NOTE:*** *Bulk operations on the `Cache` interface guarantee atomicity per entry but not over
all given keys in the same bulk operations since no transactional behavior is applied over the whole batch process.*
<br></br>

### Factory and FactoryBuilder

The `javax.cache.configuration.Factory` implementations are used to configure features like
`CacheEntryListener`, `ExpirePolicy` and `CacheLoader`s or `CacheWriter`s. These factory implementations are required to distribute the
different features to nodes in a cluster environment like Hazelcast. Therefore, these factory implementations have to be serializable.

`Factory` implementations are easy to do: they follow the default Provider- or Factory-Pattern. The sample class
`UserCacheEntryListenerFactory` shown below implements a custom JCache `Factory`.

```java
public class UserCacheEntryListenerFactory
    implements Factory<CacheEntryListener<Integer, User>> {

  @Override
  public CacheEntryListener<Integer, User> create() {
    // Just create a new listener instance
    return new UserCacheEntryListener();
  }
}
```

To simplify the process for the users, JCache API offers a set of helper methods collected in
`javax.cache.configuration.FactoryBuilder`. In the above configuration example, `FactoryBuilder::factoryOf` is used to create a
singleton factory for the given instance.

### CacheLoader

`javax.cache.integration.CacheLoader` loads cache entries from any external backend resource. If the cache is
configured to be `read-through`, then `CacheLoader::load` is called transparently from the cache when the key or the value is not
yet found in the cache. If no value is found for a given key, it returns null.

If the cache is not configured to be `read-through`, nothing is loaded automatically. However, the user code must call `javax.cache.Cache::loadAll` to load data for the given set of keys into the cache.

For the bulk load operation (`loadAll()`), some keys may not be found in the returned result set. In this case, a `javax.cache.integration.CompletionListener` parameter can be used as an asynchronous callback after all the key-value pairs are loaded because loading many key-value pairs can take lots of time.

Let's look at the `UserCacheLoader` implementation.

```java
public class UserCacheLoader
    implements CacheLoader<Integer, User>, Serializable {

  private final UserDao userDao;

  public UserCacheLoader( UserDao userDao ) {
    // Store the dao instance created externally
    this.userDao = userDao;
  }

  @Override
  public User load( Integer key ) throws CacheLoaderException {
    // Just call through into the dao
    return userDao.findUserById( key );
  }

  @Override
  public Map<Integer, User> loadAll( Iterable<? extends Integer> keys )
      throws CacheLoaderException {

    // Create the resulting map  
    Map<Integer, User> loaded = new HashMap<Integer, User>();
    // For every key in the given set of keys
    for ( Integer key : keys ) {
      // Try to retrieve the user
      User user = userDao.findUserById( key );
      // If user is not found do not add the key to the result set
      if ( user != null ) {
        loaded.put( key, user );
      }
    }
    return loaded;
  }
}
```

The implementation is quite straight forward. An important note is that
any kind of exception has to be wrapped into `javax.cache.integration.CacheLoaderException`.

### CacheWriter

A `javax.cache.integration.CacheWriter` is used to update an external backend resource. If the cache is configured to be
`write-through` this process is executed transparently to the users code otherwise at the current state there is no way to trigger
writing changed entries to the external resource to a user defined point in time.

If bulk operations throw an exception, `java.util.Collection` has to be cleaned of all successfully written keys so
the cache implementation can determine what keys are written and can be applied to the cache state.

```java
public class UserCacheWriter
    implements CacheWriter<Integer, User>, Serializable {

  private final UserDao userDao;

  public UserCacheWriter( UserDao userDao ) {
    // Store the dao instance created externally
    this.userDao = userDao;
  }

  @Override
  public void write( Cache.Entry<? extends Integer, ? extends User> entry )
      throws CacheWriterException {

    // Store the user using the dao
    userDao.storeUser( entry.getKey(), entry.getValue() );
  }

  @Override
  public void writeAll( Collection<Cache.Entry<...>> entries )
      throws CacheWriterException {

    // Retrieve the iterator to clean up the collection from
    // written keys in case of an exception
    Iterator<Cache.Entry<...>> iterator = entries.iterator();
    while ( iterator.hasNext() ) {
      // Write entry using dao
      write( iterator.next() );
      // Remove from collection of keys
      iterator.remove();
    }
  }

  @Override
  public void delete( Object key ) throws CacheWriterException {
    // Test for key type
    if ( !( key instanceof Integer ) ) {
      throw new CacheWriterException( "Illegal key type" );
    }
    // Remove user using dao
    userDao.removeUser( ( Integer ) key );
  }

  @Override
  public void deleteAll( Collection<?> keys ) throws CacheWriterException {
    // Retrieve the iterator to clean up the collection from
    // written keys in case of an exception
    Iterator<?> iterator = keys.iterator();
    while ( iterator.hasNext() ) {
      // Write entry using dao
      delete( iterator.next() );
      // Remove from collection of keys
      iterator.remove();
    }
  }
}
```

Again the implementation is pretty straight forward and also as above all exceptions thrown by the external resource, like
`java.sql.SQLException` has to be wrapped into a `javax.cache.integration.CacheWriterException`. Note this is a different
exception from the one thrown by `CacheLoader`.

### JCache EntryProcessor

With `javax.cache.processor.EntryProcessor`, you can apply an atomic function to a cache entry. In a distributed
environment like Hazelcast, you can move the mutating function to the node that owns the key. If the value
object is big, it might prevent traffic by sending the object to the mutator and sending it back to the owner to update it.

By default, Hazelcast JCache sends the complete changed value to the backup partition. Again, this can cause a lot of traffic if
the object is big. Another option to prevent this is part of the Hazelcast ICache extension. Further information is available at
[BackupAwareEntryProcessor](#backupawareentryprocessor).

An arbitrary number of arguments can be passed to the `Cache::invoke` and `Cache::invokeAll` methods. All of those arguments need
to be fully serializable because in a distributed environment like Hazelcast, it is very likely that these arguments have to be passed around the cluster.

```java
public class UserUpdateEntryProcessor
    implements EntryProcessor<Integer, User, User> {

  @Override
  public User process( MutableEntry<Integer, User> entry, Object... arguments )
      throws EntryProcessorException {

    // Test arguments length
    if ( arguments.length < 1 ) {
      throw new EntryProcessorException( "One argument needed: username" );
    }

    // Get first argument and test for String type
    Object argument = arguments[0];
    if ( !( argument instanceof String ) ) {
      throw new EntryProcessorException(
          "First argument has wrong type, required java.lang.String" );
    }

    // Retrieve the value from the MutableEntry
    User user = entry.getValue();

    // Retrieve the new username from the first argument
    String newUsername = ( String ) arguments[0];

    // Set the new username
    user.setUsername( newUsername );

    // Set the changed user to mark the entry as dirty
    entry.setValue( user );

    // Return the changed user to return it to the caller
    return user;
  }
}
```

<br></br>
![image](images/NoteSmall.jpg) ***NOTE:*** *By executing the bulk `Cache::invokeAll` operation, atomicity is only guaranteed for a
single cache entry. No transactional rules are applied to the bulk operation.*

![image](images/NoteSmall.jpg) ***NOTE:*** *JCache `EntryProcessor` implementations are not allowed to call
`javax.cache.Cache` methods; this prevents operations from deadlocking between different calls.*
<br></br>

In addition, when using a `Cache::invokeAll` method, a `java.util.Map` is returned that maps the key to its
`javax.cache.processor.EntryProcessorResult`, and which itself wraps the actual result or a thrown
`javax.cache.processor.EntryProcessorException`.

### CacheEntryListener

The `javax.cache.event.CacheEntryListener` implementation is straight forward. `CacheEntryListener` is a super-interface which is used as a marker for listener classes in JCache. The specification brings a set of sub-interfaces.

- `CacheEntryCreatedListener`: Fires after a cache entry is added (even on read-through by a `CacheLoader`) to the cache.
- `CacheEntryUpdatedListener`: Fires after an already existing cache entry was updates.
- `CacheEntryRemovedListener`: Fires after a cache entry was removed (not expired) from the cache.
- `CacheEntryExpiredListener`: Fires after a cache entry has been expired. Expiry does not have to be parallel process, it is only required to be executed on the keys that are requested by `Cache::get` and some other operations. For a full table of expiry please see the [https://www.jcp.org/en/jsr/detail?id=107](https://www.jcp.org/en/jsr/detail?id=107) point 6.  

To configure `CacheEntryListener`, add a `javax.cache.configuration.CacheEntryListenerConfiguration` instance to
the JCache configuration class, as seen in the above example configuration. In addition listeners can be configured to be
executed synchronously (blocking the calling thread) or asynchronously (fully running in parallel).

In this example application, the listener is implemented to print event information on the console. That visualizes what is going on in the cache.

```java
public class UserCacheEntryListener
    implements CacheEntryCreatedListener<Integer, User>,
        CacheEntryUpdatedListener<Integer, User>,
        CacheEntryRemovedListener<Integer, User>,
        CacheEntryExpiredListener<Integer, User> {

  @Override
  public void onCreated( Iterable<CacheEntryEvent<...>> cacheEntryEvents )
      throws CacheEntryListenerException {

    printEvents( cacheEntryEvents );
  }

  @Override
  public void onUpdated( Iterable<CacheEntryEvent<...>> cacheEntryEvents )
      throws CacheEntryListenerException {

    printEvents( cacheEntryEvents );
  }

  @Override
  public void onRemoved( Iterable<CacheEntryEvent<...>> cacheEntryEvents )
      throws CacheEntryListenerException {

    printEvents( cacheEntryEvents );
  }

  @Override
  public void onExpired( Iterable<CacheEntryEvent<...>> cacheEntryEvents )
      throws CacheEntryListenerException {

    printEvents( cacheEntryEvents );
  }

  private void printEvents( Iterable<CacheEntryEvent<...>> cacheEntryEvents ) {
    Iterator<CacheEntryEvent<...>> iterator = cacheEntryEvents.iterator();
    while ( iterator.hasNext() ) {
      CacheEntryEvent<...> event = iterator.next();
      System.out.println( event.getEventType() );
    }
  }
}
```

### ExpirePolicy

In JCache, `javax.cache.expiry.ExpirePolicy` implementations are used to automatically expire cache entries based on different rules.

Expiry timeouts are defined using `javax.cache.expiry.Duration`, which is a pair of `java.util.concurrent.TimeUnit`, which
describes a time unit and a long, defining the timeout value. The minimum allowed `TimeUnit` is `TimeUnit.MILLISECONDS`.
The long value `durationAmount` must be equal or greater than zero. A value of zero (or `Duration.ZERO`) indicates that the
cache entry expires immediately.

By default, JCache delivers a set of predefined expiry strategies in the standard API.

- `AccessedExpiryPolicy`: Expires after a given set of time measured from creation of the cache entry, the expiry timeout is updated on accessing the key.
- `CreatedExpiryPolicy`: Expires after a given set of time measured from creation of the cache entry, the expiry timeout is never updated.
- `EternalExpiryPolicy`: Never expires, this is the default behavior, similar to `ExpiryPolicy` to be set to null.
- `ModifiedExpiryPolicy`: Expires after a given set of time measured from creation of the cache entry, the expiry timeout is updated on updating the key.
- `TouchedExpiryPolicy`: Expires after a given set of time measured from creation of the cache entry, the expiry timeout is updated on accessing or updating the key.

Because `EternalExpirePolicy` does not expire cache entries, it is still possible to evict values from memory if an underlying
`CacheLoader` is defined.

## Hazelcast JCache Extension - ICache

Hazelcast provides extension methods to Cache API through the interface `com.hazelcast.cache.ICache`.

It has two sets of extensions:

* Asynchronous version of all cache operations.
* Cache operations with custom `ExpiryPolicy` parameter to apply on that specific operation.

### Scopes and Namespaces

As mentioned before, a `CacheManager` can be scoped in the case of client to connect to multiple clusters, or in the case of an embedded node, a `CacheManager` can be scoped to join different clusters at the same time. This process is called scoping. To apply it, request
a `CacheManager` by passing a `java.net.URI` instance to `CachingProvider::getCacheManager`. The `java.net.URI` instance must point to either a Hazelcast configuration or to the name of a named
`com.hazelcast.core.HazelcastInstance` instance.

<br></br>
![image](images/NoteSmall.jpg) ***NOTE:*** *Multiple requests for the same `java.net.URI` result in returning a `CacheManager`
instance that shares the same `HazelcastInstance` as the `CacheManager` returned by the previous call.*
<br></br>

#### Configuration Scope

To connect or join different clusters, apply a configuration scope to the `CacheManager`. If the same `URI` is
used to request a `CacheManager` that was created previously, those `CacheManager`s share the same underlying `HazelcastInstance`.

To apply a configuration scope, pass in the path of the configuration file using the location property
`HazelcastCachingProvider#HAZELCAST_CONFIG_LOCATION` (which resolves to `hazelcast.config.location`) as a mapping inside a
`java.util.Properties` instance to the `CachingProvider#getCacheManager(uri, classLoader, properties)` call.

Here is an example of using Configuration Scope.

```java
CachingProvider cachingProvider = Caching.getCachingProvider();

// Create Properties instance pointing to a Hazelcast config file
Properties properties = new Properties();
properties.setProperty( HazelcastCachingProvider.HAZELCAST_CONFIG_LOCATION,
    "classpath://my-configs/scoped-hazelcast.xml" );

URI cacheManagerName = new URI( "my-cache-manager" );
CacheManager cacheManager = cachingProvider
    .getCacheManager( cacheManagerName, null, properties );
```

Here is an example using `HazelcastCachingProvider::propertiesByLocation` helper method.

```java
CachingProvider cachingProvider = Caching.getCachingProvider();

// Create Properties instance pointing to a Hazelcast config file
String configFile = "classpath://my-configs/scoped-hazelcast.xml";
Properties properties = HazelcastCachingProvider
    .propertiesByLocation( configFile );

URI cacheManagerName = new URI( "my-cache-manager" );
CacheManager cacheManager = cachingProvider
    .getCacheManager( cacheManagerName, null, properties );
```

The retrieved `CacheManager` is scoped to use the `HazelcastInstance` that was just created and was configured using the given XML
configuration file.

Available protocols for config file URL include `classpath://` to point to a classpath location, `file://` to point to a filesystem
location, `http://` an `https://` for remote web locations. In addition, everything that does not specify a protocol is recognized
as a placeholder that can be configured using a system property.

```java
String configFile = "my-placeholder";
Properties properties = HazelcastCachingProvider
    .propertiesByLocation( configFile );
```

Can be set on the command line by:

```plain
-Dmy-placeholder=classpath://my-configs/scoped-hazelcast.xml
```

<br></br>
![image](images/NoteSmall.jpg) ***NOTE:*** *No check is performed to prevent creating multiple `CacheManager`s with the same cluster
configuration on different configuration files. If the same cluster is referred from different configuration files, multiple
cluster members or clients are created.*

![image](images/NoteSmall.jpg) ***NOTE:*** *The configuration file location will not be a part of the resulting identity of the
`CacheManager`. An attempt to create a `CacheManager` with a different set of properties but an already used name will result in
undefined behavior.*
<br></br>

#### Named Instance Scope

A `CacheManager` can be bound to an existing and named `HazelcastInstance` instance. This requires that the instance was created
using a `com.hazelcast.config.Config` and requires that an `instanceName` be set. Multiple `CacheManager`s created using an equal
`java.net.URI` will share the same `HazelcastInstance`.

A named scope is applied nearly the same way as the configuration scope: pass in the instance name using the
`HazelcastCachingProvider#HAZELCAST_INSTANCE_NAME` (which resolves to `hazelcast.instance.name`) property as a mapping inside a
`java.util.Properties` instance to the `CachingProvider#getCacheManager(uri, classLoader, properties)` call.

Here is an example of Named Instance Scope.

```java
Config config = new Config();
config.setInstanceName( "my-named-hazelcast-instance" );
// Create a named HazelcastInstance
Hazelcast.newHazelcastInstance( config );

CachingProvider cachingProvider = Caching.getCachingProvider();

// Create Properties instance pointing to a named HazelcastInstance
Properties properties = new Properties();
properties.setProperty( HazelcastCachingProvider.HAZELCAST_INSTANCE_NAME,
     "my-named-hazelcast-instance" );

URI cacheManagerName = new URI( "my-cache-manager" );
CacheManager cacheManager = cachingProvider
    .getCacheManager( cacheManagerName, null, properties );
```

Here is an example using `HazelcastCachingProvider::propertiesByInstanceName` method.

```java
Config config = new Config();
config.setInstanceName( "my-named-hazelcast-instance" );
// Create a named HazelcastInstance
Hazelcast.newHazelcastInstance( config );

CachingProvider cachingProvider = Caching.getCachingProvider();

// Create Properties instance pointing to a named HazelcastInstance
Properties properties = HazelcastCachingProvider
    .propertiesByInstanceName( "my-named-hazelcast-instance" );

URI cacheManagerName = new URI( "my-cache-manager" );
CacheManager cacheManager = cachingProvider
    .getCacheManager( cacheManager, null, properties );
```

<br></br>
![image](images/NoteSmall.jpg) ***NOTE:*** *The `instanceName` will not be a part of the resulting identity of the `CacheManager`.
An attempt to create a `CacheManager` with a different set of properties but an already used name will result in undefined behavior.*
<br></br>


#### Namespaces

The `java.net.URI`s that don't use the above mentioned Hazelcast specific schemes are recognized as namespacing. Those
`CacheManager`s share the same underlying default `HazelcastInstance` created (or set) by the `CachingProvider`, but they cache with the
same names but differently namespaces on `CacheManager` level, and therefore won't share the same data. This is useful where multiple
applications might share the same Hazelcast JCache implementation (e.g. on application or OSGi servers) but are developed by
independent teams. To prevent interfering on caches using the same name, every application can use its own namespace when
retrieving the `CacheManager`.

Here is an example of using namespacing.

```java
CachingProvider cachingProvider = Caching.getCachingProvider();

URI nsApp1 = new URI( "application-1" );
CacheManager cacheManagerApp1 = cachingProvider.getCacheManager( nsApp1, null );

URI nsApp2 = new URI( "application-2" );
CacheManager cacheManagerApp2 = cachingProvider.getCacheManager( nsApp2, null );
```

That way both applications share the same `HazelcastInstance` instance but not the same caches.

### Retrieving an ICache Instance

Besides [Scopes and Namespaces](#scopes-and-namespaces), which are implemented using the URI feature of the
specification, all other extended operations are required to retrieve the `com.hazelcast.cache.ICache` interface instance from
the JCache `javax.cache.Cache` instance. For Hazelcast, both interfaces are implemented on the same object instance. It
is recommended that you stay with the specification way to retrieve the `ICache` version, since `ICache` might be subject to change without notification.

To retrieve or unwrap the `ICache` instance, you can execute the following code snippet:

```java
CachingProvider cachingProvider = Caching.getCachingProvider();
CacheManager cacheManager = cachingProvider.getCacheManager();
Cache<Object, Object> cache = cacheManager.getCache( ... );

ICache<Object, Object> unwrappedCache = cache.unwrap( ICache.class );
```

After unwrapping the `Cache` instance into an `ICache` instance, you have access to all of the following operations, e.g.
[Async Operations](#async-operations) and [Additional Methods](#additional-methods).

### ICache Configuration

As mentioned in [JCache Declarative Configuration](#jcache-declarative-configuration), the Hazelcast ICache extension offers
additional configuration properties over the default JCache configuration. These additional properties include internal storage format, backup counts
and eviction policy.

The declarative configuration for ICache is a superset of the previously discussed JCache configuration:

```xml
<cache>
  <!-- ... default cache configuration goes here ... -->
  <backup-count>1</backup-count>
  <async-backup-count>1</async-backup-count>
  <in-memory-format>BINARY</in-memory-format>
  <eviction-policy>NONE</eviction-policy>
  <eviction-percentage>25</eviction-percentage>
  <eviction-threshold-percentage>25</eviction-threshold-percentage>
</cache>
```

- `backup-count`: The number of synchronous backups. Those backups are executed before the mutating cache operation is finished. The mutating operation is blocked. `backup-count` default value is 1.
- `async-backup-count`: The number of asynchronous backups. Those backups are executed asynchronously so the mutating operation is not blocked and it will be done immediately. `async-backup-count` default value is 0.  
- `in-memory-format`: Defines the internal storage format. For more information, please see [In Memory Format](#in-memory-format). Default is `BINARY`.
- `eviction-policy`: The eviction policy **(currently available on High-Density Memory Store only)** defines which entries are evicted (removed) from the cache when the cache is low in space. Its default value is `RANDOM`. The following eviction policies are available:
  - `LRU`: Abbreviation for Least Recently Used. When `eviction-policy` is set to `LRU`, the longest unused (not accessed) entries are removed from the cache.  
  - `LFU`: Abbreviation for Least Frequently Used. When `eviction-policy` is set to `LFU`, the entries that are used (accessed) least frequently are removed from the cache.
  - `RANDOM`: When `eviction-policy` is set to `RANDOM`, random entries are removed from the cache. No information about access frequency or last accessed time are taken into account.
  - `NONE`: When `eviction-policy` is set to `NONE`, no entries are removed from the cache at all.
- `eviction-percentage`: The eviction percentage property **(currently available on High-Density Memory Store only)** defines the amount of percentage of the cache that will be evicted when the threshold is reached. Can be set to any integer number between 0 and 100, defaults to 0.
- `eviction-threshold-percentage`: the eviction threshold property **(currently available on High-Density Memory Store only)** defines a threshold when reached to trigger the eviction process. Can be set to any integer number between 0 and 100, defaults to 0.

Since `javax.cache.configuration.MutableConfiguration` misses the above additional configuration properties, Hazelcast ICache extension
provides an extended configuration class called `com.hazelcast.config.CacheConfig`. This class is an implementation of `javax.cache.configuration.CompleteConfiguration` and all the properties shown above can be configured
using its corresponding setter methods.

### Async Operations

As another addition of Hazelcast ICache over the normal JCache specification, Hazelcast provides asynchronous versions of almost
all methods, returning a `com.hazelcast.core.ICompletableFuture`. By using these methods and the returned future objects, you can use JCache in a reactive way by registering zero or more callbacks on the future to prevent blocking the current thread.


Name of the asynchronous versions of the methods append the phrase `Async` to the method name. Sample code is shown below using the method `putAsync()`.

```java
ICache<Integer, String> unwrappedCache = cache.unwrap( ICache.class );
ICompletableFuture<String> future = unwrappedCache.putAsync( 1, "value" );
future.andThen( new ExecutionCallback<String>() {
  public void onResponse( String response ) {
    System.out.println( "Previous value: " + response );
  }

  public void onFailure( Throwable t ) {
    t.printStackTrace();
  }
} );
```

Following methods
are available in asynchronous versions:

 - `get(key)`:
  - `getAsync(key)`
  - `getAsync(key, expiryPolicy)`
 - `put(key, value)`:
  - `putAsync(key, value)`
  - `putAsync(key, value, expiryPolicy)`
 - `putIfAbsent(key, value)`:
  - `putIfAbsentAsync(key, value)`
  - `putIfAbsentAsync(key, value, expiryPolicy)`
 - `getAndPut(key, value)`:
  - `getAndPutAsync(key, value)`
  - `getAndPutAsync(key, value, expiryPolicy)`
 - `remove(key)`:
  - `removeAsync(key)`
 - `remove(key, value)`:
  - `removeAsync(key, value)`
 - `getAndRemove(key)`:
  - `getAndRemoveAsync(key)`
 - `replace(key, value)`:
  - `replaceAsync(key, value)`
  - `replaceAsync(key, value, expiryPolicy)`
 - `replace(key, oldValue, newValue)`:
  - `replaceAsync(key, oldValue, newValue)`
  - `replaceAsync(key, oldValue, newValue, expiryPolicy)`
 - `getAndReplace(key, value)`:
  - `getAndReplaceAsync(key, value)`
  - `getAndReplaceAsync(key, value, expiryPolicy)`

The methods with a given `javax.cache.expiry.ExpiryPolicy` are further discussed in the section
[Custom ExpiryPolicy](#custom-expirypolicy).

<br></br>
![image](images/NoteSmall.jpg) ***NOTE:*** *Asynchronous versions of the methods are not compatible with synchronous events.*
<br></br>

### Custom ExpiryPolicy

The JCache specification has an option to configure a single `ExpiryPolicy` per cache. Hazelcast ICache extension
offers the possibility to define a custom `ExpiryPolicy` per key by providing a set of method overloads with an `expirePolicy`
parameter, as in the list of asynchronous methods in the section [Async Methods](#async-methods). This means that custom expiry policies can passed to a cache operation.

Here is how an `ExpirePolicy` is set on JCache configuration:

```java
CompleteConfiguration<String, String> config =
    new MutableConfiguration<String, String>()
        setExpiryPolicyFactory(
            AccessedExpiryPolicy.factoryOf( Duration.ONE_MINUTE )
        );
```

To pass a custom `ExpirePolicy`, a set of overloads is provided and can be used as shown in the following code snippet:

```java
ICache<Integer, String> unwrappedCache = cache.unwrap( ICache.class );
unwrappedCache.put( 1, "value", new AccessedExpiryPolicy( Duration.ONE_DAY ) );
```

The `ExpirePolicy` instance can be pre-created, cached, and re-used, but only for each cache instance. This is because `ExpirePolicy`
implementations can be marked as `java.io.Closeable`. The following list shows the provided method overloads over `javax.cache.Cache`
by `com.hazelcast.cache.ICache` featuring the `ExpiryPolicy` parameter:

 - `get(key)`:
  - `get(key, expiryPolicy)`
 - `getAll(keys)`:
  - `getAll(keys, expirePolicy)`
 - `put(key, value)`:
  - `put(key, value, expirePolicy)`
 - `getAndPut(key, value)`:
  - `getAndPut(key, value, expirePolicy)`
 - `putAll(map)`:
  - `putAll(map, expirePolicy)`
 - `putIfAbsent(key, value)`:
  - `putIfAbsent(key, value, expirePolicy)`
 - `replace(key, value)`:
  - `replace(key, value, expirePolicy)`
 - `replace(key, oldValue, newValue)`:
  - `replace(key, oldValue, newValue, expirePolicy)`
 - `getAndReplace(key, value)`:
  - `getAndReplace(key, value, expirePolicy)`

Asynchronous method overloads are not listed here. Please see [Async Operations](#async-operations) for the list of asynchronous method overloads.

### Additional Methods

In addition to the operations explained in [Async Operations](#async-operations) and [Custom ExpiryPolicy](#custom-expirypolicy), Hazelcast ICache also provides a set of convenience methods. These methods are not part of the JCache specification.

 - `size()`: Returns the estimated size of the distributed cache.
 - `destroy()`: Destroys the cache and removes the data from memory. This is different from the method `javax.cache.Cache::close`.
 - `getLocalCacheStatistics()`: Returns a `com.hazelcast.cache.CacheStatistics` instance providing the same statistics data as the JMX beans. This method is not available yet on Hazelcast clients: the exception `java.lang.UnsupportedOperationException` is thrown when you use this method on a Hazelcast client.

### BackupAwareEntryProcessor

Another feature, especially interesting for distributed environments like Hazelcast, is the JCache specified
`javax.cache.processor.EntryProcessor`. For more general information, please see [JCache EntryProcessor](#jcache-entryprocessor).

Since Hazelcast provides backups of cached entries on other nodes, the default way to backup an object changed by an
`EntryProcessor` is to serialize the complete object and send it to the backup partition. This can be a huge network overhead for big objects.

Hazelcast offers a sub-interface for `EntryProcessor` called `com.hazelcast.cache.BackupAwareEntryProcessor`. This allows the user to create or pass another `EntryProcessor` to run on backup
partitions and apply delta changes to the backup entries.

The backup partition `EntryProcessor` can either be the currently running processor (by returning `this`) or it can be
a specialized `EntryProcessor` implementation (other from the currently running one) which does different operations or leaves
out operations, e.g. sending emails.

If we again take the `EntryProcessor` example from the demonstration application [JCache EntryProcessor](#jcache-entryprocessor),
the changed code will look like the following snippet.

```java
public class UserUpdateEntryProcessor
    implements BackupAwareEntryProcessor<Integer, User, User> {

  @Override
  public User process( MutableEntry<Integer, User> entry, Object... arguments )
      throws EntryProcessorException {

    // Test arguments length
    if ( arguments.length < 1 ) {
      throw new EntryProcessorException( "One argument needed: username" );
    }

    // Get first argument and test for String type
    Object argument = arguments[0];
    if ( !( argument instanceof String ) ) {
      throw new EntryProcessorException(
          "First argument has wrong type, required java.lang.String" );
    }

    // Retrieve the value from the MutableEntry
    User user = entry.getValue();

    // Retrieve the new username from the first argument
    String newUsername = ( String ) arguments[0];

    // Set the new username
    user.setUsername( newUsername );

    // Set the changed user to mark the entry as dirty
    entry.setValue( user );

    // Return the changed user to return it to the caller
    return user;
  }

  public EntryProcessor<K, V, T> createBackupEntryProcessor() {
    return this;
  }
}
```

You can use the additional method `BackupAwareEntryProcessor::createBackupEntryProcessor` to create or return the `EntryProcessor`
implementation to run on the backup partition (in the example above, the same processor again).

<br></br>
![image](images/NoteSmall.jpg) ***NOTE:*** *For the backup runs, the returned value from the backup processor is ignored and not
returned to the user.*
<br></br>

## JCache Specification Compliance

Hazelcast JCache is fully compliant with the JSR 107 TCK (Technology Compatibility Kit), therefore it is officially a JCache
implementation. This is tested by running the TCK against the Hazelcast implementation.

You can test Hazelcast JCache for compliance by executing the TCK. Just perform the instructions below:


1. Checkout the TCK from [https://github.com/jsr107/jsr107tck](https://github.com/jsr107/jsr107tck).
2. Change the properties in `tck-parent/pom.xml` as shown below.
3. Run the TCK by `mvn clean install`.


```xml
<properties>
  <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
  <project.reporting.outputEncoding>UTF-8</project.reporting.outputEncoding>

  <CacheInvocationContextImpl>
    javax.cache.annotation.impl.cdi.CdiCacheKeyInvocationContextImpl
  </CacheInvocationContextImpl>

  <domain-lib-dir>${project.build.directory}/domainlib</domain-lib-dir>
  <domain-jar>domain.jar</domain-jar>


  <!-- ################################################################# -->
  <!-- Change the following properties on the command line
       to override with the coordinates for your implementation-->
  <implementation-groupId>com.hazelcast</implementation-groupId>
  <implementation-artifactId>hazelcast</implementation-artifactId>
  <implementation-version>3.4</implementation-version>

  <!-- Change the following properties to your CacheManager and
       Cache implementation. Used by the unwrap tests. -->
  <CacheManagerImpl>
    com.hazelcast.client.cache.impl.HazelcastClientCacheManager
  </CacheManagerImpl>
  <CacheImpl>com.hazelcast.cache.ICache</CacheImpl>
  <CacheEntryImpl>
    com.hazelcast.cache.impl.CacheEntry
  </CacheEntryImpl>

  <!-- Change the following to point to your MBeanServer, so that
       the TCK can resolve it. -->
  <javax.management.builder.initial>
    com.hazelcast.cache.impl.TCKMBeanServerBuilder
  </javax.management.builder.initial>
  <org.jsr107.tck.management.agentId>
    TCKMbeanServer
  </org.jsr107.tck.management.agentId>
  <jsr107.api.version>1.0.0</jsr107.api.version>

  <!-- ################################################################# -->
</properties>
```

This will run the tests using an embedded Hazelcast Member.
