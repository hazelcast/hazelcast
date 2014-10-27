 
# Hazelcast JCache

## JCache Overview

This chapter describes the basics of the standardized Java caching layer API which is commonly referred to as JCache. The JCache
caching API is specified by the Java Community Process (JCP) as Java Specification Request (JSR) 107.

Caching keeps data in memory that are either slow to calculate / process or originating from another underlying backend system 
whereas caching is used to prevent additional request round trips for frequently used data. In both cases the reasoning is to 
gain performance or decrease application latencies.

Starting with Hazelcast release 3.3.1, a specification compliant JCache implementation is offered. To show our commitment to this
important specification the Java world was waiting for over a decade, we do not just provide a simple wrapper around our existing
APIs but implemented a caching structure from ground up to optimize the behavior to the needs of JCache. As mentioned before,
the Hazelcast JCache implementation is 100% TCK (Technology Compatibility Kit) compliant and therefore passes all specification
requirements.

In addition to the given specification we added a hand full of features on top, like asynchronous versions of almost all
operations, to give the user the expected power.  

This chapter gives a basic understanding of how to configure your application, how to setup Hazelcast to be your JCache
provider and shows examples of the basic usage as well as the additionally offered features that are not part of JSR-107.
To gain a full understanding of the JCache functionality and provided guarantees of different operations, it is advised to read
the specification document (which is also the main documentation for functionality) at the specification page of JSR-107:

[https://www.jcp.org/en/jsr/detail?id=107](https://www.jcp.org/en/jsr/detail?id=107)

## Setup and Configuration

This sub-chapter shows what is necessary to provide JCache API and the Hazelcast JCache implementation for your application. In
addition, it demonstrates the different configuration options as well as the description of the configuration properties.

### Application Setup

To provide the application with the new JCache functionality, it has to have the JCache API inside its application's
classpath. This API is the bridge between the specified standard and the implementation provided by Hazelcast.

The way to integrate the JCache API JAR into the application classpath depends on the build system used. For Maven, Gradle, SBT,
Ivy and a lot of other build systems (all systems using Maven based dependency repositories), the process is as easy as adding
the Maven coordinates to the build descriptor.

As already mentioned, next to the default Hazelcast coordinates that might be already part of the application, you have to add JCache
coordinates.
 
For Maven users, the coordinates look like the following snippet: 

```xml
<dependency>
  <groupId>javax.cache</groupId>
  <artifactId>cache-api</artifactId>
  <version>1.0.0</version>
</dependency>
```
For other build systems, the way to describe those might
be different.


To activate Hazelcast as the JCache provider implementation add either `hazelcast-all.jar` or
`hazelcast.jar` to the classpath (if not already available) by either one of the following Maven snippets.

If you use `hazelcast-all.jar`: 

```xml
<dependency>
  <groupId>com.hazelcast</groupId>
  <artifactId>hazelcast-all</artifactId>
  <version>3.4.0</version>
</dependency>
```

If you use `hazelcast.jar`:

```xml
<dependency>
  <groupId>com.hazelcast</groupId>
  <artifactId>hazelcast</artifactId>
  <version>3.4.0</version>
</dependency>
```
Again users of other build systems have to adjust the way of
defining the dependency to their needs.


When users want to use Hazelcast clients to connect to a remote cluster, the `hazelcast-client.jar` dependency is also required
on the client side applications. This JAR is already included in `hazelcast-all.jar`. Or, you can add it to the classpath using the following
Maven snippet:

```xml
<dependency>
  <groupId>com.hazelcast</groupId>
  <artifactId>hazelcast</artifactId>
  <version>3.4.0</version>
</dependency>
```

For other build systems, e.g. ANT, the users have to download those dependencies from either the JSR-107 and
Hazelcast community website ([http://www.hazelcast.org](http://www.hazelcast.org)) or from the Maven repository search page
([http://search.maven.org](http://search.maven.org)).

### Quick Example

Before moving on to configuration, let's have a look at a basic introduction example. Below code shows how to use Hazelcast JCache integration
inside an application in the easiest but typesafe way possible.

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

Although the example is quite simple, let's go through the code lines one by one.

First of all, we create or better retrieve the `javax.cache.spi.CachingProvider` using the static method from
`javax.cache.Caching::getCachingManager` which automatically picks up Hazelcast as the underlying JCache implementation, if
available in the classpath. This way the Hazelcast implementation of a `CachingProvider` will automatically start a new Hazelcast
node or client (depending on the chosen provider type) and pick up the configuration from either the command line parameter
or from the classpath. We will show how to use an existing `HazelcastInstance` later in this chapter, for now we keep it simple. 

In the next line, we ask the `CachingProvider` to return a `javax.cache.CacheManager` which is the general application's entry
point into JCache. The `CachingProvider` therefore is responsible to create and manage named caches. 

The next few lines create a simple `javax.cache.configuration.MutableConfiguration` to configure the cache before actually
creating it. In this case, we only configure the key and value types to make the cache typesafe which is highly recommended and
checked on retrieval of the cache.

To eventually create the cache, we call `javax.cache.CacheManager::createCache` with a name for the cache and the previously created
configuration. This call immediately returns the created cache. If a previously created cache needs to be retrieved, the 
corresponding method overload `javax.cache.CacheManager::getCache` can be used. If the cache was created using type parameters
it is required to retrieve the cache afterward using the type checking version of `getCache`.

The following lines are simple `put` and `get` calls as already known from `java.util.Map` interface whereas the
`javax.cache.Cache::put` has a `void` return type and does not return the previously assigned value of the key. To imitate the
`java.util.Map::put` method, a JCache cache has a method called `getAndPut`. 

### JCache Configuration

Hazelcast JCache provides two different ways of cache configuration:

-  the expected, typical Hazelcast ways of programmatic (using
the Config API seen above) one, and 
- a declarative way (using `hazelcast.xml` or `hazelcast-client.xml`)
 
#### JCache Declarative Configuration

As expected, Hazelcast provides a way of declarative configuration of JCache caches using its configuration files. Using
declarative configuration makes creation of a `javax.cache.Cache` fully transparent and automatically ensures thread safety
internally. A call to `javax.cache.Cache::createCache` is not required in this case, the cache can just be retrieved using
`javax.cache.Cache::getCache` overloads and by passing in the name of the cache as defined in the configuration.

TODO: hz-prefix => looking up config file (different from 3.3.1), hazelcast:config://, hazelcast:name:// 

This section only describes the JCache provided standard properties. For Hazelcast specific properties please read the
[ICache Configuration](#icache-configuration) section.

```xml
<cache name="default">
  <key-type>java.lang.Object</key-type>
  <value-type>java.lang.Object</value-type>
  <statistics-enabled>false</statistics-enabled>
  <management-enabled>false</management-enabled>

  <read-through>true</read-through>
  <write-through>true</write-through>
  <cache-loader-factory>
    com.example.cache.MyCacheLoaderFactory
  </cache-loader-factory>
  <cache-writer-factory>
    com.example.cache.MyCacheWriterFactory
  </cache-writer-factory>
  <expiry-policy-factory>
    com.example.cache.MyExpirePolicyFactory
  </expiry-policy-factory>

  <entry-listeners>
    <entry-listener old-value-required="false" synchronous="false">
      <entry-listener-factory>
        com.example.cache.MyEntryListenerFactory
      </entry-listener-factory>
      <entry-event-filter-factory>
        com.example.cache.MyEntryEventFilterFactory
      </entry-event-filter-factory>
    </entry-listener>
    ...
  </entry-listeners>
</cache>
```

- `key-type`: The fully qualified class name of the cache key type, defaults to `java.lang.Object`.
- `value-type`: The fully qualified class name of the cache value type, defaults to `java.lang.Object`.
- `statistics-enabled`: If set to true, statistics like cache hits and misses are collected. Its default value is false.
- `management-enabled`: If set to true, JMX beans are enabled and collected statistics are provided - It doesn't automatically enables statistics collection, defaults to false.
- `read-through`: If set to true, enables read-through behavior of the cache to an underlying configured `javax.cache.integration.CacheLoader` which is also known as lazy-loading, defaults to false.
- `write-through`: If set to true, enables write-through behavior of the cache to an underlying configured `javax.cache.integration.CacheWriter` which passes any changed value to the external backend resource, defaults to false.
- `cache-loader-factory`: The fully qualified class name of the `javax.cache.configuration.Factory` implementation providing a `javax.cache.integration.CacheLoader` instance to the cache.
- `cache-writer-factory`: The fully qualified class name of the `javax.cache.configuration.Factory` implementation providing a `javax.cache.integration.CacheWriter` instance to the cache.
- `expiry-policy-factory`: The fully qualified class name of the `javax.cache.configuration.Factory` implementation providing a `javax.cache.expiry.ExpiryPolicy` instance to the cache.
- `entry-listener`: A set of attributes and elements, explained below, to describe a `javax.cache.event.CacheEntryListener`.
  - `old-value-required`: If set to true, previously assigned values for the affected keys will be sent to the `javax.cache.event.CacheEntryListener` implementation. Setting this attribute to true creates additional traffic, defaults to false.
  - `synchronous`: If set to true, the `javax.cache.event.CacheEntryListener` implementation will be called in a synchronous manner, defaults to false.
  - `entry-listener-factory`: The fully qualified class name of the `javax.cache.configuration.Factory` implementation providing a `javax.cache.event.CacheEntryListener` instance.
  - `entry-event-filter-factory`: The fully qualified class name of the `javax.cache.configuration.Factory` implementation providing a `javax.cache.event.CacheEntryEventFilter` instance.

#### JCache Programmatic Configuration

Using the programmatic configuration is fairly simple; just instantiate either `javax.cache.configuration.MutableConfiguration` if 
only JCache standard configuration is used or `com.hazelcast.config.CacheConfig` for a deeper Hazelcast integration. The latter 
class offers additional options that are specific to Hazelcast like asynchronous and synchronous backup counts.
Both classes share the same supertype interface `javax.cache.configuration.CompleteConfiguration` which is part of the JCache
standard.

<br></br>
![image](images/NoteSmall.jpg) ***NOTE:*** *It is advised to keep your own code as near as possible to the standard API of JCache
to stay vendor independent. Therefore, it is recommended to configure those values using the previously explained declarative API
and only use the `javax.cache.configuration.Configuration` or `javax.cache.configuration.CompleteConfiguration` interfaces in 
your code when passing the configuration instance around.*
<br></br>

If you don't need to configure Hazelcast specific properties, it is recommended to instantiate a
`javax.cache.configuration.MutableConfiguration` and using the setters to configure it as shown in the example in
[Quick Example](#quick-example). Since configurable properties are the same as the ones explained in 
[JCache Declarative Configuration](#jcache-declarative-configuration), they are not again mentioned here. For Hazelcast specific
properties, please read the [ICache Configuration](#icache-configuration) section.

## JCache Providers

JCache providers are used to create caches by a specification compliant implementation. Those providers abstract the platform
specific behavior and bindings, and provide the different JCache required features.

Hazelcast has two types of providers that can be used. Depending on the application setup and the cluster topology,
there is either the Client Provider (used from Hazelcast clients) or the Server Provider (which is used by cluster nodes).

### Provider Configuration

JCache `javax.cache.spi.CachingProvider`s are configured by either specifying the provider at the command line or in a declarative
manner inside the typical Hazelcast configuration XML file. For more information on how to setup properties in the XML 
configuration, please see [JCache Declarative Configuration](#jcache-declarative-configuration).

Hazelcast implements a delegating `CachingProvider` that can automatically be configured for either client or server mode and
delegates to the real underlying implementation based on the users choice. It is recommended to use this `CachingProvider`
implementation.

The delegating `CachingProvider`s fully qualified class name is:

```plain
com.hazelcast.cache.impl.HazelcastCachingProvider
```

To configure the delegating provider at the command line, the following parameter needs to be added to the Java startup call
depending on the chosen provider:

```plain
-Dhazelcast.jcache.provider.type=[client|server]
```

By default, the delegating `CachingProvider` is automatically picked up by the JCache SPI and provided in the previously shown
way. In cases where multiple `javax.cache.spi.CachingProvider` implementations reside on the classpath (like in some Application
Server scenarios), there is an additional way of selecting a `CachingProvider`: to explicitly call `Caching::getCachingProvider`
overloads and provide them using the canonical class name of the provider to be used. The class names of server and client providers
provided by Hazelcast are mentioned in the following two subsections.

<br></br>
![image](images/NoteSmall.jpg) ***NOTE:*** *Hazelcast advises to use the `Caching::getCachingProvider` overloads to select a
`CachingProvider` explicitly to make sure uploading to later environments or Application Server versions doesn't result in
unexpected behavior like a wrong `CachingProvider` to be chosen.*
<br></br>

For more information on cluster topologies and Hazelcast clients, please see  [Hazelcast Topology](#hazelcast-topology).

### JCache Client Provider

For cluster topologies where Hazelcast light clients are used to connect to a remote Hazelcast cluster, the Client Provider is
the provider to be used to configure JCache.

This provider provides the same features as the Server provider but does not hold data on its own but delegates requests and
calls to the remotely connected cluster.
 
The client provider is able to connect to multiple clusters at the same time. (???TODO: how is this possible? ???)

For requesting this CachingProvider using `Caching#getCachingProvider( String )` or
`Caching#getCachingProvider( String, ClassLoader )`, use the following fully qualified class name:

```plain
com.hazelcast.client.cache.impl.HazelcastClientCachingProvider
```

### JCache Server Provider

If a Hazelcast node is embedded into an application directly and the Hazelcast client is not used, the Server Provider is
required. In this case, the node itself becomes a part of the distributed cache and requests and operations are distributed
directly across the cluster by its given key.

This provider provides the same features as the Client provider, but keeps data in the local Hazelcast node and also distributes
non-owned keys to other direct cluster members.

??? TODO: URI -> namespacing, different from client! ???

For requesting this CachingProvider using `Caching#getCachingProvider( String )` or
`Caching#getCachingProvider( String, ClassLoader )`, use the following fully qualified class name:

```plain
com.hazelcast.cache.impl.HazelcastServerCachingProvider
```

## Introduction to the JCache API

This section explains the JCache API by providing simple examples and use cases. While walking through the examples, we will have
a look at a couple of the standard API classes and see how those are used.

### JCache API Walk-through

This subsection creates a small account application with providing a caching layer over an imagined database abstraction. The
database layer will be simulated using single demo data in a simple DAO interface. To show the difference between the "database"
access and retrieving values from the cache, a small waiting time is used in the DAO implementation to simulate network and
database latency.

Before we move into the interesting part and start implementing the JCache caching layer, we first have a quick look at some basic
classes we need for this example.

The `User` class is the representation of a user table in the database. To keep it simple, it has just has two properties: 
`userId` and `username`.

```java
public class User {
  private int userId;
  private String username;
  
  // Getters and setters
}
```

The DAO interface is also kept easy and it provides just a simple method to retrieve a user by its `userId`.

```java
public interface UserDAO {
  User findUserById( int userId );
  boolean storeUser( int userId, User user );
  boolean removeUser( int userId );
  Collection<Integer> allUserIds();
}
```

To show most of the standard features the configuration itself is more complex this time.

```java
// Create javax.cache.configuration.CompleteConfiguration subclass
CompleteConfiguration<Integer, User> config =
    new MutableConfiguration<Integer, User>()
        // Configure the cache to be typesafe
        .setTypes( Integer.class, User.class )
        // Configure to expire entries 30 secs after creation in the cache
        .setExpiryPolicyFactory( FactoryBuilder.factoryOf(
            new AccessedExpiryPolicy( new Duration( TimeUnit.SECONDS, 1 ) )
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

Compared with the previous basic configuration this is pretty complex but it seems way more complex than it actually is. So let's
go through it line by line.

First of all we set the expected types for the cache which already known from the previous example, in the next line an
`javax.cache.expiry.ExpirePolicy` is configured. Like almost all integration `ExpirePolicy` implementations are configured using
`javax.cache.configuration.Factory` instances. `Factory` and `FactoryBuilder` are explained later in this chapter, for now let's
just recognise them as given.

The next two lines configure the thread to be read-through and write-through to the underlying backend resource that is configured
over the next few lines. JCache API offers `javax.cache.integration.CacheLoader` and `javax.cache.integration.CacheWriter` to
implement adapter classes to any kind of backend resource, e.g. JPA, JDBC, or any other backend technology implementable in Java.
The interfaces provides the typical CRUD operations like create, get, update, delete and some bulk operation versions of those
common operations. We will look into the implementation of those implementations in a bit.

The last configuration setting defines entry listeners based on sub-interfaces of `javax.cache.event.CacheEntryListener`. This
config does not use a `javax.cache.event.CacheEntryEventFilter` since the listener is meant to be fired on every change that
happens on the cache. Again we will look in the implementation of the listener in later in this chapter.

Now let's walk through the different interfaces and classes provided by JCache, a full running example that is presented in this
subsection is available in the samples repository ([here](http://github.com/hazelcast/...TODO)). The application is built to be a
commandline app and therefore offers a small shell to accept different commands. After startup enter help to see all available
commands including their description.

### Roundup of Basics

In the section [Quick Example](#quick-example), we have already seen a couple of the base classes and explained how those work. 
Below, we quickly repeat their usages.

*_javax.cache.Caching_:*

The access point into the JCache API. It is used to retrieve the general `CachingProvider` backed by any compliant JCache
implementation like Hazelcast JCache.

*_javax.cache.spi.CachingProvider_:*

The SPI that is implemented to bridge between JCache API and the implementation itself. Hazelcast nodes and clients use different
providers chosen as seen in the subsection for [Provider Configuration](#provider-configuration) which enable the JCache API to
interact with Hazelcast clusters.

When a `javax.cache.spi.CachingProvider::getCacheManager` overload is used that takes a `java.lang.ClassLoader` argument, this
classloader will be part of the scope of the created `java.cache.Cache` and it is not possible to retrieve it on other nodes.
We advise not to use those overloads, those are not meant to be used in distributed environments!

*_javax.cache.CacheManager_:*

The `CacheManager` provides the capability to create new and manage existing JCache caches.
  
<br></br>
![image](images/NoteSmall.jpg) ***NOTE:*** *A `javax.cache.Cache` instance created with key and value types in the configuration
provides a type checking of those types at retrieval of the cache. For that reason, all non-types retrieval methods like
`getCache` throw an exception because types cannot be checked.*
<br></br>
  
*_javax.cache.configuration.Configuration_, _javax.cache.configuration.MutableConfiguration_:*

These two classes are used to configure a cache prior to retrieving it from a `CacheManager`. The `Configuration` interface,
therefore, acts as a common super type for all compatible configuration classes such as `MutableConfiguration`.

Hazelcast itself offers a special implementation (`com.hazelcast.config.CacheConfig`) of the `Configuration` interface which
offers more options on the specific Hazelcast properties that can be set to configure features like synchronous and asynchronous
backups counts or selecting the underlying [In Memory Format](#in-memory-format) of the cache. For more information on this
configuration class, please see the reference in [JCache Programmatic Configuration](#jcache-programmatic-configuration).
 
*_javax.cache.Cache_:*

This interface represents the cache instance itself. It is comparable to `java.util.Map` but offers special operations dedicated
to the caching use case. Therefore, for example `javax.cache.Cache::put`, unlike `java.util.Map::put`, does not return the old 
value previously assigned to the given key.

<br></br>
![image](images/NoteSmall.jpg) ***NOTE:*** *Bulk operations on the `Cache` interface guarantee atomicity per entry but not over 
all given keys in the same bulk operations since no transactional behavior is applied over the whole batch process.*
<br></br>

### Factory and FactoryBuilder

`javax.cache.configuration.Factory` implementations are used for configuration of the different features like 
`CacheEntryListener`, `ExpirePolicy` and `CacheLoader`s or `CacheWriter`s. Those factories are required for distribution of the
different features to nodes in a cluster environment like Hazelcast, therefore factories have to be serializable.

`Factory` implementations are easy to implement and follow the default Provider- or Factory-Pattern, 
`UserCacheEntryListenerFactory` demonstrates the process to implement a custom JCache `Factory`.

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

To simplify the process for users JCache API offers a set of useful helper methods collected in
`javax.cache.configuration.FactoryBuilder`. In the above configuration example `FactoryBuilder::factoryOf` is used to create a 
singleton factory for the given instance.

### CacheLoader

A `javax.cache.integration.CacheLoader` is meant to load cache entries from any external backend resource. If the cache is
configured to be `read-through` then `CacheLoader::load` is called transparently from the cache when the key or the value is not
yet found in the cache. If no value is found for a given key it is expected to return null.

If the cache is not configured to be `read-through` nothing is loaded automatically but `javax.cache.Cache::loadAll` have to
be called by user code to load data for the given set of keys into the cache.

For the bulk load operation every non found key have not to be part of the returned result set of operation, it also takes a
`javax.cache.integration.CompletionListener` parameter is used as an asynchronous callback after all key-value pairs since loading
a lot of pairs can take lots of time.

Let's have a look at the `UserCacheLoader` implementation.

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

Not much words need to be spoken about the implementation since it is quite straight forward. The only important note is, that
any kind of exception has to be wrapped into `javax.cache.integration.CacheLoaderException`.

### CacheWriter

A `javax.cache.integration.CacheWriter` is used to update an external backend resource. If the cache is configured to be
`write-through` this process is executed transparently to the users code otherwise at the current state there is no way to trigger
writing changed entries to the external resource to a user defined point in time.

If bulk operations throw an exception the given `java.util.Collection` has to be cleaned from all successfully written keys so
the cache implementation is able to determine what keys are written and can be applied to the cache state.

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

By a `javax.cache.processor.EntryProcessor` it is possible to apply an atomic function to a cache entry. In a distributed
environment like Hazelcast it might be interesting to move the mutating function to the node that owns the key. If the value
object is big it might prevents traffic by sending the object to the mutator and sending it back to the owner to update it.

Hazelcast JCache by default sends the complete changed value to the backup partition which again can cause a lot of traffic if
the object is big. Another option to prevent is part of the Hazelcast ICache extension. Further information are available at
[BackupAwareEntryProcessor](#backupawareentryprocessor).

An arbitrary number of arguments can be passed to `Cache::invoke` and `Cache::invokeAll` methods. All of those arguments need
to be fully serializable since in a distributed environment like Hazelcast it is very likely that they have to be passed around
the cluster.

```java
public class UserUpdateEntryProcessor implements EntryProcessor<Integer, User, User> {

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
single cache entry. No transactional rules are applied to the bulk operation at all.*

![image](images/NoteSmall.jpg) ***NOTE:*** *JCache `EntryProcessor` implementations are not allowed to call any kind 
`javax.cache.Cache` methods to prevent operations from deadlocking between different calls.*
<br></br>

In addition when using `Cache::invokeAll` method a `java.util.Map` is returned mapping the key to its 
`javax.cache.processor.EntryProcessorResult` which itself wraps the actual result or a thrown
`javax.cache.processor.EntryProcessorException`.

### CacheEntryListener

The `javax.cache.event.CacheEntryListener` implementation again is really straight forward. `CacheEntryListener` itself is just
a super-interface which is used as a marker for listener classes in JCache. A set of sub-interfaces is brought by the
specification.

- `CacheEntryCreatedListener`: Fires after a cache entry is added (even on read-through by a `CacheLoader`) to the cache.
- `CacheEntryUpdatedListener`: Fires after an already existing cache entry was updates. 
- `CacheEntryRemovedListener`: Fires after a cache entry was removed (not expired) from the cache.
- `CacheEntryExpiredListener`: Fires after a cache entry has been expired. Expiry does not have to be parallel process, it is only required to be executed on the keys that are requested by `Cache::get` and some other operations. For a full table of expiry please see the [https://www.jcp.org/en/jsr/detail?id=107](https://www.jcp.org/en/jsr/detail?id=107) point 6.  

`CacheEntryListener` configuration is done by adding a `javax.cache.configuration.CacheEntryListenerConfiguration` instance to
the JCache configuration class, as seen in the above example configuration. In addition listeners can be configured to be
executed synchronously (blocking the calling thread) or asynchronously (fully running in parallel).

In the example application the listener is just implemented to print event information on the console to visualize what if going
on in the cache that means the implementation is again pretty easy.

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

In JCache `javax.cache.expiry.ExpirePolicy` implementations are used to automatically expire cache entries based on different
rules.

Expiry timeouts are defined using `javax.cache.expiry.Duration` which itself is a pair of a `java.util.concurrent.TimeUnit` 
describing a time unit and a long to define the timeout value. The minimum allowed `TimeUnit` is `TimeUnit.MILLISECONDS` and
the long value `durationAmount` must be equal or greater than zero. A duration of zero (or `Duration.ZERO`) is indicated that the
cache entry is meant to be expired now.

By default JCache delivers a set of predefined expiry strategies in the standard API:

- `AccessedExpiryPolicy`: Expires after a given set of time measured from creation of the cache entry, the expiry timeout is updated on accessing the key.
- `CreatedExpiryPolicy`: Expires after a given set of time measured from creation of the cache entry, the expiry timeout is never updated.
- `EternalExpiryPolicy`: Never expires, this is the default behavior, similar to `ExpiryPolicy` to be set to null.
- `ModifiedExpiryPolicy`: Expires after a given set of time measured from creation of the cache entry, the expiry timeout is updated on updating the key. 
- `TouchedExpiryPolicy`: Expires after a given set of time measured from creation of the cache entry, the expiry timeout is updated on accessing or updating the key. 

Whereas `EternalExpirePolicy` does not expire cache entries, it is still possible to evict values from memory if an underlying
`CacheLoader` is defined.

### Scopes and Namespaces


## Hazelcast JCache Extension - ICache

Hazelcast provides extension methods to Cache API through the interface `com.hazelcast.cache.ICache`. 

It has two set of extensions:

* Asynchronous version of all cache operations.
* Cache operations with custom `ExpiryPolicy` parameter to apply on that specific operation.

### Retrieving an ICache Instance

### ICache Configuration

### Async Operations

A method ending with `Async` is the asynchronous version of that method (for example `getAsync(K)`, `replaceAsync(K,V)`). These methods return a `Future` where you can get the result or wait the operation to be completed.


```java
ICache<String , SessionData> cache = cache.unwrap( ICache.class );
Future<SessionData> future = cache.getAsync("key-1" ) ;
SessionData sessionData = future.get();
```

<br></br>
![image](images/NoteSmall.jpg) ***NOTE:*** *Asynchronous methods are not compatible with synchronous events.*
<br></br>

### Additional Methods

### BackupAwareEntryProcessor

### Custom ExpiryPolicy

You can provide a custom expiry policy for a cache operation if you want to by-pass the global one already set in your `config` configuration.

Using the cache configuration, you can set an expiration of one minute as shown in the sample code below.

```java
MutableConfiguration<String, String> config = new MutableConfiguration<String, String>();
config.setExpiryPolicyFactory(AccessedExpiryPolicy.factoryOf(ONE_MINUTE));
```

And you use the cache as usual:


```java
cache.put(“session-key-1”, SessionData);
```

This will use the global configuration and if we by-pass the policy and want to use a different expiry policy for some operation,

```java
ICache<String , SessionData> icache =  cache.unwrap( ICache.class );
icache.put("session-key-2", SessionData,  AccessedExpiryPolicy.factoryOf(TEN_MINUTE) );
```

Now, your customized session will expire in ten minutes after being accessed.

## JCache Specification Compliance

Hazelcast JCache is fully compliant to the JSR 107 TCK (Technology Compatibility Kit), therefore it is officially a JCache
implementation. This is tested by running the TCK against the Hazelcast implementation.

Everybody can test Hazelcast JCache for compliance by executing the TCK on his own, just perform the instructions below:

1. Checkout the TCK from [https://github.com/jsr107/jsr107tck](https://github.com/jsr107/jsr107tck).
2. Change the properties as below.


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
  <implementation-version>3.3.1</implementation-version>

  <!-- Change the following properties to your CacheManager and 
       Cache implementation. Used by the unwrap tests. -->
  <CacheManagerImpl>
    com.hazelcast.cache.impl.HazelcastCacheManager
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
