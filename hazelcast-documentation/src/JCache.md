 
# Hazelcast JCache

## JCache Overview

As part of the upcoming Java Enterprise Edition (Java EE) 8 specification JCache provides a standardized caching layer for 
applications. This caching API is specified by the Java Community Process (JCP) as Java Specification Request (JSR) 107.

Caching keeps data in memory that are either slow to calculate / process or originating from another underlying backend system 
whereas caching is used to prevent additional request round trips for frequently used data. In both cases the reasoning is to 
gain performance or decreasing application latencies.

Starting with Hazelcast release 3.3.1 a specification compliant JCache implementation is offered. To show our commitment to this
important specification the Java world was waiting for over a decade, we do not just provide a simple wrapper around our existing
APIs but implemented a caching structure from ground up to optimize the behavior to the needs of JCache. As mentioned before
the Hazelcast JCache implementation is 100% TCK (Technology Compatibility Kit) compliant and therefor passes all specification
requirements.

In addition to the given specification we added a hand full of features on top, like asynchronous versions of almost all
operations, to give the user the power he expects.  

This chapter will give a basic understanding of how to configure your application, how to setup Hazelcast to be your JCache
provider and will show examples of the basic usage as well as the additionally offered features that are not part of JSR-107.
To gain a full understanding of the JCache functionality and provided guarantees of different operations it is advised to read
the specification document (which is also the main documentation for functionality) at the specification page of JSR-107:

[https://www.jcp.org/en/jsr/detail?id=107](https://www.jcp.org/en/jsr/detail?id=107)

## Setup and Configuration

This subchapter will show was is necessary to provide JCache API and the Hazelcast JCache implementation to your application. In
addition it demonstrates the different configuration options as well as the description of the configuration properties.

### Application Setup

To provide the application with the new JCache functionality it has to have the JCache API inside it's application's
classpath. This API is the bridge between the specified standard and the implementation provided by Hazelcast.

How to integrate the JCache API JAR into the application classpath depends on the buildsystem used. For Maven, Gradle, SBT,
Ivy and a lot of other buildsystems (all systems using Maven based dependency repositories) the process is as easy as adding
the Maven coordinates to the build descriptor.

As already mentioned, next to the default Hazelcast coordinates that might be already part of the application, JCache
coordinates have to be added.
 
For Maven users the coordinates look like the following snippet, for other buildsystems the the way to describe those might
diverge:

```xml
<dependency>
  <groupId>javax.cache</groupId>
  <artifactId>cache-api</artifactId>
  <version>1.0.0</version>
</dependency>
```

To activate Hazelcast as the JCache provider implementation either, if not already available, add the hazelcast-all.jar or
hazelcast.jar to the classpath by the following Maven snippet. Again users of other buildsystems have to adjust the way of
defining the dependency to their needs:

```xml
<dependency>
  <groupId>com.hazelcast</groupId>
  <artifactId>hazelcast-all</artifactId>
  <version>3.4.0</version>
</dependency>
```

or

```xml
<dependency>
  <groupId>com.hazelcast</groupId>
  <artifactId>hazelcast</artifactId>
  <version>3.4.0</version>
</dependency>
```

When users want to use Hazelcast clients to connect to a remote cluster the hazelcast-client.jar dependency is also required
on client side applications. It is already included in the hazelcast-all.jar or can be added to the classpath using the following
Maven snippet:

```xml
<dependency>
  <groupId>com.hazelcast</groupId>
  <artifactId>hazelcast</artifactId>
  <version>3.4.0</version>
</dependency>
```

For users of other buildsystems like for example ANT users have to download those dependencies from either the JSR-107 and
Hazelcast community website ([http://www.hazelcast.org](http://www.hazelcast.org)) or from the Maven repository search page
([http://search.maven.org](http://search.maven.org)).

### JCache Configuration

TODO

## JCache Providers

JCache providers are used to create caches by a specification compliant implementation. Those providers abstract the platform
specific behavior and bindings and provide the different JCache required features.

Hazelcast has two types of providers that are possible to be use. Depending on the application setup and the cluster topology
there is either the Client Provider (used from Hazelcast clients) or the Server Provider (which is used by cluster nodes).

### Provider Configuration

JCache `javax.cache.spi.CachingProvider`s are configured by either specifying the provider at the commandline or configure
it in a declarative manner inside the typical Hazelcast configuration XML file. For more information on how to setup properties
in the XML configuration look at [Declarative Configuration](#declarative-configuration).

To configure the provider at the commandline the following parameter needs to be added to the Java startup call depending on the
chosen provider:

```plain
-Dhazelcast.jcache.provider.type=[client|server]
```

To learn more about cluster topologies and Hazelcast clients read [Hazelcast Topology](#hazelcast-topology).

### Client Provider

For cluster topologies where Hazelcast light clients are used to connect to a remote Hazelcast cluster the Client Provider is
the provider to be used to configure JCache.

This provider provides the same features as the Server provider but does not hold data on it's own but delegates requests and
calls to the remotely connected cluster.
 
The client provider is able to connect to multiple clusters at the same time. (TODO: how is this possible?)

### Server Provider

If a Hazelcast node is embedded into an application directly and the Hazelcast client is not used, the Server Provider is
required. In this case the node itself becomes part of the distributed cache and requests and operations are distributed directly
inside the cluster by it's given key.

This provider provides the same features as the Client provider but keeps data in the local Hazelcast node as well as distributes
non-owned keys to other direct cluster members.

## Introduction to the JCache API

This section explains the JCache API by providing small examples and use cases. While walking through the examples we will have
a look at a couple of the standard API classes and see how those will be used.

### Quick Basic Example

As a basic introduction how to use Hazelcast JCache integration inside an application we will have a quick look at the easiest
but typesafe example possible.

```java
// Retrieve the CachingProvider which is automatically backed by
// the chosen Hazelcast server or client provider
CachingProvider cachingProvider = Caching.getCachingProvider();

// Create a CacheManager
CacheManager cacheManager = cachingProvider.getCacheManager();

// Create a simple but typesafe configuration for the cache
MutableConfiguration<String, String> config = 
    new MutableConfiguration<String, String>()
        .setTypes(String.class, String.class);

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

Even though the example is quite simple let's take the time to work through the code lines one by one.

First of all we create or better retrieve the `javax.cache.spi.CachingProvider` using the static method from
`javax.cache.Caching::getCachingManager` which automatically picks up Hazelcast as the underlying JCache implementation if
available in the classpath. This way the Hazelcast implementation of a CachingProvider will automatically startup a new Hazelcast
node or client (depending on the chosen provider type) and pick up the configuration from either the commandline parameter
or from the classpath. How to use an existing `HazelcastInstance` will be shown later in the chapter, for now we keep it simple. 

In the next line we ask the `CachingProvider` to return a `javax.cache.CacheManager` which is the general entry point into JCache.
The `CachingProvider` therefor is responsible to create and manage named caches. 

The next few lines creating a simple `javax.cache.configuration.MutableConfiguration` to configure the cache before actually
creating it. In this case we only configure the key and value types to make the cache typesafe which is highly recommended and
checked on retrieval of the cache.

To eventually create the cache call `javax.cache.CacheManager::createCache` with a name for the cache and the previously created
configuration. This call immediately returns the created cache. If a previously created cache needs to be retrieved the 
corresponding method overloads for `javax.cache.CacheManager::getCache` can be used. Recommended is the version given in the
example to test the type parameters to be correct against the assigned configuration.

The following lines are simple `put` and `get` calls as already known from `java.util.Map` interface whereas the
`javax.cache.Cache::put` has a `void` return type and does not return the previously assigned value of the key. To imitate the
`java.util.Map::put` method a JCache cache has a method called `getAndPut`. 

### JCache API Walkthrough

This subsection will create a small account application with providing a caching layer over thought database abstraction. The
database layer will be simulated using single demo data in a simple DAO layer. To show the difference between the "database"
access and retrieving values from the cache a small waiting time is used in the DAO layer to simulate network and database
latency.

Before we move into the interesting part and start implementing the JCache caching layer we first have a quick look at some basic
classes we need for this example.

The User class is the representation of a user table in the database, to keep it simple it has just has two properties with
userId and username.

```java
public class User {
  private int userId;
  private String username;
  
  // Getters and setters
}
```

The DAO interface is also kept easy and provides just a simple method to retrieve a user by it's userId.

```java
public interface UserDAO {
  User findUserById( int userId );
}
```

The full running example that is presented in this subsection is available in the samples repository 
([here](http://github.com/hazelcast/...TODO)).

#### Roundup of basics

In the quick example we've already seen a couple of the base classes and explained how those work. I guess as a roundup we don't
need to them one by one again but quickly repeat their uses.

* `javax.cache.Caching`: 
* `javax.cache.spi.CachingProvider`: 
* `javax.cache.CacheManager`: 
* `javax.cache.configuration.MutableConfiguration`: 
* `javax.cache.Cache`:

## Hazelcast Cache Extension - ICache

Hazelcast provides extension methods to Cache API through the interface `com.hazelcast.cache.ICache`. 

It has two set of extensions:

* asynchronous version of all cache operations
* cache operations with custom `ExpiryPolicy` parameter to apply on that specific operation.


### Async operations

A method ending with `Async` is the asynchronous version of that method (for example `getAsync(K)` , `replaceAsync(K,V)`). These methods return a `Future` where you can get the result or wait the operation to be completed.


```java
ICache<String , SessionData> icache =  cache.unwrap( ICache.class );
Future<SessionData> future = icache.getAsync("key-1" ) ;
SessionData sessionData = future.get();
```
<br></br>
![image](images/NoteSmall.jpg) ***NOTE:*** *Asynchronous methods are not compatible with synchronous events.*
<br></br>

### Custom ExpiryPolicy

You can provide a custom expiry policy for a cache operation if you want to by-pass the global one already set in your config configuration.

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

## Running the JCache TCK

To run the JCache (JSR107) TCK against Hazelcast, perform the below instructions.

1. Checkout the TCK from [https://github.com/jsr107/jsr107tck](https://github.com/jsr107/jsr107tck).
2. Change the properties as below.


```java
<properties>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    <project.reporting.outputEncoding>UTF-8</project.reporting.outputEncoding>

    <CacheInvocationContextImpl>
        javax.cache.annotation.impl.cdi.CdiCacheKeyInvocationContextImpl
    </CacheInvocationContextImpl>

    <domain-lib-dir>${project.build.directory}/domainlib</domain-lib-dir>
    <domain-jar>domain.jar</domain-jar>


    <!--################################################################################################################-->
    <!--Change the following properties on the command line to override with the coordinates for your implementation-->
    <implementation-groupId>com.hazelcast</implementation-groupId>
    <implementation-artifactId>hazelcast</implementation-artifactId>
    <implementation-version>3.3.1</implementation-version>

    <!-- Change the following properties to your CacheManager and Cache implementation. Used by the unwrap tests. -->
    <CacheManagerImpl>com.hazelcast.cache.impl.HazelcastCacheManager</CacheManagerImpl>
    <CacheImpl>com.hazelcast.cache.ICache</CacheImpl>
    <CacheEntryImpl>com.hazelcast.cache.impl.CacheEntry</CacheEntryImpl>

    <!--Change the following to point to your MBeanServer, so that the TCK can resolve it. -->
    <javax.management.builder.initial>com.hazelcast.cache.impl.TCKMBeanServerBuilder</javax.management.builder.initial>
    <org.jsr107.tck.management.agentId>TCKMbeanServer</org.jsr107.tck.management.agentId>
    <jsr107.api.version>1.0.0</jsr107.api.version>

    <!--################################################################################################################-->
</properties>
```

This will run the tests using an embedded Hazelcast Member.
