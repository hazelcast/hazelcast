 
# Hazelcast JCache Implementation

## JCache Overview

Starting with release 3.3.1, Hazelcast offers its JCache (Java Cache) implementation. JCache is the official caching API of Java and it provides a common caching specification for the Java platform. It makes it easy for Java developers to cache data, which in many cases improves the performance.  When it comes to handling huge and non-stop changing data, caching is especially important.

Hazelcast provides a built-in distributed implementation of JSR-107 using Hazelcast infrastructure. It is 100% TCK (Technology Compatibility Kit) compliant. 

This chapter explains the usage of Hazelcast’s JCache implementation. For the full details of JCache, please visit its website at Java Community Process (JCP):

[https://www.jcp.org/en/jsr/detail?id=107](#https://www.jcp.org/en/jsr/detail?id=107)

## Setup
Very similar to Hazelcast's setup and usage, it is as easy as adding a dependency or dropping a jar file.

Hazelcast has built-in JCache implementaion which will be enabled by just adding the `cache-api` dependency. When Hazelcast sees the `javax.cache.Caching` class on the classpath, it will just enable JCache.

- Add the file `hazelcast-3.3.1.jar` to your classpath or add it as a dependency.
- Download the `cache-api` from maven repo or add it as a dependency as shown below.

```xml
<dependency>
  <groupId>javax.cache</groupId>
  <artifactId>cache-api</artifactId>
  <version>1.0.0</version>
</dependency>
```
## Provider Types

Hazelcast has two types of providers that you can use. You can think of these two types as Hazelcast client and server in terms of their usage purposes.

### Client Provider

In order to access the distributed cache cluster through light clients, client provider is the JCache provider to be used. This is like using a Hazelcast client.

### Server Provider

If you want to embed JCache server into your application, you can use the server provider. This is actually like embedding a Hazelcast node into your application.

## Provider Setup

After adding the `cache-api` dependency, there are three options to use Hazelcast. You will add one of these with the related default provider as

1. hazelcast-VERSION.jar ==> default provider: Server 
2. hazelcast-client-VERSION.jar ==> default provider: Client
3. hazelcast-all-VERSION.jar ==> default provider: Client

Which ever you add as dependency will enable its default cache provider. If you add hazelcast-client or hazelcast-all jar, but want to force using the server caching provider mode then you can use

`-Dhazelcast.jcache.provider.type=[client|server]`

system parameter to setup the provider type.


## Usage

After setting dependencies, you can start using JCache as described by the specification JSR 107. Please note that Hazelcast specific configurations still can be performed as described by this document.

## Sample JCache Code

A sample code is shown below.

```java
CachingProvider cachingProvider = Caching.getCachingProvider();
CacheManager cacheManager = cachingProvider.getCacheManager();

//configure the cache
MutableConfiguration<String, String> config = new MutableConfiguration<String, String>();
config.setStoreByValue(true)
.setTypes(String.class, String.class)
.setExpiryPolicyFactory(AccessedExpiryPolicy.factoryOf(ONE_MINUTE))
.setStatisticsEnabled(false);

//create the cache
cacheManager.createCache(name, config);

//get the cache
Cache<String, Integer> cache = cacheManager.getCache(name, String.class, String.class);
cache.put("theKey", "Hello World");
String value = cache.get("theKey");
System.out.println(value);//prints 'Hello World'
```

## Hazelcastcache Extension - ICache

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
***NOTE:*** *Asynchronous methods are not compatible with synchronous events.*
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

## Code Samples

Please see [Hazelcast JCache Code Samples](https://github.com/hazelcast/hazelcast-code-samples/tree/master/jcache/src/main/java/com/hazelcast/examples) for more examples.

