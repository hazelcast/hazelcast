

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

