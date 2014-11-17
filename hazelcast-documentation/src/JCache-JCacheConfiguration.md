

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

