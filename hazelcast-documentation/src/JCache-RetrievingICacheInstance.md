
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

