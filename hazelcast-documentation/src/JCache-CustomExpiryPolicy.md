### Custom ExpiryPolicy

The JCache specification has an option to configure a single `ExpiryPolicy` per cache. Hazelcast ICache extension
offers the possibility to define a custom `ExpiryPolicy` per key by providing a set of method overloads with an `expirePolicy`
parameter, as in the list of asynchronous methods in the [Async Methods section](#async-methods). This means that custom expiry policies can passed to a cache operation.

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

Asynchronous method overloads are not listed here. Please see the [Async Operations section](#async-operations) for the list of asynchronous method overloads.

