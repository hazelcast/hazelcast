

### JCache Server Provider

If a Hazelcast node is embedded into an application directly and the Hazelcast client is not used, the Server Provider is
required. In this case, the node itself becomes a part of the distributed cache and requests and operations are distributed
directly across the cluster by its given key.

The Server Provider provides the same features as the Client provider, but it keeps data in the local Hazelcast node and also distributes
non-owned keys to other direct cluster members.

Like the Client Provider, the Server Provider is able to connect to multiple clusters at the same time. This can be achieved by scoping the client side `CacheManager` with different Hazelcast configuration files. For more
information please see the [Scopes and Namespaces section](#scopes-and-namespaces).

To request this `CachingProvider` using `Caching#getCachingProvider( String )` or
`Caching#getCachingProvider( String, ClassLoader )`, use the following fully qualified class name:

```plain
com.hazelcast.cache.impl.HazelcastServerCachingProvider
```

