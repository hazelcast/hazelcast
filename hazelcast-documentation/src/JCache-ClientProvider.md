

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

