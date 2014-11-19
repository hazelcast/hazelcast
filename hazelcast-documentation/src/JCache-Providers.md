

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

