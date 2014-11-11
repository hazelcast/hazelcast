

## Hazelcast JCache Extension - ICache

Hazelcast provides extension methods to Cache API through the interface `com.hazelcast.cache.ICache`.

It has two sets of extensions:

* Asynchronous version of all cache operations.
* Cache operations with custom `ExpiryPolicy` parameter to apply on that specific operation.

### Scopes and Namespaces

As mentioned before, a `CacheManager` can be scoped in the case of client to connect to multiple clusters, or in the case of an embedded node, a `CacheManager` can be scoped to join different clusters at the same time. This process is called scoping. To apply it, request
a `CacheManager` by passing a `java.net.URI` instance to `CachingProvider::getCacheManager`. The `java.net.URI` instance must point to either a Hazelcast configuration or to the name of a named
`com.hazelcast.core.HazelcastInstance` instance.

<br></br>
![image](images/NoteSmall.jpg) ***NOTE:*** *Multiple requests for the same `java.net.URI` result in returning a `CacheManager`
instance that shares the same `HazelcastInstance` as the `CacheManager` returned by the previous call.*
<br></br>

#### Configuration Scope

To connect or join different clusters, apply a configuration scope to the `CacheManager`. If the same `URI` is
used to request a `CacheManager` that was created previously, those `CacheManager`s share the same underlying `HazelcastInstance`.

To apply a configuration scope, pass in the path of the configuration file using the location property
`HazelcastCachingProvider#HAZELCAST_CONFIG_LOCATION` (which resolves to `hazelcast.config.location`) as a mapping inside a
`java.util.Properties` instance to the `CachingProvider#getCacheManager(uri, classLoader, properties)` call.

Here is an example of using Configuration Scope.

```java
CachingProvider cachingProvider = Caching.getCachingProvider();

// Create Properties instance pointing to a Hazelcast config file
Properties properties = new Properties();
properties.setProperty( HazelcastCachingProvider.HAZELCAST_CONFIG_LOCATION,
    "classpath://my-configs/scoped-hazelcast.xml" );

URI cacheManagerName = new URI( "my-cache-manager" );
CacheManager cacheManager = cachingProvider
    .getCacheManager( cacheManagerName, null, properties );
```

Here is an example using `HazelcastCachingProvider::propertiesByLocation` helper method.

```java
CachingProvider cachingProvider = Caching.getCachingProvider();

// Create Properties instance pointing to a Hazelcast config file
String configFile = "classpath://my-configs/scoped-hazelcast.xml";
Properties properties = HazelcastCachingProvider
    .propertiesByLocation( configFile );

URI cacheManagerName = new URI( "my-cache-manager" );
CacheManager cacheManager = cachingProvider
    .getCacheManager( cacheManagerName, null, properties );
```

The retrieved `CacheManager` is scoped to use the `HazelcastInstance` that was just created and was configured using the given XML
configuration file.

Available protocols for config file URL include `classpath://` to point to a classpath location, `file://` to point to a filesystem
location, `http://` an `https://` for remote web locations. In addition, everything that does not specify a protocol is recognized
as a placeholder that can be configured using a system property.

```java
String configFile = "my-placeholder";
Properties properties = HazelcastCachingProvider
    .propertiesByLocation( configFile );
```

Can be set on the command line by:

```plain
-Dmy-placeholder=classpath://my-configs/scoped-hazelcast.xml
```

<br></br>
![image](images/NoteSmall.jpg) ***NOTE:*** *No check is performed to prevent creating multiple `CacheManager`s with the same cluster
configuration on different configuration files. If the same cluster is referred from different configuration files, multiple
cluster members or clients are created.*

![image](images/NoteSmall.jpg) ***NOTE:*** *The configuration file location will not be a part of the resulting identity of the
`CacheManager`. An attempt to create a `CacheManager` with a different set of properties but an already used name will result in
undefined behavior.*
<br></br>

#### Named Instance Scope

A `CacheManager` can be bound to an existing and named `HazelcastInstance` instance. This requires that the instance was created
using a `com.hazelcast.config.Config` and requires that an `instanceName` be set. Multiple `CacheManager`s created using an equal
`java.net.URI` will share the same `HazelcastInstance`.

A named scope is applied nearly the same way as the configuration scope: pass in the instance name using the
`HazelcastCachingProvider#HAZELCAST_INSTANCE_NAME` (which resolves to `hazelcast.instance.name`) property as a mapping inside a
`java.util.Properties` instance to the `CachingProvider#getCacheManager(uri, classLoader, properties)` call.

Here is an example of Named Instance Scope.

```java
Config config = new Config();
config.setInstanceName( "my-named-hazelcast-instance" );
// Create a named HazelcastInstance
Hazelcast.newHazelcastInstance( config );

CachingProvider cachingProvider = Caching.getCachingProvider();

// Create Properties instance pointing to a named HazelcastInstance
Properties properties = new Properties();
properties.setProperty( HazelcastCachingProvider.HAZELCAST_INSTANCE_NAME,
     "my-named-hazelcast-instance" );

URI cacheManagerName = new URI( "my-cache-manager" );
CacheManager cacheManager = cachingProvider
    .getCacheManager( cacheManagerName, null, properties );
```

Here is an example using `HazelcastCachingProvider::propertiesByInstanceName` method.

```java
Config config = new Config();
config.setInstanceName( "my-named-hazelcast-instance" );
// Create a named HazelcastInstance
Hazelcast.newHazelcastInstance( config );

CachingProvider cachingProvider = Caching.getCachingProvider();

// Create Properties instance pointing to a named HazelcastInstance
Properties properties = HazelcastCachingProvider
    .propertiesByInstanceName( "my-named-hazelcast-instance" );

URI cacheManagerName = new URI( "my-cache-manager" );
CacheManager cacheManager = cachingProvider
    .getCacheManager( cacheManager, null, properties );
```

<br></br>
![image](images/NoteSmall.jpg) ***NOTE:*** *The `instanceName` will not be a part of the resulting identity of the `CacheManager`.
An attempt to create a `CacheManager` with a different set of properties but an already used name will result in undefined behavior.*
<br></br>


#### Namespaces

The `java.net.URI`s that don't use the above mentioned Hazelcast specific schemes are recognized as namespacing. Those
`CacheManager`s share the same underlying default `HazelcastInstance` created (or set) by the `CachingProvider`, but they cache with the
same names but differently namespaces on `CacheManager` level, and therefore won't share the same data. This is useful where multiple
applications might share the same Hazelcast JCache implementation (e.g. on application or OSGi servers) but are developed by
independent teams. To prevent interfering on caches using the same name, every application can use its own namespace when
retrieving the `CacheManager`.

Here is an example of using namespacing.

```java
CachingProvider cachingProvider = Caching.getCachingProvider();

URI nsApp1 = new URI( "application-1" );
CacheManager cacheManagerApp1 = cachingProvider.getCacheManager( nsApp1, null );

URI nsApp2 = new URI( "application-2" );
CacheManager cacheManagerApp2 = cachingProvider.getCacheManager( nsApp2, null );
```

That way both applications share the same `HazelcastInstance` instance but not the same caches.

