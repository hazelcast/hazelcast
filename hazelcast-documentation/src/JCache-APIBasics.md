
### Roundup of Basics

In the section [Quick Example](#quick-example), we have already seen a couple of the base classes and explained how those work. Following are quick descriptions of them.

**`javax.cache.Caching`**:

The access point into the JCache API. It retrieves the general `CachingProvider` backed by any compliant JCache
implementation, such as Hazelcast JCache.

**`javax.cache.spi.CachingProvider`**:

The SPI that is implemented to bridge between the JCache API and the implementation itself. Hazelcast nodes and clients use different
providers chosen as seen in the subsection for [Provider Configuration](#provider-configuration) which enable the JCache API to
interact with Hazelcast clusters.

When a `javax.cache.spi.CachingProvider::getCacheManager` overload is used that takes a `java.lang.ClassLoader` argument, this
classloader will be part of the scope of the created `java.cache.Cache` and it is not possible to retrieve it on other nodes.
We advise not to use those overloads, those are not meant to be used in distributed environments!

**`javax.cache.CacheManager`**:

The `CacheManager` provides the capability to create new and manage existing JCache caches.

<br></br>
![image](images/NoteSmall.jpg) ***NOTE:*** *A `javax.cache.Cache` instance created with key and value types in the configuration
provides a type checking of those types at retrieval of the cache. For that reason, all non-types retrieval methods like
`getCache` throw an exception because types cannot be checked.*
<br></br>

**`javax.cache.configuration.Configuration`**, **`javax.cache.configuration.MutableConfiguration`**:

These two classes are used to configure a cache prior to retrieving it from a `CacheManager`. The `Configuration` interface,
therefore, acts as a common super type for all compatible configuration classes such as `MutableConfiguration`.

Hazelcast itself offers a special implementation (`com.hazelcast.config.CacheConfig`) of the `Configuration` interface which
offers more options on the specific Hazelcast properties that can be set to configure features like synchronous and asynchronous
backups counts or selecting the underlying [In Memory Format](#in-memory-format) of the cache. For more information on this
configuration class, please see the reference in [JCache Programmatic Configuration](#jcache-programmatic-configuration).

**`javax.cache.Cache`**:

This interface represents the cache instance itself. It is comparable to `java.util.Map` but offers special operations dedicated
to the caching use case. Therefore, for example `javax.cache.Cache::put`, unlike `java.util.Map::put`, does not return the old
value previously assigned to the given key.

<br></br>
![image](images/NoteSmall.jpg) ***NOTE:*** *Bulk operations on the `Cache` interface guarantee atomicity per entry but not over
all given keys in the same bulk operations since no transactional behavior is applied over the whole batch process.*
<br></br>

