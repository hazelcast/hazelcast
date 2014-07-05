# Integrated Clustering

## Hibernate Second Level Cache

Hazelcast provides distributed second level cache for your Hibernate entities, collections and queries. 


### Supported Hibernate Versions

- hibernate 3.3.x+
- hibernate 4.x

### Hibernate Configuration

First of all, add `hazelcast-hibernate3-<`*hazelcastversion*`>.jar` or `hazelcast-hibernate4-<`*hazelcastversion*`>.jar`  into your classpath depending on your Hibernate version.

Then add below properties into your Hibernate configuration file (e.g. `hibernate.cfg.xml`).

**Enabling the use of second level cache**

```xml
<property name="hibernate.cache.use_second_level_cache">true</property>
```

**Hibernate RegionFactory**

-   *HazelcastCacheRegionFactory*

	`HazelcastCacheRegionFactory` uses Hazelcast Distributed Map to cache the data, so all cache operations go through the wire.

	```xml    
	<property name="hibernate.cache.region.factory_class">
	  com.hazelcast.hibernate.HazelcastCacheRegionFactory
	</property>
	```

-   *HazelcastLocalCacheRegionFactory*

	You can use `HazelcastLocalCacheRegionFactory` which stores data in local node and sends invalidation messages when an entry is updated/deleted on local.

	```xml
	<property name="hibernate.cache.region.factory_class">
	  com.hazelcast.hibernate.HazelcastLocalCacheRegionFactory
	</property>
	```

**Optional Settings**

-   To enable use of query cache:

	```xml
	<property name="hibernate.cache.use_query_cache">true</property>
	```

-   And to force minimal puts into cache:

	```xml
	<property name="hibernate.cache.use_minimal_puts">true</property>
	```

### Hazelcast Configuration

-   To configure Hazelcast for Hibernate, it is enough to put configuration file named `hazelcast.xml` into root of your classpath. If Hazelcast cannot find `hazelcast.xml`, then it will use the default configuration from `hazelcast.jar`.

-   You can define custom named Hazelcast configuration XML file with one of these Hibernate configuration properties.

	-	```xml
<property name="hibernate.cache.provider_configuration_file_resource_path">
  hazelcast-custom-config.xml
</property>
```

	or

	-	```xml
<property name="hibernate.cache.hazelcast.configuration_file_path">
  hazelcast-custom-config.xml
</property>
```

Hazelcast creates a separate distributed map for each Hibernate cache region. So, these regions can be configured easily via Hazelcast map configuration. You can define **backup**, **eviction**, **TTL** and **Near Cache** properties.

- [Backup Configuration](#map-backups)

- [Eviction And TTL Configuration](#eviction)

- [Near Cache Configuration](#near-cache)

### RegionFactory Options

##### HazelcastCacheRegionFactory

HazelcastCacheRegionFactory uses standard Hazelcast distributed maps. Therefore, all operations like get, put and remove will be performed using the distributed map logic. The only downside of using HazelcastCacheRegionFactory may be the lower performance compared to HazelcastLocalCacheRegionFactory since operations are handled as distributed calls.

***NOTE:*** *If you use HazelcastCacheRegionFactory, you can see your maps on [Management Center](#management-center).*



##### HazelcastLocalCacheRegionFactory

In this option, each cluster member has a local map and each of them is registered to a Hazelcast Topic (ITopic). Whenever a `put` or `remove` operation is performed on a member, an invalidation message is generated on the ITopic and sent to other members. And those other members also remove the related key, value pair on their local maps as soon as they get these invalidation messages. New value is only updated on this member when `get` operation is run for that key. In the case of `get` operations, invalidation messages are not generated and reads are performed on the local map.

An illustration of the above logic is shown below.

![image](images/HZLocalCacheRgnFactory.jpg)

If your operations are mostly read ones, then this option is a better one regarding performance.

***NOTE:*** *If you use HazelcastLocalCacheRegionFactory, you cannot see your maps on [Management Center](#management-center).*





### Hazelcast Modes for Hibernate Usage

Hibernate 2nd Level Cache can use Hazelcast in two modes: P2P and Client/Server

##### P2P (Peer-to-Peer)

With P2P mode, each Hibernate deployment launches its own Hazelcast Instance. You can also configure Hibernate to use an existing instance so instead of creating a new `HazelcastInstance` for each `SessionFactory`, an existing instance can be used by setting `hibernate.cache.hazelcast.instance_name` Hibernate property to `HazelcastInstance`'s name. For more information see [Named HazelcastInstance](#named-hazelcastinstance).

**Disabling shutdown during SessionFactory.close()**

Shutting down `HazelcastInstance` can be disabled during `SessionFactory.close()`. To achieve this set the Hibernate property `hibernate.cache.hazelcast.shutdown_on_session_factory_close` to false. *(In this case Hazelcast property `hazelcast.shutdownhook.enabled` should not be set to false.)* Default value is `true`.


##### CLIENT/SERVER

-   You can set up Hazelcast to connect to the cluster as Native Client. Native client is not a member; it connects to one of the cluster members and delegates all cluster wide operations to it. When the relied cluster member dies, client will transparently switch to another live member.

```xml   
<property name="hibernate.cache.hazelcast.use_native_client">true</property>
```

To setup Native Client properly, you should add Hazelcast **group-name**, **group-password** and **cluster member address** properties. Native Client will connect to defined member and will get addresses of all members in the cluster. If the connected member dies or leaves the cluster, client will automatically switch to another member in the cluster.

```xml  
<property name="hibernate.cache.hazelcast.native_client_address">10.34.22.15</property>
<property name="hibernate.cache.hazelcast.native_client_group">dev</property>
<property name="hibernate.cache.hazelcast.native_client_password">dev-pass</property>
```

***NOTE***: *To use Native Client, you should add `hazelcast-client-<version>.jar` into your classpath. Refer to [Native Clients](#native-client) for more information.*

### Hibernate Concurrency Strategies

Hibernate has four cache concurrency strategies: *read-only*, *read-write*, *nonstrict-read-write* and *transactional*. But, Hibernate does not force cache providers to support all strategies. Hazelcast supports first three (**read-only**, **read-write**, **nonstrict-read-write**) of these strategies. It has no support for *transactional* strategy yet.

-   If you are using XML based class configurations, you should add a *cache* element into your configuration with *usage* attribute with one of *read-only*, *read-write*, *nonstrict-read-write*.
   
```xml
<class name="eg.Immutable" mutable="false">
  <cache usage="read-only"/>
  .... 
</class>

<class name="eg.Cat" .... >
  <cache usage="read-write"/>
  ....
  <set name="kittens" ... >
    <cache usage="read-write"/>
    ....
  </set>
</class>
```
-   If you are using Hibernate-Annotations, then you can add *class-cache* or *collection-cache* element into your Hibernate configuration file with *usage* attribute with one of *read only*, *read/write*, *nonstrict read/write*.

```xml    
<class-cache usage="read-only" class="eg.Immutable"/>
<class-cache usage="read-write" class="eg.Cat"/>
<collection-cache collection="eg.Cat.kittens" usage="read-write"/>
```

OR

-   Alternatively, you can put Hibernate Annotation's *@Cache* annotation on your entities and collections.

```java    
@Cache(usage = CacheConcurrencyStrategy.READ_WRITE)
public class Cat implements Serializable {
  ...
}
```

### Advanced Settings

**Accessing underlying HazelcastInstance**

Using `com.hazelcast.hibernate.instance.HazelcastAccessor` you can access the underlying `HazelcastInstance` used by Hibernate SessionFactory.

```java   
SessionFactory sessionFactory = ...;
HazelcastInstance hazelcastInstance = HazelcastAccessor
    .getHazelcastInstance(sessionFactory);        
```

**Changing/setting lock timeout value of *read-write* strategy**

Lock timeout value can be set using `hibernate.cache.hazelcast.lock_timeout_in_seconds` Hibernate property. Value should be in seconds and default value is 300 seconds.



<br></br>