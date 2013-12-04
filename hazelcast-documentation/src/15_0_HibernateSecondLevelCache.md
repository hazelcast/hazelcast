# Hibernate Second Level Cache

Hazelcast provides distributed second level cache for your Hibernate entities, collections and queries. Hazelcast has two implementations of Hibernate 2nd level cache, one for hibernate-pre-3.3 and one for hibernate-3.3.x versions. In your Hibernate configuration file (ex: hibernate.cfg.xml), add these properties:

-   To enable use of second level cache

```xml
<property name="hibernate.cache.use_second_level_cache">true</property>
```
-   To enable use of query cache

```xml
<property name="hibernate.cache.use_query_cache">true</property>
```
-   And to force minimal puts into cache

```xml
<property name="hibernate.cache.use_minimal_puts">true</property>
```
-   To configure Hazelcast for Hibernate, it is enough to put configuration file named `hazelcast.xml` into root of your classpath. If Hazelcast can not find `hazelcast.xml` then it will use default configuration from hazelcast.jar.

-   You can define custom named Hazelcast configuration xml file with one of these Hibernate configuration properties.

```xml
<property name="hibernate.cache.provider_configuration_file_resource_path">
     hazelcast-custom-config.xml
</property>
```
or
```xml
<property name="hibernate.cache.hazelcast.configuration_file_path">
     hazelcast-custom-config.xml
</property>
```
-   You can set up Hazelcast to connect cluster as LiteMember. LiteMember is a member of the cluster, it has socket connection to every member in the cluster and it knows where the data, but does not contain any data.

```xml    
<property name="hibernate.cache.hazelcast.use_lite_member">true</property>
```
-   You can set up Hazelcast to connect cluster as Native Client. Native client is not member and it connects to one of the cluster members and delegates all cluster wide operations to it. When the relied cluster member dies, client will transparently switch to another live member. *(Native Client property takes precedence over LiteMember property.)*

```xml   
<property name="hibernate.cache.hazelcast.use_native_client">true</property>
```
    To setup Native Client properly, you should add Hazelcast **group-name**, **group-password** and **cluster member address** properties. Native Client will connect to defined member and will get addresses of all members in the cluster. If the connected member will die or leave the cluster, client will automatically switch to another member in the cluster.

```xml  
<property name="hibernate.cache.hazelcast.native_client_address">10.34.22.15</property>
<property name="hibernate.cache.hazelcast.native_client_group">dev</property>
<property name="hibernate.cache.hazelcast.native_client_password">dev-pass</property>
```
*To use Native Client you should add `hazelcast-client-<version>.jar` into your classpath.*

[Read more about NativeClient & LiteMember](#native-client)

-   To define Hibernate RegionFacyory, add following property.

```xml    
<property name="hibernate.cache.region.factory_class">
     com.hazelcast.hibernate.HazelcastCacheRegionFactory
</property>
```
    Or as an alternative you can use `HazelcastLocalCacheRegionFactory` which stores data in local node and sends invalidation messages when an entry is updated on local.

```xml
<property name="hibernate.cache.region.factory_class">
     com.hazelcast.hibernate.HazelcastLocalCacheRegionFactory
</property>
```
Hazelcast creates a separate distributed map for each Hibernate cache region. So these regions can be configured easily via Hazelcast map configuration. You can define **backup**, **eviction**, **TTL** and **Near Cache** properties.

-   [Backup Configuration](#backups)

-   [Eviction And TTL Configuration](#eviction)

-   [Near Cache Configuration](#near-cache)

Hibernate has four cache concurrency strategies: *read-only*, *read-write*, *nonstrict-read-write* and *transactional*. But Hibernate does not forces cache providers to support all strategies. And Hazelcast supports first three (**read-only**, **read-write**, **nonstrict-read-write**) of these four strategies. Hazelcast has no support for *transactional* strategy yet.

-   If you are using xml based class configurations, you should add a *cache* element into your configuration with *usage* attribute with one of *read-only*, *read-write*, *nonstrict-read-write*.

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
-   If you are using Hibernate-Annotations then you can add *class-cache* or *collection-cache* element into your Hibernate configuration file with *usage* attribute with one of *read only*, *read/write*, *nonstrict read/write*.

```xml    
<class-cache usage="read-only" class="eg.Immutable"/>
<class-cache usage="read-write" class="eg.Cat"/>
<collection-cache collection="eg.Cat.kittens" usage="read-write"/>
```
OR

-   Alternatively, you can put Hibernate Annotation's *@Cache* annotation on your entities and collections.

```java    
Cache(usage = CacheConcurrencyStrategy.READ_WRITE)
public class Cat implements Serializable {
     ...
}
```
The last thing you should be aware of is to drop hazelcast-hibernate-\<version\>.jar into your classpath.

-   **Accessing underlying `HazelcastInstance`**

    Using `com.hazelcast.hibernate.instance.HazelcastAccessor` you can access the underlying `HazelcastInstance` used by Hibernate SessionFactory.

```java   
SessionFactory sessionFactory = ...;
HazelcastInstance hazelcastInstance = HazelcastAccessor.getHazelcastInstance(sessionFactory);        
```
-   **Changing/setting lock timeout value of *read-write* strategy**

    Lock timeout value can be set using `hibernate.cache.hazelcast.lock_timeout_in_seconds` Hibernate property. Value should be in seconds and default value is 300 seconds.

-   **Using named `HazelcastInstance`**

    Instead of creating a new `HazelcastInstance` for each `SessionFactory`, an existing instance can be used by setting `hibernate.cache.hazelcast.instance_name` Hibernate property to `HazelcastInstance`'s name. For more information see [Named HazelcastInstance](#named-hazelcastinstance).

-   **Disabling shutdown during SessionFactory.close()**

    Shutting down `HazelcastInstance` can be disabled during `SessionFactory.close()` by setting `hibernate.cache.hazelcast.shutdown_on_session_factory_close` Hibernate property to false. *(In this case Hazelcast property `hazelcast.shutdownhook.enabled` should not be set to false.)* Default value is `true`.


