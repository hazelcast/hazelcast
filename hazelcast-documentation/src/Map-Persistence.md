

### Map Persistence

Hazelcast allows you to load and store the distributed map entries from/to a persistent data store such as a relational database.  

Hazelcast supports read-through, write-through and write-behind modes which are explained in below subsections.


***ATTENTION:*** *DataStore needs to be a centralized system that is
accessible from all Hazelcast Nodes. Persisting to local file system is not supported.*

#### Read-Through

If a loader implementation is provided and the requested key does not exist in-memory, `get(key)` calls your loader implementation to load the entry from the data store.

#### Write-Through

Map Store can be configured as write-through by setting the `write-delay-seconds` property to **0**. This means the entries will be put to the data store synchronously.

In this mode, when the `map.put(key,value)` call returns, you can be sure that

-   `MapStore.store(key,value)` is successfully called so the entry is persisted.

-   In-Memory entry is updated

-   In-Memory backup copies are successfully created on other JVMs (if `backup-count` is greater than 0)

Same behavior goes for the `map.remove(key)`, only difference is that  `MapStore.delete(key)` is called when it will be deleted.

If `MapStore` throws an exception, then the exception will be propagated back to the original `put` or `remove` call in the form of `RuntimeException`. 

#### Write-Behind

Map Store can be configured as write-behind by setting the `write-delay-seconds` property to a value bigger than **0**. This means the modified entries will be put to the data store asynchronously after a configured delay. 

***NOTE:*** *In write-behind mode, Hazelcast coalesces updates on a specific key, i.e. applies only the last update on it.* 

In this mode, when the `map.put(key,value)` call returns, you can be sure that

-   In-Memory entry is updated

-   In-Memory backup copies are successfully created on other JVMs (if `backup-count` is greater than 0)

-   The entry is marked as dirty so that after `write-delay-seconds`, it can be persisted with `MapStore.store(key,value)` call.

Same behavior goes for the `map.remove(key)`, only difference is that  `MapStore.delete(key)` is called when it will be deleted.

If `MapStore` throws an exception, then Hazelcast retries to store the entry. If it still cannot be stored, a log message is printed and the entry is re-queued. 

For batch write operations, which are only allowed in write-behind mode, Hazelcast will call `MapStore.storeAll(map)`, and `MapStore.deleteAll(collection)` to do all writes in a single call.
<br></br>

***ATTENTION:*** *If a map entry is marked as dirty, i.e. it is waiting to be persisted to the `MapStore` in a write-behind scenario, the eviction process forces the entry to be stored. By this way, you will have control on the number of entries waiting to be stored, so that a possible OutOfMemory exception can be prevented.*
<br></br>


***ATTENTION:*** *MapStore or MapLoader implementations should not use Hazelcast Map/Queue/MultiMap/List/Set operations. Your implementation should only work with your data store. Otherwise, you may get into deadlock situations.*

Here is a sample configuration:

```xml
<hazelcast>
  ...
  <map name="default">
    ...
    <map-store enabled="true">
      <!--
        Name of the class implementing MapLoader and/or MapStore.
        The class should implement at least of these interfaces and
        contain no-argument constructor. Note that the inner classes are not supported.
      -->
      <class-name>com.hazelcast.examples.DummyStore</class-name>
      <!--
        Number of seconds to delay to call the MapStore.store(key, value).
        If the value is zero then it is write-through so MapStore.store(key, value)
        will be called as soon as the entry is updated.
        Otherwise it is write-behind so updates will be stored after write-delay-seconds
        value by calling Hazelcast.storeAll(map). Default value is 0.
      -->
      <write-delay-seconds>60</write-delay-seconds>
      <!--
        Size of the entries for a batch write operation. Default value is 1.
      -->
      <write-batch-size>1000</write-batch-size>
    </map-store>
  </map>
</hazelcast>
```

#### MapStoreFactory and MapLoaderLifecycleSupport Interfaces

As you know, a configuration can be applied to more than one map using wildcards (Please see [Using Wildcard](#using-wildcard)), meaning the configuration is shared among the maps. But, `MapStore` does not know which entries to be stored when there is one configuration applied to multiple maps. To overcome this, Hazelcast provides `MapStoreFactory` interface.

Using this factory, `MapStore`s for each map can be created, when a wildcard configuration is used. A sample code is given below.

```java
Config config = new Config();
MapConfig mapConfig = config.getMapConfig( "*" );
MapStoreConfig mapStoreConfig = mapConfig.getMapStoreConfig();
mapStoreConfig.setFactoryImplementation( new MapStoreFactory<Object, Object>() {
  @Override
  public MapLoader<Object, Object> newMapStore( String mapName, Properties properties ) {
    return null;
  }
});
```

Moreover, if the configuration implements `MapLoaderLifecycleSupport` interface, then the user will have the control to initialize the `MapLoader` implementation with the given map name, configuration properties and the Hazelcast instance. See the below code portion.

```java
public interface MapLoaderLifecycleSupport {

  /**
   * Initializes this MapLoader implementation. Hazelcast will call
   * this method when the map is first used on the
   * HazelcastInstance. Implementation can
   * initialize required resources for the implementing
   * mapLoader such as reading a config file and/or creating
   * database connection.
   */
  void init( HazelcastInstance hazelcastInstance, Properties properties, String mapName );

  /**
   * Hazelcast will call this method before shutting down.
   * This method can be overridden to cleanup the resources
   * held by this map loader implementation, such as closing the
   * database connections etc.
   */
  void destroy();
}
```


#### Initialization on startup

`MapLoader.loadAllKeys` API is used for pre-populating the in-memory map when the map is first touched/used. If `MapLoader.loadAllKeys` returns NULL then nothing will be loaded. Your `MapLoader.loadAllKeys` implementation can return all or some of the keys. You may select and return only the `hot` keys, for instance. Also note that this is the fastest way of pre-populating the map as Hazelcast will optimize the loading process by having each node loading owned portion of the entries.

Moreover, there is InitialLoadMode configuration parameter in the class [`MapStoreConfig`](https://github.com/hazelcast/hazelcast/blob/5f4f6a876e572f91431ad22f01ad5af9f5837f72/hazelcast/src/main/java/com/hazelcast/config/MapStoreConfig.java) class. This parameter has two values: LAZY and EAGER. If InitialLoadMode is set as LAZY, data is not loaded during the map creation. If it is set as EAGER, whole data is loaded while the map is being created and everything becomes ready to use. Also, if you add indices to your map by [`MapIndexConfig`](https://github.com/hazelcast/hazelcast/blob/da5cceee74e471e33f65f43f31d891c9741e31e3/hazelcast/src/main/java/com/hazelcast/config/MapIndexConfig.java) class or [`addIndex`](#indexing) method, then InitialLoadMode is overridden and MapStoreConfig behaves as if EAGER mode is on.

Here is MapLoader initialization flow:

1. When `getMap()` is first called from any node, initialization will start depending on the the value of InitialLoadMode. If it is set as EAGER, initialization starts.  If it is set as LAZY, initialization actually does not start but data is loaded at each time a partition loading is completed.
2. Hazelcast will call `MapLoader.loadAllKeys()` to get all your keys on each node
3. Each node will figure out the list of keys it owns
4. Each node will load all its owned keys by calling `MapLoader.loadAll(keys)`
5. Each node puts its owned entries into the map by calling `IMap.putTransient(key,value)`

***ATTENTION:*** *If the load mode is LAZY and when `clear()` method is called (which triggers `MapStore.deleteAll()`), Hazelcast will remove **ONLY** the loaded entries from your map and datastore. Since the whole data is not loaded for this case (LAZY mode), please note that there may be still entries in your datastore.*

#### Forcing All Keys To Be Loaded

The method `loadAll` is developed to load some or all keys into a data store in order to optimize the multiple load operations. The method has two signatures (i.e. same method can take two different parameter lists). One loads the given keys and the other loads all keys. Please see the sample code below.


```java
public class LoadAll {

    public static void main(String[] args) {
        final int numberOfEntriesToAdd = 1000;
        final String mapName = LoadAll.class.getCanonicalName();
        final Config config = createNewConfig(mapName);
        final HazelcastInstance node = Hazelcast.newHazelcastInstance(config);
        final IMap<Integer, Integer> map = node.getMap(mapName);
       
        populateMap(map, numberOfEntriesToAdd);
        System.out.printf("# Map store has %d elements\n", numberOfEntriesToAdd);
   
        map.evictAll();
        System.out.printf("# After evictAll map size\t: %d\n", map.size());
        
        map.loadAll(true);
        System.out.printf("# After loadAll map size\t: %d\n", map.size());
    }
}
```


#### Post Processing Map Store

In some scenarios, you may need to modify the object after storing it into the map store.
For example, you can get ID or version auto generated by your database and you need to modify your object stored in distributed map, not to break the sync between database and data grid. You can do that by implementing `PostProcessingMapStore` interface;
so the modified object will be put to the distributed map. That will cause an extra step of `Serialization`, so use it just when needed (This explanation is only valid when using `write-through` map store configuration).

Here is an example of post processing map store:

```java
class ProcessingStore extends MapStore<Integer, Employee> implements PostProcessingMapStore {
  @Override
  public void store( Integer key, Employee employee ) {
    EmployeeId id = saveEmployee();
    employee.setId( id.getId() );
  }
}
```
