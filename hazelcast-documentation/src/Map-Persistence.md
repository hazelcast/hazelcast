

### Map Persistence

Hazelcast allows you to load and store the distributed map entries from/to a persistent data store such as a relational database. To do this, you can use Hazelcast's `MapStore` and `MapLoader` interfaces.

When you provide a `MapLoader` implementation and request an entry (`IMap.get()`) that does not exist in memory, `MapLoader`'s `load` or `loadAll` methods will load that entry from the data store. This loaded entry is placed into the map and will stay there until it is removed or evicted.

When a `MapStore` implementation is provided, an entry is also put into a user defined data store. 

![image](images/NoteSmall.jpg) ***NOTE:*** *Data store needs to be a centralized system that is
accessible from all Hazelcast Nodes. Persistence to local file system is not supported.*

Following is a `MapStore` example.

```java
public class PersonMapStore implements MapStore<Long, Person> {
    private final Connection con;

    public PersonMapStore() {
        try {
            con = DriverManager.getConnection("jdbc:hsqldb:mydatabase", "SA", "");
            con.createStatement().executeUpdate(
                    "create table if not exists person (id bigint, name varchar(45))");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized void delete(Long key) {
        System.out.println("Delete:" + key);
        try {
            con.createStatement().executeUpdate(
                    format("delete from person where id = %s", key));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized void store(Long key, Person value) {
        try {
            con.createStatement().executeUpdate(
                    format("insert into person values(%s,'%s')", key, value.name));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized void storeAll(Map<Long, Person> map) {
        for (Map.Entry<Long, Person> entry : map.entrySet())
            store(entry.getKey(), entry.getValue());
    }

    public synchronized void deleteAll(Collection<Long> keys) {
        for (Long key : keys) delete(key);
    }

    public synchronized Person load(Long key) {
        try {
            ResultSet resultSet = con.createStatement().executeQuery(
                    format("select name from person where id =%s", key));
            try {
                if (!resultSet.next()) return null;
                String name = resultSet.getString(1);
                return new Person(name);
            } finally {
                resultSet.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized Map<Long, Person> loadAll(Collection<Long> keys) {
        Map<Long, Person> result = new HashMap<Long, Person>();
        for (Long key : keys) result.put(key, load(key));
        return result;
    }

    public Set<Long> loadAllKeys() {
        return null;
    }
}
```

![image](images/NoteSmall.jpg) ***NOTE:*** *Loading process is performed on a thread different than the partition threads using ExecutorService.*

<br></br>
***RELATED INFORMATION***

*For more MapStore/MapLoader code samples please see [here](https://github.com/hazelcast/hazelcast-code-samples/tree/master/distributed-map/mapstore/src/main/java).*
<br></br>

Hazelcast supports read-through, write-through, and write-behind persistence modes which are explained in below subsections.

#### Read-Through

If an entry does not exist in the memory when an application asks for it, Hazelcast asks your loader implementation to load that entry from the data store.  If the entry exists there, the loader implementation gets it, hands it to Hazelcast, and Hazelcast puts it into the memory. This is read-through persistence mode.



#### Write-Through

`MapStore` can be configured to be write-through by setting the `write-delay-seconds` property to **0**. This means the entries will be put to the data store synchronously.

In this mode, when the `map.put(key,value)` call returns:

-   `MapStore.store(key,value)` is successfully called so the entry is persisted.

-   In-Memory entry is updated.

-   In-Memory backup copies are successfully created on other JVMs (if `backup-count` is greater than 0).

The same behavior goes for a `map.remove(key)` call. The only difference is that  `MapStore.delete(key)` is called when the entry will be deleted.

If `MapStore` throws an exception, then the exception will be propagated back to the original `put` or `remove` call in the form of `RuntimeException`. 

#### Write-Behind

You can configure `MapStore` as write-behind by setting the `write-delay-seconds` property to a value bigger than **0**. This means the modified entries will be put to the data store asynchronously after a configured delay. 

![image](images/NoteSmall.jpg) ***NOTE:*** *In write-behind mode, by default Hazelcast coalesces updates on a specific key, i.e. applies only the last update on it. But you can also set `MapStoreConfig#setWriteCoalescing` to `FALSE` and you can store all updates on a key to the store.*

![image](images/NoteSmall.jpg) ***NOTE:*** *When you set `MapStoreConfig#setWriteCoalescing` to `FALSE`, after you reached per-node max write-behind-queue capacity, subsequent put operations will fail with `ReachedMaxSizeException`. This exception will be thrown to prevent uncontrolled grow of write-behind queues. You can set per node max capacity with `GroupProperty#MAP_WRITE_BEHIND_QUEUE_CAPACITY` *


In this mode, when the `map.put(key,value)` call returns:

-   In-Memory entry is updated.

-   In-Memory backup copies are successfully created on other JVMs (if `backup-count` is greater than 0).

-   The entry is marked as dirty so that after `write-delay-seconds`, it can be persisted with `MapStore.store(key,value)` call.

The same behavior goes for the `map.remove(key)`, the only difference is that  `MapStore.delete(key)` is called when the entry will be deleted.

If `MapStore` throws an exception, then Hazelcast tries to store the entry again. If the entry still cannot be stored, a log message is printed and the entry is re-queued. 

For batch write operations, which are only allowed in write-behind mode, Hazelcast will call `MapStore.storeAll(map)` and `MapStore.deleteAll(collection)` to do all writes in a single call.
<br></br>

![image](images/NoteSmall.jpg) ***NOTE:*** *If a map entry is marked as dirty, i.e. it is waiting to be persisted to the `MapStore` in a write-behind scenario, the eviction process forces the entry to be stored. By this way, you will have control on the number of entries waiting to be stored, and thus you can prevent a possible OutOfMemory exception.*
<br></br>


![image](images/NoteSmall.jpg) ***NOTE:*** *`MapStore` or `MapLoader` implementations should not use Hazelcast Map/Queue/MultiMap/List/Set operations. Your implementation should only work with your data store. Otherwise, you may get into deadlock situations.*

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
        Used to create batch chunks when writing map store.
        In default mode all entries will be tried to persist in one go.
        To create batch chunks, minimum meaningful value for write-batch-size
        is 2. For values smaller than 2, it works as in default mode.
      -->
      <write-batch-size>1000</write-batch-size>
    </map-store>
  </map>
</hazelcast>
```

#### MapStoreFactory And MapLoaderLifecycleSupport Interfaces

A configuration can be applied to more than one map using wildcards (see [Using Wildcard](#using-wildcard)), meaning that the configuration is shared among the maps. But `MapStore` does not know which entries to store when there is one configuration applied to multiple maps. To overcome this, Hazelcast provides the `MapStoreFactory` interface.

Using the `MapStoreFactory` interface, `MapStore`s for each map can be created when a wildcard configuration is used. Sample code is shown below.

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

If the configuration implements the `MapLoaderLifecycleSupport` interface, then the user can initialize the `MapLoader` implementation with the given map name, configuration properties, and the Hazelcast instance. See the following example code.

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


#### Initialization On Startup

You can use the `MapLoader.loadAllKeys` API to pre-populate the in-memory map when the map is first touched/used. If `MapLoader.loadAllKeys` returns NULL then nothing will be loaded. Your `MapLoader.loadAllKeys` implementation can return all or some of the keys. For example, you may select and return only the `hot` keys. `MapLoader.loadAllKeys` is the fastest way of pre-populating the map since Hazelcast will optimize the loading process by having each node loading its owned portion of the entries.

The `InitialLoadMode` configuration parameter in the class [`MapStoreConfig`](https://github.com/hazelcast/hazelcast/blob/5f4f6a876e572f91431ad22f01ad5af9f5837f72/hazelcast/src/main/java/com/hazelcast/config/MapStoreConfig.java) has two values: `LAZY` and `EAGER`. If `InitialLoadMode` is set to `LAZY`, data is not loaded during the map creation. If it is set to `EAGER`, the whole data is loaded while the map is created and everything becomes ready to use. Also, if you add indices to your map with the [`MapIndexConfig`](https://github.com/hazelcast/hazelcast/blob/da5cceee74e471e33f65f43f31d891c9741e31e3/hazelcast/src/main/java/com/hazelcast/config/MapIndexConfig.java) class or the [`addIndex`](#indexing) method, then `InitialLoadMode` is overridden and `MapStoreConfig` behaves as if `EAGER` mode is on.

Here is the `MapLoader` initialization flow:

1. When `getMap()` is first called from any node, initialization will start depending on the the value of `InitialLoadMode`. If it is set to `EAGER`, initialization starts.  If it is set to `LAZY`, initialization does not start but data is loaded each time a partition loading completes.
2. Hazelcast will call `MapLoader.loadAllKeys()` to get all your keys on each node.
3. Each node will figure out the list of keys it owns.
4. Each node will load all its owned keys by calling `MapLoader.loadAll(keys)`.
5. Each node puts its owned entries into the map by calling `IMap.putTransient(key,value)`.

![image](images/NoteSmall.jpg) ***NOTE:*** *If the load mode is `LAZY` and when the `clear()` method is called (which triggers `MapStore.deleteAll()`), Hazelcast will remove **ONLY** the loaded entries from your map and datastore. Since the whole data is not loaded for this case (`LAZY` mode), please note that there may be still entries in your datastore.*

#### Forcing All Keys To Be Loaded

The method `loadAll` loads some or all keys into a data store in order to optimize the multiple load operations. The method has two signatures (i.e. the same method can take two different parameter lists). One signature loads the given keys and the other loads all keys. Please see the sample code below.


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
For example, you can get an ID or version auto generated by your database and then you need to modify your object stored in the distributed map but not to break the sync between database and data grid. You can do that by implementing the `PostProcessingMapStore` interface to put the modified object into the distributed map. That will cause an extra step of `Serialization`, so use it only when needed. (This explanation is only valid when using the `write-through` map store configuration.)

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
