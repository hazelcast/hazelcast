
### Persistence

Hazelcast allows you to load and store the distributed map entries from/to a persistent datastore such as relational database. If a loader implementation is provided, when `get(key)` is called, if the map entry doesn't exist in-memory then Hazelcast will call your loader implementation to load the entry from a datastore. If a store implementation is provided, when `put(key,value)` is called, Hazelcast will call your store implementation to store the entry into a datastore. Hazelcast can call your implementation to store the entries synchronously (write-through) with no-delay or asynchronously (write-behind) with delay and it is defined by the `write-delay-seconds` value in the configuration.

If it is write-through, when the `map.put(key,value)` call returns, you can be sure that

-   `MapStore.store(key,value)` is successfully called so the entry is persisted.

-   In-Memory entry is updated

-   In-Memory backup copies are successfully created on other JVMs (if backup-count is greater than 0)

If it is write-behind, when the `map.put(key,value)` call returns, you can be sure that

-   In-Memory entry is updated

-   In-Memory backup copies are successfully created on other JVMs (if backup-count is greater than 0)

-   The entry is marked as `dirty` so that after `write-delay-seconds`, it can be persisted.

Same behavior goes for the `remove(key` and `MapStore.delete(key)`. If `MapStore` throws an exception then the exception will be propagated back to the original `put` or `remove` call in the form of `RuntimeException`. When write-through is used, Hazelcast will call `MapStore.store(key,value)` and `MapStore.delete(key)` for each entry update. When write-behind is used, Hazelcast will call`MapStore.store(map)`, and `MapStore.delete(collection)` to do all writes in a single call. Also note that your MapStore or MapLoader implementation should not use Hazelcast Map/Queue/MultiMap/List/Set operations. Your implementation should only work with your data store. Otherwise you may get into deadlock situations.

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
            <write-delay-seconds>0</write-delay-seconds>
        </map-store>
    </map>
</hazelcast>
```
#### Initialization on startup: ####

`MapLoader.loadAllKeys` API is used for pre-populating the in-memory map when the map is first touched/used. If `MapLoader.loadAllKeys` returns NULL then nothing will be loaded. Your `MapLoader.loadAllKeys` implementation can return all or some of the keys. You may select and return only the `hot` keys, for instance. Also note that this is the fastest way of pre-populating the map as Hazelcast will optimize the loading process by having each node loading owned portion of the entries.

Here is MapLoader initialization flow;

1.  When `getMap()` first called from any node, initialization starts

2.  Hazelcast will call `MapLoader.loadAllKeys()` to get all your keys on each node

3.  Each node will figure out the list of keys it owns

4.  Each node will load all its owned keys by calling `MapLoader.loadAll(keys)`

5.  Each node puts its owned entries into the map by calling `IMap.putTransient(key,value)`


