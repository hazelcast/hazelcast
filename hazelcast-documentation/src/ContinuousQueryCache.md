## Continuous Query Cache

![](images/enterprise-onlycopy.jpg)

<br></br>

![image](images/NoteSmall.jpg) ***NOTE:*** *This feature is supported for Hazelcast Enterprise 3.5 or higher.*

A continuous query cache is used to cache the result of a continuous query. After the construction of a continuous query cache, all changes on the underlying `IMap` are immediately reflected to this cache as a stream of events.
Therefore, this cache will be an always up-to-date view of the `IMap`. 
  
A continuous query cache is beneficial when you need to query the distributed `IMap` data in a very frequent and fast way. By using a continuous query cache, the result of the query will always be ready and local to the application.
     
The following code snippet shows how you can access a continuous query cache from the server side.
     
```java

QueryCacheConfig queryCacheConfig = new QueryCacheConfig("cache-name");
queryCacheConfig.getPredicateConfig().setImplementation(new OddKeysPredicate());
       
MapConfig mapConfig = new MapConfig("map-name");
mapConfig.addQueryCacheConfig(queryCacheConfig);
       
Config config = new Config();
config.addMapConfig(mapConfig);
      
HazelcastInstance node = Hazelcast.newHazelcastInstance(config);
IEnterpriseMap<Integer, String> map = (IEnterpriseMap) node.getMap("map-name");
       
QueryCache<Integer, String> cache = map.getQueryCache("cache-name");

```     

The following code snippet shows how you can access a continuous query cache from the client side.

     
```java

QueryCacheConfig queryCacheConfig = new QueryCacheConfig("cache-name");
queryCacheConfig.getPredicateConfig().setImplementation(new OddKeysPredicate());
       
ClientConfig clientConfig = new ClientConfig();
clientConfig.addQueryCacheConfig("map-name", queryCacheConfig);
      
HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
IEnterpriseMap<Integer, Integer> clientMap = (IEnterpriseMap) client.getMap("map-name");
       
QueryCache<Integer, Integer> cache = clientMap.getQueryCache("cache-name");

```

### Features of Continuous Query Cache

* You can enable/disable the initial query that is run on the existing `IMap` data during the continuous query cache construction, according to the supplied predicate via `QueryCacheConfig#setPopulate`.
* You can index and perform queries on a continuous query cache.
* A continuous query cache is evictable. Note that a continuous query cache has a default maximum capacity of 10000. If you need a non-evictable cache, you should configure the eviction via `QueryCacheConfig#setEvictionConfig`.
* You can listen to a continuous query cache using `QueryCache#addEntryListener`.
* The events on the `IMap` are reflected in a continuous query cache, keeping the same order of those events. Note that the order of the events implies the order in a partition. Therefore, you can only expect ordered events from the same partition. You can listen to the event losses using `EventLostListener` and events can be recoverable with the method `QueryCache#tryRecover`. If your buffer size on the node side is large enough, you can recover from a possible event loss scenario. 
Currently, setting the size of `QueryCacheConfig#setBufferSize` is the only option for recovery since the events which feed a continuous query cache have no backups.
You can use the following example code for a recovery case. 

    ```java
       
       QueryCache queryCache = map.getQueryCache("cache-name", new SqlPredicate("this > 20"), true);
       queryCache.addEntryListener(new EventLostListener() {
           @Override
           public void eventLost(EventLostEvent event) {
               queryCache.tryRecover();
           }
       }, false);
    ```
   
* You can perform event batching and coalescing on a continuous query cache.
* You can configure a continuous query cache declaratively or programmatically.
* You can populate a continuous query cache with only the keys of its entries and you can retrieve the subsequent values directly via `QueryCache#get` from the underlying `IMap`. This helps to decrease the initial population time when the values are very large. 
<br></br>





