## Continuous Query Cache

![](images/enterprise-onlycopy.jpg)

<br></br>

![image](images/NoteSmall.jpg) ***NOTE:*** *This feature is supported for Hazelcast Enterprise 3.5 or higher.*

Continuous query cache is used to cache the result of a continuous query. After construction of a continuous query cache, all changes on the underlying `IMap` is immediately reflected to this cache as a stream of events.
Therefore, this cache will be an always up-to-date view of the `IMap`. 
  
Continuous query cache is beneficial when you need to query the distributed `IMap` data in a very frequent and fast way. By using continuous query cache, the result of the query will always be ready and local to the application.
     
You can access this continuous query cache from the server and client side respectively, as shown below.
     
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

1. You can enable/disable the initial query run on the existing `IMap` data during construction of the continuous query cache, according to the supplied predicate via `QueryCacheConfig#setPopulate`.
2. Continuous query cache is indexable and queryable.
3. Continuous query cache is evictable. Note that continuous query cache has a default maximum capacity of 10000. If you need a non-evictable cache, you should configure the eviction via `QueryCacheConfig#setEvictionConfig`.
4. Continuous query cache is listenable via `QueryCache#addEntryListener`.
5. Events on `IMap` are guaranteed to be reflected in this cache in order in which the events happen. Note that this happening order is a partition order, so you can only expect ordered events from the same partition. You can listen to loss of events via `EventLostListener` and events can be recoverable with the `QueryCache#tryRecover` method. If your buffer size on the node side is big enough, you can recover from a possible event loss scenario. 
Currently, setting the size of `QueryCacheConfig#setBufferSize` is the only option for recovery because the events which feed continuous query cache have no backups.
You can use the example below for a recovery case. 

    ```java
       
       QueryCache queryCache = map.getQueryCache("cache-name", new SqlPredicate("this > 20"), true);
       queryCache.addEntryListener(new EventLostListener() {
           @Override
           public void eventLost(EventLostEvent event) {
               queryCache.tryRecover();
           }
       }, false);
    ```
   
6. You can do event batching and coalescing on continuous query cache.
7. You can configure continuous query cache declaratively and programmatically.
8. You can populate continuous query cache with only the keys of entries and you can retrieve subsequent values directly via `QueryCache#get` from the underlying `IMap`. This will help to decrease the initial population time if the values are very large. 
<br></br>





