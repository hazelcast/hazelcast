## Continuous Query Cache

![](images/enterprise-onlycopy.jpg)

<br></br>

![image](images/NoteSmall.jpg) ***NOTE:*** *This feature is supported for Hazelcast Enterprise 3.5 or higher.*

This feature is used to cache the result of a continuous query. After construction of continuous-query-cache all changes on underlying `IMap` is immediately reflected to this cache as a stream of events.
So this cache will be an always up to date view of the `IMap`. 
  
This feature is beneficial when one needs to query the distributed `IMap` data most frequently and very fast. By using continuous query cache, the result of the query will be always ready and local to the application.
     
You can access this continuous query cache from server and client side respectively like these :
     
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

***Features***

1. Enable/disable initial query run on the existing `IMap` data during construction of continuous query cache according to the supplied predicate via `QueryCacheConfig#setPopulate` 
2. Indexable and queryable.
3. Evictable. Note that continuous-query-cache has a default max capacity of 10000. If you need a not evictable one, you should configure eviction via `QueryCacheConfig#setEvictionConfig`.
4. Listenable via `QueryCache#addEntryListener`
5. Events on `IMap` are guaranteed to be reflected this cache in the happening order. Any loss of event can be listenable via `EventLostListener` and it can 
   be recoverable with `QueryCache#tryRecover` method. If your buffer size on node side is big enough, you can recover from a possible event loss scenario. 
   At the moment, setting the size of `QueryCacheConfig#setBufferSize` is the only option for recovery because the events which feed continuous query cache have no backups.
   Below snippet can be used for recovery case. 

    ```java
       
       QueryCache queryCache = map.getQueryCache("cache-name", new SqlPredicate("this > 20"), true);
       queryCache.addEntryListener(new EventLostListener() {
           @Override
           public void eventLost(EventLostEvent event) {
               queryCache.tryRecover();
           }
       }, false);
    ```
   
6. Event batching and coalescing.
7. Declarative and programmatic configuration
8. It can be populated with only keys of entries and later values can be retrieved directly via `QueryCache#get` from underlying `IMap`. This will help
   to decrease initial population time if the values are very big in size. 
<br></br>




