## Continuous Query Cache

![](images/enterprise-onlycopy.jpg)

<br></br>

![image](images/NoteSmall.jpg) ***NOTE:*** *This feature is supported for Hazelcast Enterprise 3.5 or higher.*

This feature is used to cache the result of a continuous query. After construction of a continuous query cache, all changes on underlying `IMap` is immediately reflected to this cache as a stream of events.
Therefore, this cache will be an always up to date view of the `IMap`. 
  
This feature is beneficial when you need to query the distributed `IMap` data in a very frequent and fast way. By using continuous query cache, the result of the query will be always ready and local to the application.
     
You can access this continuous query cache from the server and client side respectively as shown below.
     
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

1. Enable/disable initial query run on the existing `IMap` data during construction of continuous query cache according to the supplied predicate via `QueryCacheConfig#setPopulate`.
2. Indexable and queryable.
3. Evictable. Note that continuous query cache has a default maximum capacity of 10000. If you need a not evictable one, you should configure the eviction via `QueryCacheConfig#setEvictionConfig`.
4. Listenable via `QueryCache#addEntryListener`.
5. Events on `IMap` are guaranteed to be reflected to this cache in the happening order. Any loss of event can be listened via `EventLostListener` and it can be recoverable with `QueryCache#tryRecover` method. If your buffer size on the node side is big enough, you can recover from a possible event loss scenario. 
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
8. It can be populated with only keys of entries and subsequent values can be retrieved directly via `QueryCache#get` from the underlying `IMap`. This will help
   to decrease initial population time if the values are very big in size. 
<br></br>




