
### Factory and FactoryBuilder

The `javax.cache.configuration.Factory` implementations are used to configure features like
`CacheEntryListener`, `ExpirePolicy` and `CacheLoader`s or `CacheWriter`s. These factory implementations are required to distribute the
different features to nodes in a cluster environment like Hazelcast. Therefore, these factory implementations have to be serializable.

`Factory` implementations are easy to do: they follow the default Provider- or Factory-Pattern. The sample class
`UserCacheEntryListenerFactory` shown below implements a custom JCache `Factory`.

```java
public class UserCacheEntryListenerFactory
    implements Factory<CacheEntryListener<Integer, User>> {

  @Override
  public CacheEntryListener<Integer, User> create() {
    // Just create a new listener instance
    return new UserCacheEntryListener();
  }
}
```

To simplify the process for the users, JCache API offers a set of helper methods collected in
`javax.cache.configuration.FactoryBuilder`. In the above configuration example, `FactoryBuilder::factoryOf` is used to create a
singleton factory for the given instance.

