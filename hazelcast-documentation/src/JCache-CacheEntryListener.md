
### CacheEntryListener

The `javax.cache.event.CacheEntryListener` implementation is straight forward. `CacheEntryListener` is a super-interface which is used as a marker for listener classes in JCache. The specification brings a set of sub-interfaces.

- `CacheEntryCreatedListener`: Fires after a cache entry is added (even on read-through by a `CacheLoader`) to the cache.
- `CacheEntryUpdatedListener`: Fires after an already existing cache entry was updates.
- `CacheEntryRemovedListener`: Fires after a cache entry was removed (not expired) from the cache.
- `CacheEntryExpiredListener`: Fires after a cache entry has been expired. Expiry does not have to be parallel process, it is only required to be executed on the keys that are requested by `Cache::get` and some other operations. For a full table of expiry please see the [https://www.jcp.org/en/jsr/detail?id=107](https://www.jcp.org/en/jsr/detail?id=107) point 6.  

To configure `CacheEntryListener`, add a `javax.cache.configuration.CacheEntryListenerConfiguration` instance to
the JCache configuration class, as seen in the above example configuration. In addition listeners can be configured to be
executed synchronously (blocking the calling thread) or asynchronously (fully running in parallel).

In this example application, the listener is implemented to print event information on the console. That visualizes what is going on in the cache.

```java
public class UserCacheEntryListener
    implements CacheEntryCreatedListener<Integer, User>,
        CacheEntryUpdatedListener<Integer, User>,
        CacheEntryRemovedListener<Integer, User>,
        CacheEntryExpiredListener<Integer, User> {

  @Override
  public void onCreated( Iterable<CacheEntryEvent<...>> cacheEntryEvents )
      throws CacheEntryListenerException {

    printEvents( cacheEntryEvents );
  }

  @Override
  public void onUpdated( Iterable<CacheEntryEvent<...>> cacheEntryEvents )
      throws CacheEntryListenerException {

    printEvents( cacheEntryEvents );
  }

  @Override
  public void onRemoved( Iterable<CacheEntryEvent<...>> cacheEntryEvents )
      throws CacheEntryListenerException {

    printEvents( cacheEntryEvents );
  }

  @Override
  public void onExpired( Iterable<CacheEntryEvent<...>> cacheEntryEvents )
      throws CacheEntryListenerException {

    printEvents( cacheEntryEvents );
  }

  private void printEvents( Iterable<CacheEntryEvent<...>> cacheEntryEvents ) {
    Iterator<CacheEntryEvent<...>> iterator = cacheEntryEvents.iterator();
    while ( iterator.hasNext() ) {
      CacheEntryEvent<...> event = iterator.next();
      System.out.println( event.getEventType() );
    }
  }
}
```

