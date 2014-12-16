
### CacheWriter

A `javax.cache.integration.CacheWriter` is used to update an external backend resource. If the cache is configured to be
`write-through` this process is executed transparently to the users code otherwise at the current state there is no way to trigger
writing changed entries to the external resource to a user defined point in time.

If bulk operations throw an exception, `java.util.Collection` has to be cleaned of all successfully written keys so
the cache implementation can determine what keys are written and can be applied to the cache state.

```java
public class UserCacheWriter
    implements CacheWriter<Integer, User>, Serializable {

  private final UserDao userDao;

  public UserCacheWriter( UserDao userDao ) {
    // Store the dao instance created externally
    this.userDao = userDao;
  }

  @Override
  public void write( Cache.Entry<? extends Integer, ? extends User> entry )
      throws CacheWriterException {

    // Store the user using the dao
    userDao.storeUser( entry.getKey(), entry.getValue() );
  }

  @Override
  public void writeAll( Collection<Cache.Entry<...>> entries )
      throws CacheWriterException {

    // Retrieve the iterator to clean up the collection from
    // written keys in case of an exception
    Iterator<Cache.Entry<...>> iterator = entries.iterator();
    while ( iterator.hasNext() ) {
      // Write entry using dao
      write( iterator.next() );
      // Remove from collection of keys
      iterator.remove();
    }
  }

  @Override
  public void delete( Object key ) throws CacheWriterException {
    // Test for key type
    if ( !( key instanceof Integer ) ) {
      throw new CacheWriterException( "Illegal key type" );
    }
    // Remove user using dao
    userDao.removeUser( ( Integer ) key );
  }

  @Override
  public void deleteAll( Collection<?> keys ) throws CacheWriterException {
    // Retrieve the iterator to clean up the collection from
    // written keys in case of an exception
    Iterator<?> iterator = keys.iterator();
    while ( iterator.hasNext() ) {
      // Write entry using dao
      delete( iterator.next() );
      // Remove from collection of keys
      iterator.remove();
    }
  }
}
```

Again the implementation is pretty straight forward and also as above all exceptions thrown by the external resource, like
`java.sql.SQLException` has to be wrapped into a `javax.cache.integration.CacheWriterException`. Note this is a different
exception from the one thrown by `CacheLoader`.

