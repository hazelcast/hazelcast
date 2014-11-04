


### Interceptors

You can add intercept operations and then execute your own business logic synchronously blocking the operations. You can change the returned value from a get operation, change the value to be put, or cancel operations by throwing an exception.

Interceptors are different from listeners. With listeners, you take an action after the operation has been completed. Interceptor actions are synchronous and you can alter the behavior of operation, change the values, or totally cancel it.

MapInterceptors are chained, so adding the same interceptor multiple times to the same map can result in duplicate effects. This can easily happen when the interceptor is added to the map on node initialization, so that each node adds the same interceptor. When adding the interceptor in this way, be sure that the `hashCode()` method is implemented to return the same value for every instance of the interceptor. It is not striclty necessary, but it is a good idea to also implement `equals()` as this will ensure that the MapInterceptor can be removed reliably. 

IMap API has two methods for adding and removing interceptor to the map.

```java
/**
 * Adds an interceptor for this map. Added interceptor will intercept operations
 * and execute user defined methods and will cancel operations if user defined method throw exception.
 * 
 *
 * @param interceptor map interceptor
 * @return id of registered interceptor
 */
String addInterceptor( MapInterceptor interceptor );

/**
 * Removes the given interceptor for this map. So it will not intercept operations anymore.
 * 
 *
 * @param id registration id of map interceptor
 */
void removeInterceptor( String id );
```

Here is the `MapInterceptor` interface:

```java
public interface MapInterceptor extends Serializable {

  /**
   * Intercept the get operation before it returns a value.
   * Return another object to change the return value of get(..)
   * Returning null will cause the get(..) operation to return the original value,
   * namely return null if you do not want to change anything.
   * 
   *
   * @param value the original value to be returned as the result of get(..) operation
   * @return the new value that will be returned by get(..) operation
   */
  Object interceptGet( Object value );

  /**
   * Called after get(..) operation is completed.
   * 
   *
   * @param value the value returned as the result of get(..) operation
   */
  void afterGet( Object value );

  /**
   * Intercept put operation before modifying map data.
   * Return the object to be put into the map.
   * Returning null will cause the put(..) operation to operate as expected,
   * namely no interception. Throwing an exception will cancel the put operation.
   * 
   *
   * @param oldValue the value currently in map
   * @param newValue the new value to be put
   * @return new value after intercept operation
   */
  Object interceptPut( Object oldValue, Object newValue );

  /**
   * Called after put(..) operation is completed.
   * 
   *
   * @param value the value returned as the result of put(..) operation
   */
  void afterPut( Object value );

  /**
   * Intercept remove operation before removing the data.
   * Return the object to be returned as the result of remove operation.
   * Throwing an exception will cancel the remove operation.
   * 
   *
   * @param removedValue the existing value to be removed
   * @return the value to be returned as the result of remove operation
   */
  Object interceptRemove( Object removedValue );

  /**
   * Called after remove(..) operation is completed.
   * 
   *
   * @param value the value returned as the result of remove(..) operation
   */
  void afterRemove( Object value );
}
```

**Example Usage:**

```java
public class InterceptorTest {

  @Test
  public void testMapInterceptor() throws InterruptedException {
    HazelcastInstance hazelcastInstance1 = Hazelcast.newHazelcastInstance();
    HazelcastInstance hazelcastInstance2 = Hazelcast.newHazelcastInstance();
    IMap<Object, Object> map = hazelcastInstance1.getMap( "testMapInterceptor" );
    SimpleInterceptor interceptor = new SimpleInterceptor();
    map.addInterceptor( interceptor );
    map.put( 1, "New York" );
    map.put( 2, "Istanbul" );
    map.put( 3, "Tokyo" );
    map.put( 4, "London" );
    map.put( 5, "Paris" );
    map.put( 6, "Cairo" );
    map.put( 7, "Hong Kong" );

    try {
      map.remove( 1 );
    } catch ( Exception ignore ) {
    }
    try {
      map.remove( 2 );
    } catch ( Exception ignore ) {
    }

    assertEquals( map.size(), 6) ;

    assertEquals( map.get( 1 ), null );
    assertEquals( map.get( 2 ), "ISTANBUL:" );
    assertEquals( map.get( 3 ), "TOKYO:" );
    assertEquals( map.get( 4 ), "LONDON:" );
    assertEquals( map.get( 5 ), "PARIS:" );
    assertEquals( map.get( 6 ), "CAIRO:" );
    assertEquals( map.get( 7 ), "HONG KONG:" );

    map.removeInterceptor( interceptor );
    map.put( 8, "Moscow" );

    assertEquals( map.get( 8 ), "Moscow" );
    assertEquals( map.get( 1 ), null );
    assertEquals( map.get( 2 ), "ISTANBUL" );
    assertEquals( map.get( 3 ), "TOKYO" );
    assertEquals( map.get( 4 ), "LONDON" );
    assertEquals( map.get( 5 ), "PARIS" );
    assertEquals( map.get( 6 ), "CAIRO" );
    assertEquals( map.get( 7 ), "HONG KONG" );
  }

  static class SimpleInterceptor implements MapInterceptor, Serializable {

    @Override
    public Object interceptGet( Object value ) {
      if (value == null)
        return null;
      return value + ":";
    }

    @Override
    public void afterGet( Object value ) {
    }

    @Override
    public Object interceptPut( Object oldValue, Object newValue ) {
      return newValue.toString().toUpperCase();
    }

    @Override
    public void afterPut( Object value ) {
    }

    @Override
    public Object interceptRemove( Object removedValue ) {
      if(removedValue.equals( "ISTANBUL" ))
        throw new RuntimeException( "you can not remove this" );
      return removedValue;
    }

    @Override
    public void afterRemove( Object value ) {
      // do something
    }
  }
}
```


