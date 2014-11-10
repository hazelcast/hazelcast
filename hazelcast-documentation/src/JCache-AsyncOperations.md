
### Async Operations

As another addition of Hazelcast ICache over the normal JCache specification, Hazelcast provides asynchronous versions of almost
all methods, returning a `com.hazelcast.core.ICompletableFuture`. By using these methods and the returned future objects, you can use JCache in a reactive way by registering zero or more callbacks on the future to prevent blocking the current thread.


Name of the asynchronous versions of the methods append the phrase `Async` to the method name. Sample code is shown below using the method `putAsync()`.

```java
ICache<Integer, String> unwrappedCache = cache.unwrap( ICache.class );
ICompletableFuture<String> future = unwrappedCache.putAsync( 1, "value" );
future.andThen( new ExecutionCallback<String>() {
  public void onResponse( String response ) {
    System.out.println( "Previous value: " + response );
  }

  public void onFailure( Throwable t ) {
    t.printStackTrace();
  }
} );
```

Following methods
are available in asynchronous versions:

 - `get(key)`:
  - `getAsync(key)`
  - `getAsync(key, expiryPolicy)`
 - `put(key, value)`:
  - `putAsync(key, value)`
  - `putAsync(key, value, expiryPolicy)`
 - `putIfAbsent(key, value)`:
  - `putIfAbsentAsync(key, value)`
  - `putIfAbsentAsync(key, value, expiryPolicy)`
 - `getAndPut(key, value)`:
  - `getAndPutAsync(key, value)`
  - `getAndPutAsync(key, value, expiryPolicy)`
 - `remove(key)`:
  - `removeAsync(key)`
 - `remove(key, value)`:
  - `removeAsync(key, value)`
 - `getAndRemove(key)`:
  - `getAndRemoveAsync(key)`
 - `replace(key, value)`:
  - `replaceAsync(key, value)`
  - `replaceAsync(key, value, expiryPolicy)`
 - `replace(key, oldValue, newValue)`:
  - `replaceAsync(key, oldValue, newValue)`
  - `replaceAsync(key, oldValue, newValue, expiryPolicy)`
 - `getAndReplace(key, value)`:
  - `getAndReplaceAsync(key, value)`
  - `getAndReplaceAsync(key, value, expiryPolicy)`

The methods with a given `javax.cache.expiry.ExpiryPolicy` are further discussed in the section
[Custom ExpiryPolicy](#custom-expirypolicy).

<br></br>
![image](images/NoteSmall.jpg) ***NOTE:*** *Asynchronous versions of the methods are not compatible with synchronous events.*
<br></br>

