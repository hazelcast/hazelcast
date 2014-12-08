
### JCache EntryProcessor

With `javax.cache.processor.EntryProcessor`, you can apply an atomic function to a cache entry. In a distributed
environment like Hazelcast, you can move the mutating function to the node that owns the key. If the value
object is big, it might prevent traffic by sending the object to the mutator and sending it back to the owner to update it.

By default, Hazelcast JCache sends the complete changed value to the backup partition. Again, this can cause a lot of traffic if
the object is big. Another option to prevent this is part of the Hazelcast ICache extension. Further information is available at
[BackupAwareEntryProcessor](#backupawareentryprocessor).

An arbitrary number of arguments can be passed to the `Cache::invoke` and `Cache::invokeAll` methods. All of those arguments need
to be fully serializable because in a distributed environment like Hazelcast, it is very likely that these arguments have to be passed around the cluster.

```java
public class UserUpdateEntryProcessor
    implements EntryProcessor<Integer, User, User> {

  @Override
  public User process( MutableEntry<Integer, User> entry, Object... arguments )
      throws EntryProcessorException {

    // Test arguments length
    if ( arguments.length < 1 ) {
      throw new EntryProcessorException( "One argument needed: username" );
    }

    // Get first argument and test for String type
    Object argument = arguments[0];
    if ( !( argument instanceof String ) ) {
      throw new EntryProcessorException(
          "First argument has wrong type, required java.lang.String" );
    }

    // Retrieve the value from the MutableEntry
    User user = entry.getValue();

    // Retrieve the new username from the first argument
    String newUsername = ( String ) arguments[0];

    // Set the new username
    user.setUsername( newUsername );

    // Set the changed user to mark the entry as dirty
    entry.setValue( user );

    // Return the changed user to return it to the caller
    return user;
  }
}
```

<br></br>
![image](images/NoteSmall.jpg) ***NOTE:*** *By executing the bulk `Cache::invokeAll` operation, atomicity is only guaranteed for a
single cache entry. No transactional rules are applied to the bulk operation.*

![image](images/NoteSmall.jpg) ***NOTE:*** *JCache `EntryProcessor` implementations are not allowed to call
`javax.cache.Cache` methods; this prevents operations from deadlocking between different calls.*
<br></br>

In addition, when using a `Cache::invokeAll` method, a `java.util.Map` is returned that maps the key to its
`javax.cache.processor.EntryProcessorResult`, and which itself wraps the actual result or a thrown
`javax.cache.processor.EntryProcessorException`.

