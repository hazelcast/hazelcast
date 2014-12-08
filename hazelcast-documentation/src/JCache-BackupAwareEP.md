
### BackupAwareEntryProcessor

Another feature, especially interesting for distributed environments like Hazelcast, is the JCache specified
`javax.cache.processor.EntryProcessor`. For more general information, please see the [JCache EntryProcessor section](#jcache-entryprocessor).

Since Hazelcast provides backups of cached entries on other nodes, the default way to backup an object changed by an
`EntryProcessor` is to serialize the complete object and send it to the backup partition. This can be a huge network overhead for big objects.

Hazelcast offers a sub-interface for `EntryProcessor` called `com.hazelcast.cache.BackupAwareEntryProcessor`. This allows the user to create or pass another `EntryProcessor` to run on backup
partitions and apply delta changes to the backup entries.

The backup partition `EntryProcessor` can either be the currently running processor (by returning `this`) or it can be
a specialized `EntryProcessor` implementation (other from the currently running one) which does different operations or leaves
out operations, e.g. sending emails.

If we again take the `EntryProcessor` example from the demonstration application provided in the [JCache EntryProcessor sectiob](#jcache-entryprocessor),
the changed code will look like the following snippet.

```java
public class UserUpdateEntryProcessor
    implements BackupAwareEntryProcessor<Integer, User, User> {

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

  public EntryProcessor<K, V, T> createBackupEntryProcessor() {
    return this;
  }
}
```

You can use the additional method `BackupAwareEntryProcessor::createBackupEntryProcessor` to create or return the `EntryProcessor`
implementation to run on the backup partition (in the example above, the same processor again).

<br></br>
![image](images/NoteSmall.jpg) ***NOTE:*** *For the backup runs, the returned value from the backup processor is ignored and not
returned to the user.*
<br></br>

