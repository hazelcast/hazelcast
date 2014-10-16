
### Abstract Entry Processor

`AbstractEntryProcessor` class can be used when the same processing will be performed both on primary and backup map entries (i.e. same logic applies to them). If `EntryProcessor` is used, you need to apply the same logic to backup entries separately. `AbstractEntryProcessor` class brings an easiness on this primary/backup processing.

Please see below sample code.

```java
public abstract class AbstractEntryProcessor <K, V>
    implements EntryProcessor <K, V> {
    
  private final EntryBackupProcessor <K,V> entryBackupProcessor;
  public AbstractEntryProcessor() {
    this(true);
  }

  public AbstractEntryProcessor(boolean applyOnBackup) {
    if ( applyOnBackup ) {
      entryBackupProcessor = new EntryBackupProcessorImpl();
    } else {
      entryBackupProcessor = null;
    }
  } 

  @Override
  public abstract Object process(Map.Entry<K, V> entry);

  @Override
  public final EntryBackupProcessor <K, V> getBackupProcessor() {
    return entryBackupProcessor;
  }

  private class EntryBackupProcessorImpl implements EntryBackupProcessor <K,V>{
    @Override
    public void processBackup(Map.Entry<K, V> entry) {
      process(entry); 
    }
  }	
}
```

In the above sample, the method `getBackupProcessor` returns an `EntryBackupProcessor` instance. This means, the same processing will be applied to both primary and backup entries. If you want to apply the processing only on the primary entries, then `getBackupProcessor` method should return null. 

