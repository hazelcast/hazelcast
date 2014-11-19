
### List Configuration

**Declarative:**

```xml
<list name="default">
   <backup-count>1</backup-count>
   <async-backup-count>0</async-backup-count>
   <max-size>10</max-size>
   <statistics-enabled>true</statistics-enabled>
   <item-listeners>
      <item-listener>???<item-listener>
   </item-listeners>
</list>
```

**Programmatic:**

```java
Config config = new Config();
CollectionConfig collectionList = config.getCollectionConfig();
collectionList.setName( "MyList" ).setBackupCount( "1" )
        .setMaxSize( "10" ).setStatisticsEnabled( "true" );
```
   

It has below parameters.


- `backup-count`: Count of synchronous backups. Remember that, List is a non-partitioned data structure, i.e. all entries of a List resides in one partition. When this parameter is '1', it means there will be a backup of that List in another node in the cluster. When it is '2', 2 nodes will have the backup.
- `async-backup-count`: Count of asynchronous backups.
- `statistics-enabled`: If set as `true`, you can retrieve statistics for this List.
- `max-size`: It is the maximum entry size for this List.
- `item-listeners`: ???




