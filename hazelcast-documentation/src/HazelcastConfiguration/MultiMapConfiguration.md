
## Multimap Configuration

The following are the example configurations.

**Declarative:**

```xml
<hazelcast>
  <multimap name="default">
    <backup-count>0</backup-count>
    <async-backup-count>1</async-backup-count>
    <value-collection-type>SET</value-collection-type>
    <entry-listeners>
        <entry-listener include-value="false" local="false">
           com.hazelcast.examples.EntryListener
        </entry-listener>
    </entry-listeners>   
  </map>
</hazelcast>
```

**Programmatic:**

```java
MultiMapConfig mmConfig = new MultiMapConfig();
mmConfig.setName( "default" );

mmConfig.setBackupCount( "0" ).setAsyncBackupCount( "1" );
         
mmConfig.setValueCollectionType( "SET" );
```


Most of MultiMap configuration elements and attributes  have the same names and functionalities explained in the [Map Configuration section](#map-configuration). Below are the ones specific to MultiMap.

- `statistics-enabled`: You can retrieve some statistics like owned entry count, backup entry count, last update time, locked entry count by setting this parameter's value as "true". The method for retrieving the statistics is `getLocalMultiMapStats()`.
- `value-collection-type`: Type of the value collection. It can be `Set` or `List`.


