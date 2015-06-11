## Management Center Configuration

Management Center configuration is used to enable/disable Hazelcast Management Center and specify a time frequency for which the tool is updated with the cluster information. 

The example configurations are shown below.

**Declarative:**

```xml
<management-center enabled="true" update-interval="3">http://localhost:8080/
mancenter</management-center>
```

**Programmatic:**

```java
Config config = new Config();
config.getManagementCenterConfig().setEnabled( true )
         .setUrl( "http://localhost:8080/mancenter" )
            .setUpdateInterval( 3 );
```

Management Center configuration has the following attributes.

- `enabled`: Set to `true` to enable Management Center.
- `url`: The URL where Management Center will work.
- `updateInterval`: The time frequency (in seconds) for which Management Center will take information from Hazelcast cluster.
