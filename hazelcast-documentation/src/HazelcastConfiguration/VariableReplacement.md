
## Variable Replacement in XML Configuration

Hazelcast and Hazelcast Client replace your xml variables with system property.

#### Example of Hazelcast XML Variable Replacement
`hazelcast-config.xml`:

```xml
<hazelcast>
 <group>
        <name>${group.name}</name>
        <password>${group.password}</password>
    </group>
</hazelcast>
```
If you start up your JVM with “-Dgroup.name=dev -Dgroup.password=somepassword" the properties will automatically be put 
in the System properties and therefore be accessible in the XML configuration.

In some cases you don’t want to rely on system properties
In these cases you can use the XmlConfigBuilder and set a Properties instance explicitly:

 
```java
Properties properties = new Properties();
//fill the properties, e.g. from database/ldap etc
XmlConfigBuilder builder = new XmlConfigBuilder();
builder.setProperties(properties)
Config config = builder.build();
HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
}
```

#### Example of Hazelcast Client XML Variable Replacement

`hazelcast-client-config.xml`:

```xml
<hazelcast-client>
    <group>
        <name>${group.name}</name>
        <password>${group.password}</password>
    </group>
</hazelcast>
```