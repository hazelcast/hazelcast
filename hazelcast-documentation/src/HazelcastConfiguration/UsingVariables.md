
## Using Variables

In your Hazelcast and/or Hazelcast Client declarative configuration, you can use variables to set the values of the elements. This is valid when you set a system property programmatically or using the command line interface. Then, you can use a variable in the declarative configuration to access to those values of the system properties you set.

For example, see the following command that sets two system properties.

```
-Dgroup.name=dev -Dgroup.password=somepassword
```

Let's reach to the values of these system properties in the declarative configuration of Hazelcast as shown below.

```xml
<hazelcast>
 <group>
        <name>${group.name}</name>
        <password>${group.password}</password>
    </group>
</hazelcast>
```

This also applies to the declarative configuration of Hazelcast Client as shown below.

```xml
<hazelcast-client>
    <group>
        <name>${group.name}</name>
        <password>${group.password}</password>
    </group>
</hazelcast-client>
```

If you do not want to rely on the system properties, you can use the `XmlConfigBuilder` and set a `Properties` instance explicitly as shown below.

 
```java
Properties properties = new Properties();

//fill the properties, e.g. from database/LDAP, etc.

XmlConfigBuilder builder = new XmlConfigBuilder();
builder.setProperties(properties)
Config config = builder.build();
HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
}
```
