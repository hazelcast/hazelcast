


### Add Properties

Remember that the `init` method takes `Properties` object as an argument. This means we can add properties to the service. These properties are passed to the method `init`. Adding properties can be done declaratively as shown below.

```xml
<service enabled="true">
   <name>CounterService</name>
   <class-name>CounterService</class-name>
   <properties> 
      <someproperty>10</someproperty>
   </properties>
</service>
```

If you want to parse a more complex XML, the interface `com.hazelcast.spi.ServiceConfigurationParser` can be used. It gives you access to the XML DOM tree.

