


### Add Properties

The `init` method for `CounterService` takes the `Properties` object as an argument. This means we can add properties to the service that are passed to the method `init`. You can add properties declaratively as shown below.

```xml
<service enabled="true">
   <name>CounterService</name>
   <class-name>CounterService</class-name>
   <properties> 
      <someproperty>10</someproperty>
   </properties>
</service>
```

If you want to parse a more complex XML, you can use the interface `com.hazelcast.spi.ServiceConfigurationParser`. It gives you access to the XML DOM tree.

