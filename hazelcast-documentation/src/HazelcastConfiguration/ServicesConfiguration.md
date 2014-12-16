

## Services Configuration

This configuration is used for Hazelcast Service Provider Interface (SPI). The following are the example configurations.


**Declarative:**

```xml
<services enable-defaults="true">
   <service enabled="true">
      <name>MyService</name>
      <class-name>MyServiceClass</class-name>
      <properties>
         <property>
            <property name="com.property.foo">value</property>
      </properties>
      <configuration>
      abcConfig
      </configuration>
   </service>
</services>
```

**Programmatic:**

```java
Config config = new Config();
ServicesConfig servicesConfig = config.getServicesConfig();

servicesConfig.setEnableDefaults( true );

ServiceConfig svcConfig = servicesConfig.getServiceConfig();
svcConfig.setEnabled( true ).setName( "MyService" )
         .setClassName( "MyServiceClass" );
         
svcConfig.setProperty( "com.property.foo", "value" );
```
   

It has below elements.

- `name`: Name of the service to be registered.
- `class-name`: Name of the class that you develop for your service.
- `properties`: This element includes the custom properties that you can add to your service. You enable/disable
 these properties and set their values using this element.
- `configuration`: You can include configuration items which you develop using the `Config` object in your code.



