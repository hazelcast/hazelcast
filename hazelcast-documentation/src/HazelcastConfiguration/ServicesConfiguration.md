

### Services Configuration

This configuration is used for Hazelcast Service Provider Interface (SPI). The following are the example configurations.


**Declarative:**

```xml
<services enable-defaults="true">
   <service enabled="true">
      <name>MyService</name>
      <class-name>MyServiceClass</class-name>
      <properties>
         <property>
            <property-1 enabled="true">100</property-1>
            <property-2>true</property-2>
      </properties>
      <configuration>
      ???
      </configuration>
   </service>
</services>
```

**Programmatic:**

```java
Config config = new Config();
ServicesConfig servicesConfig = config.getServicesConfig();

servicesConfig.

ServiceConfig svcConfig = servicesConfig.getServiceConfig();
svcConfig.setEnabled( true ).setName( "MyService" )
         .setClassName( "MyServiceClass" );
         
svcConfig.

```
   

It has below parameters.

- `name`: Name of the service to be registered.
- `class-name`: Name of the class that you develop for your service.
- `properties`: This element includes the custom properties that you can add to your service. You enable/disable
 these properties and set their values using this element.



