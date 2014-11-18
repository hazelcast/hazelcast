

### Services Configuration

This configuration is used for SPI. 


**Declarative:**

```xml
<services>
   <service enabled="true">
      <name>MyService</name>
      <class-name>MyServiceClass</class-name>
      <properties>
         <custom-property-1 enabled="true">100</custom-property-1>
         <custom-property-2>true</custom-property-2>
      </properties>
   </service>
</services>
```

**Programmatic:**

```java
Config config = new Config();
JobTrackerConfig JTcfg = config.getJobTrackerConfig()
   JTcfg.setName( "default" ).setQueueSize( "0" )
         .setChunkSize( "1000" )
```
   

It has below parameters.



