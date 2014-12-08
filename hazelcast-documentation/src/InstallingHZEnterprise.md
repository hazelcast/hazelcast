

### Hazelcast Enterprise

There are two Maven repositories defined for Hazelcast Enterprise:

```
<repository>
       <id>Hazelcast Private Snapshot Repository</id>
       <url>https://repository-hazelcast-l337.forge.cloudbees.com/snapshot/</url>
</repository>
<repository>
        <id>Hazelcast Private Release Repository</id>
        <url>https://repository-hazelcast-l337.forge.cloudbees.com/release/</url>
</repository>
```

Hazelcast Enterprise customers may also define dependencies, a sample of which is shown below.

```
<dependency>
     <groupId>com.hazelcast</groupId>
     <artifactId>hazelcast-enterprise-tomcat6</artifactId>
     <version>${project.version}</version>
</dependency>
<dependency>
     <groupId>com.hazelcast</groupId>
     <artifactId>hazelcast-enterprise-tomcat7</artifactId>
     <version>${project.version}</version>
</dependency>
<dependency>
      <groupId>com.hazelcast</groupId>
      <artifactId>hazelcast-enterprise</artifactId>
      <version>${project.version}</version>
</dependency>
<dependency>
      <groupId>com.hazelcast</groupId>
      <artifactId>hazelcast-enterprise-all</artifactId>
      <version>${project.version}</version>
</dependency>
```

##### Setting the License Key for Hazelcast Enterprise

To use Hazelcast Enterprise, you need to set the license key in configuration.

-   **Declarative Configuration**

```xml
<hazelcast>
  ...
  <license-key>HAZELCAST_ENTERPRISE_LICENSE_KEY</license-key>
  ...
</hazelcast>
```

-   **Programmatic Configuration**

```java
Config config = new Config();
config.setLicenseKey( "HAZELCAST_ENTERPRISE_LICENSE_KEY" );
```

-   **Spring XML Configuration**

```xml
<hz:config>
  ...
  <hz:license-key>HAZELCAST_ENTERPRISE_LICENSE_KEY</hz:license-key>
  ...
</hz:config>
```

-   **JVM System Property**

```plain
-Dhazelcast.enterprise.license.key=HAZELCAST_ENTERPRISE_LICENSE_KEY
```

<br> </br>


