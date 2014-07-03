## Setting License Key

![](images/enterprise-onlycopy.jpg)


To be able to use Hazelcast Enterprise, you need to set license the key in configuration.

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
</hazelcast>
```
-   **JVM System Property**

```plain
-Dhazelcast.enterprise.license.key=HAZELCAST_ENTERPRISE_LICENSE_KEY
```

<br> </br>