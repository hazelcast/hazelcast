## Setting License Key (Enterprise Edition Only)

To be able to use Hazelcast Enterprise Edition, you need to set license key in configuration.

-   **Hazelcast XML Configuration**

```xml
<hazelcast>
    ...
    <license-key>HAZELCAST_ENTERPRISE_LICENSE_KEY</license-key>
    ...
</hazelcast>
```
-   **Hazelcast Config API**

```java
Config config = new Config();
config.setLicenseKey("HAZELCAST_ENTERPRISE_LICENSE_KEY");
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

```
-Dhazelcast.enterprise.license.key=HAZELCAST_ENTERPRISE_LICENSE_KEY
```

