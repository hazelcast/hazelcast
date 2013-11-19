

# Security (Enterprise Edition Only)#

Hazelcast has an extensible, JAAS based security feature which can be used to authenticate both cluster members and clients and to do access control checks on client operations. Access control can be done according to endpoint principal and/or endpoint address. 

Security can be enabled and configured either in configuration xml 

```xml
<hazelcast xsi:schemaLocation="http://www.hazelcast.com/schema/config
    http://www.hazelcast.com/schema/config/hazelcast-config-3.1.xsd"
    xmlns="http://www.hazelcast.com/schema/config"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
    
    ...
    <security enabled="true">
    ...
    </security>
</hazelcast>
```
or using Config API.

```java
Config cfg = new Config();
SecurityConfig securityCfg = cfg.getSecurityConfig();
securityCfg.setEnabled(true);
```

Also see how to configure license key.

