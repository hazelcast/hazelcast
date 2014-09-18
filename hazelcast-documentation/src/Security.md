

# Security


## Enabling Security for Hazelcast Enterprise

![](images/enterprise-onlycopy.jpg)



Hazelcast has an extensible, JAAS based security feature which can be used to authenticate both cluster members and clients and to perform access control checks on client operations. Access control can be done according to endpoint principal and/or endpoint address. 

Security can be enabled as stated in the below programmatic or declarative configuration.



```xml
<hazelcast xsi:schemaLocation="http://www.hazelcast.com/schema/config
    http://www.hazelcast.com/schema/config/hazelcast-config-3.3.xsd"
    xmlns="http://www.hazelcast.com/schema/config"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
    
  ...
    
  <security enabled="true">
    ...
  </security>
</hazelcast>
```



```java
Config cfg = new Config();
SecurityConfig securityCfg = cfg.getSecurityConfig();
securityCfg.setEnabled( true );
```

Also, please see [Setting License Key](#setting-license-key).

