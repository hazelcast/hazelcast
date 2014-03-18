

### Restricting Outbound Ports

By default, Hazelcast lets the system to pick up an ephemeral port during socket bind operation. But security policies/firewalls may require to restrict outbound ports to be used by Hazelcast enabled applications. To fulfill this requirement, you can configure Hazelcast to use only defined outbound ports.

```xml
<hazelcast>
    ...
    <network>
        <port auto-increment="true">5701</port>
        <outbound-ports>
            <ports>33000-35000</ports>   <!-- ports between 33000 and 35000 -->
            <ports>37000,37001,37002,37003</ports> <!-- comma separated ports -->
            <ports>38000,38500-38600</ports>
        </outbound-ports>
        ...
    </network>
    ...
</hazelcast>
```
```java
...
NetworkConfig networkConfig = config.getNetworkConfig();
networkConfig.addOutboundPortDefinition("35000-35100");         // ports between 35000 and 35100
networkConfig.addOutboundPortDefinition("36001, 36002, 36003"); // comma separated ports
networkConfig.addOutboundPort(37000);
networkConfig.addOutboundPort(37001);
...
```

***Note:*** *You can use port ranges and/or comma separated ports.*
