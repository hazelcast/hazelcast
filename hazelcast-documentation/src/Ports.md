

### Ports

You can specify the ports which Hazelcast will use to communicate between cluster members. The name of the parameter for this is `port` and its default value is `5701`.

```xml
<network>
   <port>5701</port>
</network>
```

By default, Hazelcast will try 100 ports to bind. Meaning that, if you set the value of port as 5701, as members are joining to the cluster, Hazelcast tries to find ports between 5701 and 5801. 

You can choose to change the port count in the cases like having large instances on a single machine or willing to have only a few ports to be assigned. The parameter `port-count` is used for this purpose, whose default value is 100.

```xml
<network>
   <port port-count="20">5781</port>
</network>
```

According to the above example, Hazelcast will try to find free ports between 5781 and 5801. Normally, you will not need to change this value, but it will come very handy when needed. You may also want to choose to use only one port. In that case, you can disable the auto-increment feature of `port`, as shown below.

```xml
<network>
   <port auto-increment="false">5701</port>
</network>
```

Natrually, the parameter `port-count` is ignored when the above configuration is made.


#### Outbound Ports

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

Or, by programmatically:

```java
...
NetworkConfig networkConfig = config.getNetworkConfig();
networkConfig.addOutboundPortDefinition("35000-35100");  // ports between 35000 and 35100
networkConfig.addOutboundPortDefinition("36001, 36002, 36003"); // comma separated ports
networkConfig.addOutboundPort(37000);
networkConfig.addOutboundPort(37001);
...
```

***Note:*** *You can use port ranges and/or comma separated ports.*
<br></br>