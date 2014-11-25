

### Ports

You can specify the ports that Hazelcast will use to communicate between cluster members. The name of the parameter is `port` and its default value is `5701`.

```xml
<network>
  <port>5701</port>
</network>
```

By default, Hazelcast will try 100 ports to bind: if you set the value of `port` as 5701, as members join the cluster, Hazelcast will try to find ports between 5701 and 5801. 

You can change the port count. You might have cases like having large instances on a single machine or having only a few ports to be assigned. Use the parameter `port-count` for this purpose. Its default value is 100.

```xml
<network>
  <port port-count="20">5781</port>
</network>
```

According to the above example, Hazelcast will try to find free ports between 5781 and 5801. Normally, you will not need to change `port-count`, but it will come handy when needed. You may also want to choose to use only one port. In that case, you can disable the auto-increment feature of `port`, as shown below.

```xml
<network>
  <port auto-increment="false">5701</port>
</network>
```

The parameter `port-count` is ignored when `auto-increment` is used.


#### Outbound Ports

By default, Hazelcast lets the system pick up an ephemeral port during socket bind operations. But security policies/firewalls may require you to restrict the outbound ports used by Hazelcast-enabled applications. To fulfill this requirement, you can configure Hazelcast to use only defined outbound ports.

You can configure this declaratively. 

```xml
<hazelcast>
  ...
  <network>
    <port auto-increment="true">5701</port>
    <outbound-ports>
      <!-- ports between 33000 and 35000 -->
      <ports>33000-35000</ports>
      <!-- comma separated ports -->
      <ports>37000,37001,37002,37003</ports> 
      <ports>38000,38500-38600</ports>
    </outbound-ports>
    ...
  </network>
  ...
</hazelcast>
```

You can also declare this programmatically.

```java
...
NetworkConfig networkConfig = config.getNetworkConfig();
// ports between 35000 and 35100
networkConfig.addOutboundPortDefinition("35000-35100");
// comma separated ports
networkConfig.addOutboundPortDefinition("36001, 36002, 36003");
networkConfig.addOutboundPort(37000);
networkConfig.addOutboundPort(37001);
...
```

![image](images/NoteSmall.jpg) ***NOTE:*** *You can use port ranges and/or comma separated ports.*
<br></br>
