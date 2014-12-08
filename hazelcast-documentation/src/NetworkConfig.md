

## Network Configuration

This section discusses how to configure Hazelcast for your network.

### Configuring TCP/IP Cluster

If multicast is not the preferred way of discovery for your environment, then you can configure Hazelcast for full TCP/IP cluster. As the configuration below shows, when the `enabled` attribute of `multicast` is set to false, `tcp-ip` has to be set to true. 

For the none-multicast option, you must list all or a subset of the nodes' hostnames and/or IP addresses. Note that you do not have to list all  cluster members, but at least one of them has to be active in the cluster when a new member joins. 


```xml
<hazelcast>
  ...
  <network>
    <port auto-increment="true">5701</port>
    <join>
      <multicast enabled="false">
        <multicast-group>224.2.2.3</multicast-group>
        <multicast-port>54327</multicast-port>
      </multicast>
      <tcp-ip enabled="true">
        <member>machine1</member>
        <member>machine2</member>
        <member>machine3:5799</member>
        <member>192.168.1.0-7</member>
        <member>192.168.1.21</member>
      </tcp-ip>
    </join>
    ...
  </network>
  ...
</hazelcast>
```

As shown above, you can provide IP addresses or hostnames for `member` elements. You can also give a range of IP addresses like `192.168.1.0-7`.

Instead of providing members line by line, you have the option to use the `members` element and write comma-separated IP addresses, as shown below.

`<members>192.168.1.0-7,192.168.1.21</members>`

If you do not provide ports for the members, Hazelcast automatically tries the ports 5701, 5702, and so on.

By default, Hazelcast binds to all local network interfaces to accept incoming traffic. You can change this behavior using the system property `hazelcast.socket.bind.any`. If you set this property to `false`, Hazelcast uses the interfaces specified in the `interfaces` element (please refer to the [Specifying Network Interfaces section](#specifying-network-interfaces)). If no interfaces are provided, then it will try to resolve one interface to bind, given in the `member` elements.

The `tcp-ip` element accepts an attribute called `connection-timeout-seconds` whose default value is 5. Increasing this value is recommended if you have many IPs listed and the members cannot properly build up the cluster.

There is also an element, `required-member`, that specifies a particular cluster member that must be available before a cluster is formed. 

```xml
<hazelcast>
  ...
  <network>
    <join>
      <tcp-ip enabled="true">
        <required-member>192.168.1.21</required-member>
        <member>machine2</member>
        <member>machine3:5799</member>
        <member>192.168.1.0-7</member>
      </tcp-ip>
    </join>
    ...
  </network>
  ...
</hazelcast>
```

In this example, the cluster will not be formed unless the member with IP address 192.168.1.21 is up.
<br></br>