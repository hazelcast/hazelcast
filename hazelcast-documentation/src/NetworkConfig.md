

## Network Configuration



### Configuring TCP/IP Cluster

If multicast is not preferred as the way of discovery for your environment, then you can configure Hazelcast for full TCP/IP cluster. As below configuration shows, while `enable` attribute of `multicast` is set to false, `tcp-ip` has to be set to true. 

For the none-multicast option, all or subset of nodes' hostnames and/or IP addresses must be listed. Note that, all of the cluster members do not have to be listed there but at least one of them has to be active in cluster when a new member joins. 


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

As it can be seen, IP addresses or hostnames can be provided for `member` tags. You can also give a range of IP addresses like `192.168.1.0-7`.

Instead of providing members line by line, you have the option to use `members` tag and write comma-separated IP addresses, as shown below.

`<members>192.168.1.0-7,192.168.1.21</members>`

If ports of members are not provided, Hazelcast automatically tries the ports 5701, 5702, and so on.

Hazelcast binds to all local network interfaces to accept incoming traffic, by default. This behavior can be changed using the system property `hazelcast.socket.bind.any`. When the value of this property is set to `false`, Hazelcast uses the interfaces specified between `interfaces` tag (please refer to *[Specifying Network Interfaces](#specifying-network-interfaces)* section). If there are not any interfaces provided, then it will try to resolve one interface to bind, given in the `member` tags.

The `tcp-ip` tag accepts an attribute called `connection-timeout-seconds` whose default value is 5. Increasing this value is recommended if you have many IPs listed and members cannot properly build up the cluster.

There is also a tag, `required-member`, to specify a particular cluster member which is wanted to be available before a cluster is formed. 

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