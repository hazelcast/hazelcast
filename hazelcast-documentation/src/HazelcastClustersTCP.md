

### Discovery by TCP/IP

If multicast is not the preferred way of discovery for your environment, then you can configure Hazelcast to be a full TCP/IP cluster. When configuring the Hazelcast for discovery by TCP/IP, you must list all or a subset of the nodes' hostnames and/or IP addresses. Note that you do not have to list all  cluster members, but at least one of them has to be active in the cluster when a new member joins.

The following is an example declarative configuration. You should set the  `enabled` attribute of `tcp-ip` element to `true`.

```xml
<hazelcast>
  ...
  <network>
    ...
    <join>
      <multicast enabled="false">
      </multicast>
      <tcp-ip enabled="true">
        <member>machine1</member>
        <member>machine2</member>
        <member>machine3:5799</member>
        <member>192.168.1.0-7</member>
        <member>192.168.1.21</member>
      </tcp-ip>
      ...
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

<br></br>
***RELATED INFORMATION***

*Please refer to the [tcp-ip element section](#tcp-ip-element) for the full description of discovery by TCP/IP configuration.*
<br></br>

