
### IPv6 Support

Hazelcast seamlessly supports IPv6 addresses. (This support is switched off by default, please see the note at the end of this section.)

You only need to define the IPv6 addresses or interfaces in [network configuration](#network-configuration). The only limitation at the moment is that you cannot define wildcard IPv6 addresses in the [TCP-IP](#configuring-tcp-ip-cluster) join configuration. The [Interfaces section](#specifying-network-interfaces) section does not have this limitation, you can configure wildcard IPv6 interfaces the same as IPv4 interfaces.

```xml
<hazelcast>
  ...
  <network>
    <port auto-increment="true">5701</port>
    <join>
      <multicast enabled="false">
        <multicast-group>FF02:0:0:0:0:0:0:1</multicast-group>
        <multicast-port>54327</multicast-port>
      </multicast>
      <tcp-ip enabled="true">
        <member>[fe80::223:6cff:fe93:7c7e]:5701</member>
        <interface>192.168.1.0-7</interface>
        <interface>192.168.1.*</interface>
        <interface>fe80:0:0:0:45c5:47ee:fe15:493a</interface>
      </tcp-ip>
    </join>
    <interfaces enabled="true">
      <interface>10.3.16.*</interface>
      <interface>10.3.10.4-18</interface>
      <interface>fe80:0:0:0:45c5:47ee:fe15:*</interface>
      <interface>fe80::223:6cff:fe93:0-5555</interface>
    </interfaces>
    ...
  </network>
  ...
</hazelcast>
```

JVM has two system properties you can use to set the preferred protocol stack (IPv4 or IPv6) and the preferred address family types (inet4 or inet6). On a dual stack machine, IPv6 stack is preferred by default. You can change this with the `java.net.preferIPv4Stack=<true|false>` system property. When querying name services, JVM prefers IPv4 addresses over IPv6 addresses and it will return an IPv4 address if possible. You can change this with the `java.net.preferIPv6Addresses=<true|false>` system property.

Also see additional [details on IPv6 support in Java](http://docs.oracle.com/javase/1.5.0/docs/guide/net/ipv6_guide/query.html#details).

![image](images/NoteSmall.jpg) ***NOTE:*** *IPv6 support is switched off by default, since some platforms have issues using IPv6 stack. Some other platforms, such as Amazon AWS, have no support at all. To enable IPv6 support, set configuration property `hazelcast.prefer.ipv4.stack` to *false*. Please see the [Advanced Configuration Properties section](#advanced-configuration-properties).*
<br></br>