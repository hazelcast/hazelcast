
### IPv6 Support

Hazelcast supports IPv6 addresses seamlessly (This support is switched off by default, please see the note at the end of this section).

All you need is to define IPv6 addresses or interfaces in [network configuration](#network-configuration). Only limitation at the moment is that you cannot define wildcard IPv6 addresses in [TCP-IP](#configuring-tcp-ip-cluster) join configuration. [Interfaces](#specifying-network-interfaces) section does not have this limitation, you can configure wildcard IPv6 interfaces same as IPv4 interfaces.

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

JVM has two system properties for setting the preferred protocol stack (IPv4 or IPv6) as well as the preferred address family types (inet4 or inet6). On a dual stack machine, IPv6 stack is preferred by default, this can be changed through `java.net.preferIPv4Stack=<true|false>` system property. And when querying name services, JVM prefers IPv4 addressed over IPv6 addresses and will return an IPv4 address if possible. This can be changed through `java.net.preferIPv6Addresses=<true|false>` system property.

Also see additional [details on IPv6 support in Java](http://docs.oracle.com/javase/1.5.0/docs/guide/net/ipv6_guide/query.html#details).

***NOTE:*** *IPv6 support has been switched off by default, since some platforms have issues in use of IPv6 stack. Some other platforms such as Amazon AWS have no support at all. To enable IPv6 support, just set configuration property `hazelcast.prefer.ipv4.stack` to *false*. See [Advanced Configuration Properties](#advanced-configuration-properties).*
<br></br>