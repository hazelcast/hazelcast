
## Network Configuration

All network related configuration is performed via `network` tag in the XML file or the class `NetworkConfig` when using programmatic configuration. Let's first give the samples for these two approaches. Then we will look at its parameters, which are a lot.

**Declarative:**


```xml
   <network>
        <public-address> ??? </public-address>
        <port auto-increment="true" port-count="100">5701</port>
        <outbound-ports>
            <ports>0</ports>
        </outbound-ports>
        <join>
            <multicast enabled="true">
                <multicast-group>224.2.2.3</multicast-group>
                <multicast-port>54327</multicast-port>
            </multicast>
            <tcp-ip enabled="false">
                <interface>127.0.0.1</interface>
            </tcp-ip>
            <aws enabled="false">
                <access-key>my-access-key</access-key>
                <secret-key>my-secret-key</secret-key>
                <region>us-west-1</region>
                <host-header>ec2.amazonaws.com</host-header>
                <security-group-name>hazelcast-sg</security-group-name>
                <tag-key>type</tag-key>
                <tag-value>hz-nodes</tag-value>
            </aws>
        </join>
        <interfaces enabled="false">
            <interface>10.10.1.*</interface>
        </interfaces>
        <ssl enabled="false" />
        <socket-interceptor enabled="false" />
        <symmetric-encryption enabled="false">
            <algorithm>PBEWithMD5AndDES</algorithm>
            <salt>thesalt</salt>
            <password>thepass</password>
            <iteration-count>19</iteration-count>
        </symmetric-encryption>
    </network>   
```

**Programmatic:**

```java
AwsConfig config = new AwsConfig();
config.setTagKey( "5551234" );
config.setTagValue( "Node1234" )
```

It has below parameters which are briefly described in the following subsections.

- public-address
- port
- outbound-ports
- join
- interfaces
- ssl
- socket-interceptor
- symmetric-encryption

### Public Address

It is used to override public address of a node. By default, a node selects its socket address as its public address. But behind a network address translation (NAT), two endpoints (nodes) may not be able to see/access each other. If both nodes set their public addresses to their defined addresses on NAT, then that way they can communicate with each other. In this case, their public addresses are not an address of a local network interface but a virtual address defined by NAT. It is optional to set and useful when you have a private cloud.

### Port

You can specify the ports which Hazelcast will use to communicate between cluster members. Its default value is `5701`. Sample configurations are shown below.

**Declarative:**

```xml
<network>
  <port port-count="20" auto-increment="false">5701</port>
</network>
```

**Programmatic:**

```java
Config config = new Config();
config.getNetworkConfig().setPort( "5900" ); 
             .setPortCount( "20" ).setPortAutoIncrement( "false" );
```

It has below attributes.

- `port-count`: By default, Hazelcast will try 100 ports to bind. Meaning that, if you set the value of port as 5701, as members are joining to the cluster, Hazelcast tries to find ports between 5701 and 5801. You can choose to change the port count in the cases like having large instances on a single machine or willing to have only a few ports to be assigned. The parameter `port-count` is used for this purpose, whose default value is 100.



- `auto-increment`: According to the above example, Hazelcast will try to find free ports between 5781 and 5801. Normally, you will not need to change this value, but it will come very handy when needed. You may also want to choose to use only one port. In that case, you can disable the auto-increment feature of `port` by setting its value as `false`.


Naturally, the parameter `port-count` is ignored when the above configuration is made.

### Outbound Ports


By default, Hazelcast lets the system to pick up an ephemeral port during socket bind operation. But security policies/firewalls may require to restrict outbound ports to be used by Hazelcast enabled applications. To fulfill this requirement, you can configure Hazelcast to use only defined outbound ports. Sample configurations are shown below.


**Declarative:**

```xml
  <network>
    <outbound-ports>
      <!-- ports between 33000 and 35000 -->
      <ports>33000-35000</ports>
      <!-- comma separated ports -->
      <ports>37000,37001,37002,37003</ports> 
      <ports>38000,38500-38600</ports>
    </outbound-ports>
  </network>
```

**Programmatic:**

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

***Note:*** *You can use port ranges and/or comma separated ports.*

As you can see in the programmatic configuration, if you want to add only one port you use the method `addOutboundPort`. If a group of ports needs to be added, then the method `addOutboundPortDefinition` is used. 

In the declarative one, the tag `ports` can be used for both (for single and multiple port definitions).


### Join

This configuration parameter is used to enable the Hazelcast instances to form a cluster, i.e. to join the members. Three ways can be used to join the members: TCP/IP, multicast and AWS (EC2). Below are sample configurations.

**Declarative:**

```xml
   <network>
        <join>
            <multicast enabled="true">
                <multicast-group>224.2.2.3</multicast-group>
                <multicast-port>54327</multicast-port>
                <multicast-time-to-live>32</multicast-time-to-live>
                <multicast-timeout-seconds>2</multicast-timeout-seconds>
                <trusted-interfaces>
                   <interface>192.168.1.102</interface>
                </trusted-interfaces>   
            </multicast>
            <tcp-ip enabled="false">
                <required-member>192.168.1.104</required-member>
                <member>192.168.1.104</member>
                <members>192.168.1.105,192.168.1.106</members>
            </tcp-ip>
            <aws enabled="false">
                <access-key>my-access-key</access-key>
                <secret-key>my-secret-key</secret-key>
                <region>us-west-1</region>
                <host-header>ec2.amazonaws.com</host-header>
                <security-group-name>hazelcast-sg</security-group-name>
                <tag-key>type</tag-key>
                <tag-value>hz-nodes</tag-value>
            </aws>
        </join>
   <network>     
```

**Programmatic:**

```java
Config config = new Config();
NetworkConfig network = config.getNetworkConfig();
JoinConfig join = network.getJoin();
join.getMulticastConfig().setEnabled( "false" )
            .addTrustedInterface( "192.168.1.102" );
join.getTcpIpConfig().addMember( "10.45.67.32" ).addMember( "10.45.67.100" )
            .setRequiredMember( "192.168.10.100" ).setEnabled( true );
```

It has below elements and attributes.

- `multicast`: It includes parameters to fine tune the multicast join mechanism.
	- `enabled`: Specifies whether the multicast discovery is enabled or not. Values can be `true` or `false`.
	- `multicast-group`: The multicast group IP address. Specify it when you want to create clusters within the same network. Values can be between 224.0.0.0 and 239.255.255.255. Default value is 224.2.2.3
	- `multicast-port`: The multicast socket port which Hazelcast member listens to and sends discovery messages through it. Default value is 54327.
	- `multicast-time-to-live`: Time-to-live value for multicast packets sent out to control the scope of multicasts. You can have more information [here](http://www.tldp.org/HOWTO/Multicast-HOWTO-2.html).
	- `multicast-timeout-seconds`: Only when the nodes are starting up, this timeout (in seconds) specifies the period during which a node waits for a multicast response from another node. For example, if you set it as 60 seconds, each node will wait for 60 seconds until a leader node is selected. Its default value is 2 seconds. 
	- `trusted-interfaces`: Includes IP addresses of trusted members. When a node wants to join to the cluster, its join request will be rejected if it is not a trusted member. You can give an IP addresses range using the wildcard (\*) on the last digit of IP address (e.g. 192.168.1.\* or 192.168.1.100-110).
	
- `tcp-ip`: It includes parameters to fine tune the TCP/IP join mechanism.
	- `enabled`: Specifies whether the TCP/IP discovery is enabled or not. Values can be `true` or `false`.
	- `required-member`: IP address of the required member. Cluster will only formed if the member with this IP address is found.
	- `member`: IP address(es) of one or more well known members. Once members are connected to these well known ones, all member addresses will be communicated with each other. You can also give comma separated IP addresses using the `members` tag.

- `aws`: It includes parameters to allow the nodes form a cluster on Amazon EC2 environment.
	- `enabled`: Specifies whether the EC2 discovery is enabled or not. Values can be `true` or `false`.
	- `access-key`, `secret-key`: Access and secret keys of your account on EC2.
	- `region`: The region where your nodes are running. Default value is `us-east-1`. Needs to be specified if the region is other than the default one.
	- `host-header`: ???. It is optional.
	- `security-group-name`:Name of the security group you specified at the EC2 management console. It is used to narrow the Hazelcast nodes to be within this group. It is optional.
	- `tag-key`, `tag-value`: To narrow the members in the cloud down to only Hazelcast nodes, you can set these parameters as the ones you specified in the EC2 console. They are optional.

### Interfaces

You can specify which network interfaces that Hazelcast should use. Servers mostly have more than one network interface so you may want to list the valid IPs. Range characters ('\*' and '-') can be used for simplicity. So 10.3.10.\*, for instance, refers to IPs between 10.3.10.0 and 10.3.10.255. Interface 10.3.10.4-18 refers to IPs between 10.3.10.4 and 10.3.10.18 (4 and 18 included). If network interface configuration is enabled (disabled by default) and if Hazelcast cannot find an matching interface, then it will print a message on console and will not start on that node.

**Declarative:**

```xml
<hazelcast>
  ...
  <network>
    ...
    <interfaces enabled="true">
      <interface>10.3.16.*</interface> 
      <interface>10.3.10.4-18</interface> 
      <interface>192.168.1.3</interface>         
    </interfaces>    
  </network>
  ...
</hazelcast> 
```

**Programmatic:**

```java
Config config = new Config();
NetworkConfig network = config.getNetworkConfig();
InterfacesConfig interface = network.getInterfaces();
interface.setEnabled( "true" )
            .addInterface( "192.168.1.3" );
```




### SSL

This is a Hazelcast Enterprise feature, please see [Security](#security) chapter.

### Socket Interceptor

This is a Hazelcast Enterprise feature, please see [Security](#security) chapter.

### Symmetric Encryption

This is a Hazelcast Enterprise feature, please see [Security](#security) chapter.

### IPv6 Support

Hazelcast supports IPv6 addresses seamlessly (This support is switched off by default, please see the note at the end of this section).

All you need is to define IPv6 addresses or interfaces in [network configuration](#network-configuration). Only limitation at the moment is that you cannot define wildcard IPv6 addresses in TCP/IP join configuration (`tcp-ip` element). [Interfaces](#interfaces) section does not have this limitation, you can configure wildcard IPv6 interfaces in the same way as IPv4 interfaces.

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

![image](images/NoteSmall.jpg) ***NOTE:*** *IPv6 support has been switched off by default, since some platforms have issues in use of IPv6 stack. Some other platforms such as Amazon AWS have no support at all. To enable IPv6 support, just set configuration property `hazelcast.prefer.ipv4.stack` to *false*. See [Advanced Configuration Properties](#advanced-configuration-properties).*
<br></br>
