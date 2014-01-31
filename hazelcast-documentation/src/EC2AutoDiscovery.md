
### EC2 Auto Discovery

Hazelcast supports EC2 Auto Discovery as of 1.9.4. It is useful when you don't want or can't provide the list of possible IP addresses. Here is a sample configuration: Disable join over multicast and tcp/ip and enable aws. Also provide the credentials. The aws tag accepts an attribute called "connection-timeout-seconds". The default value is 5. Increasing this value is recommended if you have many IP's listed and members can not properly build up the cluster.

```xml
<join>
    <multicast enabled="false">
        <multicast-group>224.2.2.3</multicast-group>
        <multicast-port>54327</multicast-port>
    </multicast>
    <tcp-ip enabled="false">
        <interface>192.168.1.2</interface>
    </tcp-ip>
    <aws enabled="true">
        <access-key>my-access-key</access-key>
        <secret-key>my-secret-key</secret-key>
        <region>us-west-1</region>                              <!-- optional, default is us-east-1 -->
        <host-header>ec2.amazonaws.com</host-header>              <!-- optional, default is ec2.amazonaws.com.
                                                If set, region shouldn't be set as it will override this property -->
        <security-group-name>hazelcast-sg</security-group-name> <!-- optional -->
        <tag-key>type</tag-key>                                  <!-- optional -->
        <tag-value>hz-nodes</tag-value>                          <!-- optional -->
    </aws>
</join>
```
You need to add hazelcast-cloud.jar dependency into your project. Note that it is also bundled inside hazelcast-all.jar. hazelcast-cloud module doesn't depend on any other third party modules.
