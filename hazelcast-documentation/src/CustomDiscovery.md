
### Custom Discovery

Hazelcast supports custom cluster node discovery. This is useful when you want to provide your custom logic for cluster node discovery and participation.  To provide your own
custom logic, enable the 'custom' join configuration, and disable all other join configurations. Within your custom configuration, you must provide a fully qualified class name for your
own implementation of the com.hazelcast.cluster.JoinerFactory interface.  Your implementation of JoinerFactory must contain a single, no-arg constructor and implement the createJoiner(Node) method.
You can then provide your custom discovery/joining logic within the returned com.hazelcast.cluster.Joiner instance.

You can expose configuration properties to your custom join logic by adding the properties within the xml configuration.  You can then access the properties by querying the Node instance passed to
the createJoiner(Node) method.

Below is a sample configuration. 

```xml
<join>
    <multicast enabled="false">
        <multicast-group>224.2.2.3</multicast-group>
        <multicast-port>54327</multicast-port>
    </multicast>
    <tcp-ip enabled="false">
        <interface>192.168.1.2</interface>
    </tcp-ip>
    <custom enabled="false" joiner-factory-class-name="org.myorg.myapp.MyCustomJoinerFactory">
        <properties>
            <property name="my-key-1">my-value-1</property>
            <property name="my-key-2">my-value-2</property>
        </properties>
    </custom>
</join>
```