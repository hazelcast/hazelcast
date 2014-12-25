

### Multicast Auto-discovery

With the multicast auto-discovery mechanism, Hazelcast allows cluster members to find each other using multicast communication. The cluster members do not need to know concrete addresses of each other, they just multicast to everyone for listening. It depends on your environment if multicast is possible or allowed.

The following is an example declarative configuration.

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
           </tcp-ip>
            <aws enabled="false">
            </aws>
        </join>
   <network>     
```

You should pay attention to the `multicast-timeout-seconds` element. This element specifies the time in seconds that a node should wait for a valid multicast response from another node running in the network before declaring itself as the leader node (first node joined to the cluster) and creating its own cluster. This only applies to the startup of nodes where no leader has been assigned yet. If you specify a high value for the `multicast-timeout-seconds` like 60 seconds, it means until a leader is selected, each node is going to wait 60 seconds before moving on. Therefore, be careful when providing a high value. If the value is too low, the nodes might give up too early and create their own cluster.

<br></br>
***RELATED INFORMATION***

*Please refer to the [multicast element section](#multicast-element) for the full description of multicast discovery configuration.*
<br></br>

