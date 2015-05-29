
## Client System Properties

There are some advanced client configuration properties to tune some aspects of Hazelcast Client. You can set them as property name and value pairs through declarative configuration, programmatic configuration, or JVM system property. Please see the [System Properties section](#system-properties) to learn how to set these properties.

The table below lists the client configuration properties with their descriptions.

Property Name | Default Value | Type | Description
:--------------|:---------------|:------|:------------
`hazelcast.client.event.queue.capacity`|1000000|string|The default value of the capacity of executor that handles incoming event packets.
`hazelcast.client.event.thread.count`|5|string|The thread count for handling incoming event packets.
`hazelcast.client.heartbeat.interval`|10000|string|The frequency of heartbeat messages sent by the clients to the members.
`hazelcast.client.heartbeat.timeout`|300000|string|Timeout for the heartbeat messages sent by the client to members. If no messages pass between client and member within the given time via this property in milliseconds, the connection will be closed.
`hazelcast.client.invocation.timeout.seconds`|120|string|Time to give up the invocation when a member in the member list is not reachable.
`hazelcast.client.shuffle.member.list`|true|string|The client shuffles the given member list to prevent all clients to connect to the same node when this property is `false`. When it is set to `true`, the client tries to connect to the nodes in the given order.


## Sample Codes for Client

Please refer to [Client Code Samples](https://github.com/hazelcast/hazelcast-code-samples/tree/master/clients).


