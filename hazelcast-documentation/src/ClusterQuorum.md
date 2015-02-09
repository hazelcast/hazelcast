### Cluster Quorum

Hazelcast Cluster Quorum enables you to define the minimum number of machines required in a cluster to remain in an operational state. If the number of machines is below the defined minimum at any time, the operations are rejected and the rejected operations return a `QuorumException` to their callers.

When a network partitioning happens, Hazelcast behaves as an AP solution (Availability and Partitioning Tolerance). With Cluster Quorum, you can tune your Hazelcast instance towards a CP solution (Consistency and Partitioning Tolerance), by rejecting updates with a minimum threshold.

By default, Hazelcast invokes Cluster Quorum when any change happens on the member list. You can manually trigger Cluster Quorum at any time.

![image](images/NoteSmall.jpg) ***NOTE:*** *Currently cluster quorum only applies to the Map and Transactional Map, support for other data structures will be added soon.*


#### Configuration

You can set up Cluster Quorum either using declarative or programmatic configuration.

Assume that you have a 5-node Hazelcast Cluster and you want to set the minimum number of 3 nodes for cluster to continue operating. The following are the example configurations for this scenario.

##### Declarative Configuration

```xml
<hazelcast>
....
<quorum name="quorumRuleWithThreeNodes" enabled=true>
  <quorum-size>3</quorum-size>
</quorum>

<map name="default">
<quorum-name>quorumRuleWithThreeNodes</quorum-name>
</map>
....
</hazelcast>

```

##### Programmatic Configuration

```java
QuorumConfig quorumConfig = new QuorumConfig();
quorumConfig.setName("quorumRuleWithThreeNodes")
quorumConfig.setEnabled(true);
quorumConfig.setSize(3);

MapConfig mapConfig = new MapConfig();
mapConfig.setQuorumName("quorumRuleWithThreeNodes");

Config config = new Config();
config.addQuorumConfig(quorumConfig);
config.addMapConfig(mapConfig);

```


#### Custom Quorum Resolvers
Cluster Quorum gives an ability to provide custom quorum resolvers. With custom resolvers, instead of sticking to a fixed size, you can implement any quorum resolution logic of your own. Cluster Quorum will call your quorum resolver when a quorum happens on the member.

Your custom resolver must implement `QuorumResolver` interface that can be seen below.

```java
public interface QuorumResolver {
    boolean resolve(MembershipEvent membershipEvent);
}
```

You can configure custom resolvers like below.
```java
QuorumConfig quorumConfig = new QuorumConfig();
quorumConfig.setName("quorumRuleWithThreeNodes")
quorumConfig.setEnabled(true);

// You can either provide a class name of your implementation,
// or you can directly set instance of your implementation
quorumConfig.setQuorumResolverClassName("com.user.CustomQuorumResolver");
quorumConfig.setQuorumResolverImplementation(new CustomQuorumResolver() {
  @Override
  public boolean resolve(MembershipEvent membershipEvent) {
    if (membershipEvent.getMembers().size() > 3) {
      return true;
    }
    return false;
  }
})

MapConfig mapConfig = new MapConfig();
mapConfig.setQuorumName("quorumRuleWithThreeNodes");

Config config = new Config();
config.addQuorumConfig(quorumConfig);
config.addMapConfig(mapConfig);

```

#### Quorum Listeners
You can register quorum listeners to be notified about quorum results. Quorum listeners are local to the node that they are registered, so they receive only events occurred on that local node. 

Quorum listeners can be configured via declarative or programmatic configuration. The following are the example configurations.
 
##### Declarative Configuration

```xml
<hazelcast>
....
<quorum name="quorumRuleWithThreeNodes" enabled=true>
  <quorum-size>3</quorum-size>
  <quorum-listeners> 
    <quorum-listener>com.company.quorum.ThreeNodeQuorumListener </quorum-listener>
  </quorum-listeners>
</quorum>

<map name="default">
<quorum-name>quorumRuleWithThreeNodes</quorum-name>
</map>
....
</hazelcast>
```

##### Programmatic Configuration

```java
QuorumListenerConfig listenerConfig = new QuorumListenerConfig();
// You can either directly set quorum listener implementation of your own
listenerConfig.setImplementation(new QuorumListener() {
            @Override
            public void onQuorumSuccess(QuorumEvent quorumEvent) {
            }

            @Override
            public void onQuorumFailure(QuorumEvent quorumEvent) {
            }
        });
// Or you can give name of the class that implements QuorumListener interface.
listenerConfig.setClassName("com.company.quorum.ThreeNodeQuorumListener");

QuorumConfig quorumConfig = new QuorumConfig();
quorumConfig.setName("quorumRuleWithThreeNodes")
quorumConfig.setEnabled(true);
quorumConfig.setSize(3);
quorumConfig.addListenerConfig(listenerConfig);


MapConfig mapConfig = new MapConfig();
mapConfig.setQuorumName("quorumRuleWithThreeNodes");

Config config = new Config();
config.addQuorumConfig(quorumConfig);
config.addMapConfig(mapConfig);
```


#### Manually Triggering Cluster Quorum
???

