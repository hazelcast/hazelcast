### Cluster Quorum

Hazelcast Cluster Quorum enables you to define minimum number of machines required in a cluster to remain in an operational state. If the number of machines is below the defined minimum at any time operations will be rejected and the rejected operations will return a `QuorumException` to their callers.

When a network partition happens, Hazelcast behaves as a AP solution, with cluster quorum you can tune your hazelcast instance, by rejecting updates with a minimum threshold, towards to a CP solution.

By default Hazelcast invokes Cluster Quorum when any change happens on the member list. But you can manually trigger Cluster Quorum at any time.

**Note: Currently cluster quorum only applies to the Map and Transactional Map, support for other data structures will be added soon.**


#### Configuration

You can set up Cluster Quorum either with XML or Programmatic Configuration.

Imagine that you have 5 node Hazelcast Cluster and you want to set minimum numbers of 3 nodes for cluster to continue operating. Your configuration should be like below,

##### XML Configuration

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
Cluster Quorum gives an ability to provide custom quorum resolvers. With custom resolvers instead of sticking to a fixed size, you can implement any quorum resolution logic of your own. Cluster Quorum will call your quorum resolver when a quorums happens on the member.

Your custom resolver must implement `QuorumResolver` interface that can seen below.

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
You can register quorum listeners to be notified about quorum results. Quorum listeners are local to the node that they are registered, so they will receive only events occured on that local node. 

Quorum listeners can be configured via XML or Programmatic configuration. In below, you can see example configurations
 
##### XML Configuration
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
