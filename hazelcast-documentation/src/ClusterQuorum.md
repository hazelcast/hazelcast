### Cluster Quorum

Hazelcast Cluster Quorum enables you to define the minimum number of machines required in a cluster for the cluster to remain in an operational state. If the number of machines is below the defined minimum at any time, the operations are rejected and the rejected operations return a `QuorumException` to their callers.

When a network partitioning happens, by default Hazelcast chooses to be available. With Cluster Quorum, you can tune your Hazelcast instance towards to achieve better consistency, by rejecting updates with a minimum threshold, this reduces the chance that number of concurrent updates to an entry from two partitioned clusters . Note that the consistency defined here is best effort not full or strong consistency.

By default, Hazelcast invokes a quorum when a change happens on the member list. You can also provide quorum results to the quorum service over the `Quorum` instance and hazelcast can act on them.

![image](images/NoteSmall.jpg) ***NOTE:*** *Currently cluster quorum only applies to the Map and Transactional Map, support for other data structures will be added soon.*


#### Configuration

You can set up Cluster Quorum using either declarative or programmatic configuration.

Assume that you have a 5-node Hazelcast Cluster and you want to set the minimum number of 3 nodes for cluster to continue operating. The following are example configurations for this scenario.

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
            public void onChange(QuorumEvent quorumEvent) {
              if (QuorumResult.PRESENT.equals(quorumEvent.getType())) {
                // handle quorum presence
              } else if (QuorumResult.ABSENT.equals(quorumEvent.getType())) {
                // handle quorum absence
              }
            }
        });
// Or you can give the name of the class that implements QuorumListener interface.
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




#### Quorum Service
Quorum service gives an ability to provide quorum results of your own over the `Quorum` instances. With quorum instances, instead of sticking to a fixed size quorum check, you can implement any quorum resolution logic of your own. You just need to notify the result to hazelcast. Hazelcast will allow or reject operations that requires quorum based on the provided quorum result.


##### Quorum

Quorum instances let you to provide quorum result of your own to the hazelcast quorum service.

Here is the Quorum interface that you can interact with.
```java
/**
 * {@link Quorum} can be used to notify quorum service for a particular quorum result that originated externally.
 */
public interface Quorum {
    /**
     * Returns latest presence/absence of the quorum.
     *
     * @return boolean presence of the quorum
     */
    boolean isPresent();

    /**
     * Sets given quorum result as the result of the quorum locally.
     * Quorum aware operations on this node will see this result
     *
     * @param presence
     */
    void setLocalResult(boolean presence);

    /**
     * Sets given quorum result as the result of the quorum on all cluster members.
     * Quorum aware operations on the cluster will see this result
     * <p/>
     * Note: This method is not thread-safe. You should be providing quorum results from single place,
     * If you update quorum state from multiple nodes with different values, you might see inconsistent quorum
     * states between cluster members.
     *
     * @param presence
     */
    void setGlobalResult(boolean presence);
}
}
```
You can retrieve quorum instance for a particular quorum over the quorum service. An example can be seen below

```java
String quorumName = "at-least-one-storage-member";
QuorumConfig quorumConfig = new QuorumConfig();
quorumConfig.setName(quorumName)
quorumConfig.setEnabled(true);

MapConfig mapConfig = new MapConfig();
mapConfig.setQuorumName(quorumName);

Config config = new Config();
config.addQuorumConfig(quorumConfig);
config.addMapConfig(mapConfig);

HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(config);
QuorumService quorumService = hazelcastInstance.getQuorumService();
Quorum quorum = quorumService.getQuorum(quorumName);

// from now on you can listen to any event in the cluster that you might be interested, make quorum then notify the hazelcast with the result.
quorum.setResult(false); // this will reject operations for the particular quorum.

```


Imagine that you have a use case like in your cluster there is a special member which has an ability to make heavy calculations, and if that special member is not present in the cluster you don't want to try processing entries. You can achieve to implement such scenario with quorums.  
