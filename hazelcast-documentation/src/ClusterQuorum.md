### Cluster Quorum

Hazelcast Cluster Quorum enables you to define the minimum number of machines required in a cluster for the cluster to remain in an operational state. If the number of machines is below the defined minimum at any time, the operations are rejected and the rejected operations return a `QuorumException` to their callers.

When a network partitioning happens, by default Hazelcast chooses to be available. With Cluster Quorum, you can tune your Hazelcast instance towards achieving better consistency by rejecting updates with a minimum threshold. This reduces the chance that the number of concurrent will update to an entry from two partitioned clusters. Note that the consistency defined here is the best effort, not full or strong consistency.

Hazelcast initiates a quorum when a change happens on the member list.

![image](images/NoteSmall.jpg) ***NOTE:*** *Currently, cluster quorum only applies to the Map and Transactional Map, support for other data structures will be added soon. Also, lock methods in the IMap interface do not participate in a quorum.*


#### Configuration

You can set up Cluster Quorum using either declarative or programmatic configuration.

Assume that you have a 5-node Hazelcast Cluster and you want to set the minimum number of 3 nodes for the cluster to continue operating. The following examples are configurations for this scenario.

##### Declarative Configuration

```xml
<hazelcast>
....
<quorum name="quorumRuleWithThreeNodes" enabled="true">
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
You can register quorum listeners to be notified about quorum results. Quorum listeners are local to the node that they are registered, so they receive only events that occurred on that local node.

Quorum listeners can be configured via declarative or programmatic configuration. The following examples are such configurations.

##### Declarative Configuration

```xml
<hazelcast>
....
<quorum name="quorumRuleWithThreeNodes" enabled="true">
  <quorum-size>3</quorum-size>
  <quorum-listeners>
    <quorum-listener>com.company.quorum.ThreeNodeQuorumListener</quorum-listener>
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
Quorum service gives you the ability to query quorum results over the `Quorum` instances.

##### Quorum

Quorum instances lets you query the quorum result of a particular quorum.

Here is a Quorum interface that you can interact with.

```java
/**
 * {@link Quorum} provides access to the current status of a quorum.
 */
public interface Quorum {
    /**
     * Returns true if quorum is present, false if absent.
     *
     * @return boolean presence of the quorum
     */
    boolean isPresent();
}
```
You can retrieve the quorum instance for a particular quorum over the quorum service, as in the following example.

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

boolean quorumPresence = quorum.isPresent();

```
