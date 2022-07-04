# CP Subsystem Group Availability Listener

|ℹ️ Since: 4.1| 
|-------------|

## Background

CP Subsystem requires manual user intervention in some cases, especially CP members crash or become unreachable. 
But there's no programmatic way of monitoring CP members. Users should track Hazelcast logs or look at Management Center 
to see any problems in CP membership healthiness. 

Our goal in this work to add a new listener to the CP Subsystem to get notified when availability of a CP group decreases. 
This also covers majority loss of a CP group.


## API Design

Two new methods will be added to the CPSubsystem interface, to register and deregister listeners.

```java
    /**
     * Registers a new CPGroupAvailabilityListener to listen CP group availability changes.
     *
     * @param listener group availability listener
     * @return id of the listener registration
     * @since 4.1
     */
    UUID addGroupAvailabilityListener(CPGroupAvailabilityListener listener);

    /**
     * Removes CPGroupAvailabilityListener registration.
     *
     * @param id of the registration
     * @return true if listener registration is removed, false otherwise
     * @since 4.1
     */
    boolean removeGroupAvailabilityListener(UUID id);
```                        

A new `CPGroupAvailabilityListener` interface and a new `CPGroupAvailabilityEvent` interface will be added to the 
CP Subsystem module under `com.hazelcast.cp.event` package.

```java
/**
 * CPGroupAvailabilityListener is notified when availability
 * of a CP group decreases or it loses the majority completely.
 * @since 4.1
 */
public interface CPGroupAvailabilityListener extends EventListener {

    /**
     * Called when a CP group's availability decreases,
     * but still has the majority of members available.
     *
     * @param event CP group availability event
     */
    void availabilityDecreased(CPGroupAvailabilityEvent event);

    /**
     * Called when a CP group has lost its majority.
     *
     * @param event CP group availability event
     */
    void majorityLost(CPGroupAvailabilityEvent event);
}
```       

```java 
/**
 * CPGroupAvailabilityEvent is published when a CP group's
 * availability is decreased or it has lost the majority completely.
 *
 * @since 4.1
 */
public interface CPGroupAvailabilityEvent {

    /**
     * Returns the id of the related CP group.
     *
     * @return CP group id
     */
    CPGroupId getGroupId();

    /**
     * Returns the current members of the CP group.
     *
     * @return group members
     */
    Collection<CPMember> getGroupMembers();

    /**
     * Returns the unavailable members of the CP group.
     *
     * @return unavailable members
     */
    Collection<CPMember> getUnavailableMembers();

    /**
     * Returns the majority member count of the CP group.
     * Simply it's {@code (memberCount/2) + 1}
     *
     * @return majority
     */
    int getMajority();

    /**
     * Returns whether this group has the majority of its members available or not.
     *
     * @return true if the group still has the majority, false otherwise
     */
    boolean isMajorityAvailable();

    /**
     * Returns whether this is the METADATA CP group or not.
     *
     * @return true if the group is METADATA group, false otherwise
     */
    boolean isMetadataGroup();
}
```    

`CPGroupAvailabilityListener` can be registered in declarative configuration too.

```xml
<hazelcast>
    ...

    <listeners>
        <listener>com.hazelcast.examples.CPGroupAvailabilityListener</listener>
    </listeners>

    ...
</hazelcast>
```          

```yaml
hazelcast:
  ...

  listeners:
    - com.hazelcast.examples.CPGroupAvailabilityListener

  ...
```

## Technical Design

CP group operations are performed by METADATA group. Because of this, events for group availability changes are published 
by METADATA members.

In general, availability decreases when a CP member becomes unreachable because of process crash, network partition, 
out of memory etc. Tracking CP group availability is not as strict as CP membership changes. It relies on Hazelcast's 
unreliable membership failure detectors. Once a member is declared as unavailable by the Hazelcast's failure detector, 
that member is removed from the cluster. When a Hazelcast member is removed from the cluster, all METADATA group members 
iterate over CP groups to find out the ones having a member with the same address as the removed member. If a group is 
affected by the member's removal, then a new `CPGroupAvailabilityEvent` is published for that group. That group can be 
METADATA group itself too.

Since all available METADATA members produce the same `CPGroupAvailabilityEvent` for a specific member removal, 
it's possible to receive multiple instances of the same event. To avoid this issue, a deduplication filter is applied 
before invoking `CPGroupAvailabilityListener`. Events with the exact same signature in a _one minute_ period will be 
discarded, only the first instance of them will be passed to the listeners. 

|❗If none of the METADATA group members are reachable or all are crashed, then `CPGroupAvailabilityEvents` will not be published anymore.|
|:-----------------------------------------|

As a special case, CPGroupAvailabilityListener has a separate method to report loss of majority. 
When majority of a CP group is lost, that CP group cannot make progress anymore. Even a new CP member cannot join
to this CP group, because membership changes also go through the Raft consensus algorithm. 
