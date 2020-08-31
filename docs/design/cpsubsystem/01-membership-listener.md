# CP Subsystem Membership Listener

|ℹ️ Since: 4.1| 
|-------------|

## Background

CP Subsystem requires manual user intervention in some cases, especially if CP members crash or become unreachable. 
But there's no programmatic way of monitoring CP members. Users should track Hazelcast logs or look at Management Center 
to see any problems in CP membership healthiness. 

Our goal in this work is to add a new listener to the CP Subsystem to listen events for CP member additions and removals.


## API Design

Two new methods will be added to the CPSubsystem interface, to register and deregister listeners.

```java
    /**
     * Registers a new CPMembershipListener to listen CP membership changes.
     *
     * @param listener membership listener
     * @return id of the listener registration
     * @since 4.1
     */
    UUID addMembershipListener(CPMembershipListener listener);

    /**
     * Removes membership listener registration. Previously registered listener
     * will not receive further events.
     *
     * @param id of the registration
     * @return true if listener registration is removed, false otherwise
     * @since 4.1
     */
    boolean removeMembershipListener(UUID id);
```                        

A new `CPMembershipListener` interface and a new `CPMembershipEvent` interface will be added to the CP Subsystem 
module under `com.hazelcast.cp.event` package.

```java
/**
 * CPMembershipListener is notified when a CP member is added to
 * or removed from the CP Subsystem.
 *
 * @since 4.1
 */
public interface CPMembershipListener extends EventListener {

    /**
     * Called when a new CP member is added to the CP Subsystem.
     *
     * @param event membership event
     */
    void memberAdded(CPMembershipEvent event);

    /**
     * Called when a CP member is removed from the CP Subsystem.
     *
     * @param event membership event
     */
    void memberRemoved(CPMembershipEvent event);
}
```       

```java 
/**
 * CPMembershipEvent is published when a CP member is added to
 * or removed from the CP Subsystem.
 *
 * @since 4.1
 */
public interface CPMembershipEvent {

    /**
     * Membership event type.
     */
    enum EventType { ADDED, REMOVED }

    /**
     * Returns the CPMember that is added to
     * or removed from CP Subsystem.
     *
     * @return the CP member
     */
    CPMember getMember();

    /**
     * Returns the type of membership change.
     *
     * @return membership event type
     */
    EventType getType();
}
```        

`CPMembershipListener` can be registered in declarative configuration too, similar to regular `MembershipListener`.

```xml
<hazelcast>
    ...

    <listeners>
        <listener>com.hazelcast.examples.CPMembershipListener</listener>
    </listeners>

    ...
</hazelcast>
```          

```yaml
hazelcast:
  ...

  listeners:
    - com.hazelcast.examples.CPMembershipListener

  ...
```

## Technical Design

CP membership changes are performed by METADATA group using Raft consensus mechanism. Because of this, events for membership 
changes can be published only by METADATA members. After committing a membership change, a new CPMembershipEvent 
is published by the leader of the METADATA group.   



