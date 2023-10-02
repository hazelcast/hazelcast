### Background
#### Motivation

The current lifecycle of a CP object does not allow the same object name to be reused after it was destroyed.

```java
IAtomicLong atomicLong = cpSubsystem.getAtomicLong("iLong");
atomicLong.set(2);
atomicLong.destroy();

IAtomicLong atomicLongNew = cpSubsystem.getAtomicLong("iLong");
atomicLongNew.get(); // throws DistributedObjectDestroyedException
```

This leads to:
- memory leaks (non-significant) since the CP subsystem should track the names of all destroyed objects
- customers dissatisfaction since they should generate another name of the object
- customers do not clean up unnecessary objects to avoid creating another object name, which results in additional overhead and memory consumption

But trying to introduce the ability to reuse the name of a destroyed object, based on the existing "get-or-create" semantics for CP objects, will allow the following behaviour:
```java
IAtomicLong atomicLong = cpSubsystem.getAtomicLong("iLong");
atomicLong.set(2);
atomicLong.destroy();
atomicLong.get(); // succeeds and implicitly recreates internal atomicLong state
```

Furthermore

<table>
<tr>
<td> Client 1 </td> <td> Client 2 </td>
</tr>
<tr>
<td> 

```java
FencedLock lock = cpSubsystem.getLock("iLock");
lock.lock();
lock.destroy();
```

</td>
<td>

```java
FencedLock lock = cpSubsystem.getLock("iLock");


lock.lock(); // recreate and take the lock
```

</td>
</tr>
</table>

Such behaviour breaks the linearizable CP object history and not all clients of object may be aware of the object destruction.

The desirable behaviour, corresponding to a linearizable history, should be as follows:

```java
IAtomicLong atomicLong = cpSubsystem.getAtomicLong("iLong");
atomicLong.set(2);
atomicLong.destroy();
atomicLong.get(); // should failed with exception

atomicLong = cpSubsystem.getAtomicLong("iLong"); // should demand obtaining a fresh proxy
atomicLong.get(); // should succeed
```

#### Current CP object lifecycle implementation

All client interactions with CP objects take place through a proxy: Client proxy or Local proxy in an embedded mode. 

Calling the `CPSubsystem#getLock/getAtomicLong/…` method returns a proxy object and does not trigger a real object creation/initialisation in a CP Subsystem.

An actual object creation/initialisation happens in a lazy way by any business method call on a proxy (for example, `atomicLong.get()`):

```java
V atomicValue = atomicValues.get(key);
if (atomicValue == null) {
    atomicValue = newAtomicValue(groupId, name, null);
    atomicValues.put(key, atomicValue);
}
```

Thus, if an object was destroyed, a new object will be created.

--Sequence diagram--

### POC Functional design

In POC, the existing "get-or-create" semantics have been reworked, and has been introduced the ability to distinguish between old and new objects with the same name.

For tracking CP object lifecycle (creation/destruction) each internal CP object should have a unique object ID (UUID, Random Long, Timestamp, FlakeIdGenerator, ...):

- A Hazelcast client should obtain an object ID during the Client-proxy creation
- Each Client → Server CP method call (operation) should include object ID
- If there is no such object ID on the Server side, there should be an exception raised and the Client has to recreate the proxy to obtain a new object ID
- When the CP object is destroyed, the object ID should be destroyed
- The object ID should be persisted and maintained in a consistent way (the same as an object value) on all CP group members

To implement a such behaviour a new codec, message and Raft operation were added:
- [CPGroupCreateCPObjectCodec](../../../hazelcast/src/main/java/com/hazelcast/client/impl/protocol/codec/CPGroupCreateCPObjectCodec.java) 
- [CreateRaftObjectMessageTask](../../../hazelcast/src/main/java/com/hazelcast/cp/internal/datastructures/spi/client/CreateRaftObjectMessageTask.java)
- [CreateOrGetRaftObjectOp](../../../hazelcast/src/main/java/com/hazelcast/cp/internal/datastructures/spi/operation/CreateOrGetRaftObjectOp.java)

This flow creates an empty data structure of a specific type (serviceName) on the Server side with an UUID and returns this UUID to the Client side:

```java
private UUID getObjectId(RaftGroupId groupId, String serviceName, String objectName) {
    ClientMessage request = CPGroupCreateCPObjectCodec.encodeRequest(groupId, serviceName, objectName);
    ClientMessage response = new ClientInvocation(client, request, objectName).invoke().joinInternal();
    return CPGroupCreateCPObjectCodec.decodeResponse(response);
}
```

This UUID is recorded as a property of the object proxy on the Client side:

```java
public class AtomicLongProxy extends ClientProxy implements IAtomicLong {

    private final RaftGroupId groupId;
    private final String objectName;
    private UUID objectUUID;

    public AtomicLongProxy(ClientContext context, RaftGroupId groupId, String proxyName, String objectName, UUID objectUUID) {
    ...
    }
}
```

On the Server side, each CP data structure (at the current moment `AtomicLong` and `AtomicRef`) should also have a UUID property:

```java
public abstract class RaftAtomicValue<T> {

    private final CPGroupId groupId;
    private final String name;
    private UUID uuid;

    public RaftAtomicValue(CPGroupId groupId, String name, UUID uuid) {
        this.groupId = groupId;
        this.name = name;
        this.uuid = uuid;
    }
    ...
}
```

To implement Client -> Server interaction there are two options:
- the name-mangling scheme
- extend Client/Server API

Let's analyze each of those options

#### The name-mangling scheme

The name-mangling scheme has been implemented in a POC

To minimize the number of changes on a client/server side, and do not recreate/update dozens of codecs, messages and Raft operations, object UUID and object name are concatenated on the Client side (the name-mangling scheme). This combined object name passes via existing API during each operation call:

```java
public class AtomicLongProxy extends ClientProxy implements IAtomicLong {

    private final RaftGroupId groupId;
    private final String objectName;
    private UUID objectUUID;
    ...
    
    public InternalCompletableFuture<Long> getAsync() {
        ClientMessage request = AtomicLongGetCodec.encodeRequest(groupId, getCombinedObjectName());
        ClientInvocationFuture future = new ClientInvocation(getClient(), request, name).invoke();
        return new ClientDelegatingFuture<>(future, getSerializationService(), AtomicLongGetCodec::decodeResponse);
    }

    private String getCombinedObjectName() {
        return objectName + "@" + objectUUID;
    }
    ...
}
```

On the Server side, the combined object name is decoded (split), and we check if an object with such name and UUID exists:

```java
    public final V getAtomicValue(CPGroupId groupId, String name) {
        checkNotNull(groupId);
        checkNotNull(name);
        Optional<UUID> uuid = getUuidFromName(name);
        String objectName = getObjectName(name);
        BiTuple<CPGroupId, String> key = BiTuple.of(groupId, objectName);
        if (destroyedValues.contains(key)) {
            throw new DistributedObjectDestroyedException("AtomicValue[" + name + "] is already destroyed!");
        }
        V atomicValue = atomicValues.get(key);
        // For 5.3 compatibility
        if (atomicValue == null && uuid.isEmpty()) {
            atomicValue = newAtomicValue(groupId, name, null, ZERO_UUID);
            atomicValues.put(key, atomicValue);
        } else if (atomicValue == null || (uuid.isPresent() && !atomicValue.uuid().equals(uuid.get()))) {
            throw new DistributedObjectDestroyedException("AtomicValue[" + name + "] is already destroyed!");
        }
        return atomicValue;
    }
```

During the Raft log snapshot (and persist), the combined object name is used again:
```java
    public final S takeSnapshot(CPGroupId groupId, long commitIndex) {
        checkNotNull(groupId);
        Map<String, T> values = new HashMap<>();
        for (V value : atomicValues.values()) {
            if (value.groupId().equals(groupId)) {
                values.put(getCombinedObjectName(value.name(), value.uuid()), value.get());
            }
        }
        ...
    }
        
    ...

    public final void restoreSnapshot(CPGroupId groupId, long commitIndex, S snapshot) {
        checkNotNull(groupId);
        for (Map.Entry<String, T> e : snapshot.getValues()) {
            String name = e.getKey();
            T val = e.getValue();
            UUID uuid = getUuidFromName(name).orElse(ZERO_UUID);
            String objectName = getObjectName(name);
            atomicValues.put(BiTuple.of(groupId, objectName), newAtomicValue(groupId, objectName, val, uuid));
        }
        ...
    }
```

Pros:
- minimal code changes on Client/Server
- no existing Client/Server API changes
- no changes in persisted Raft-log format
- minimal changes for client(s)-to-member compatibility
- no changes for member-to-member compatibility
- no migration is needed for persisted Raft-log compatibility (5.4 version can use 5.3 log)

Cons:
- less source maintainability and higher chance of error (due to juggling with the mangled name)
- mangled name parsing overhead (performance degradation)
- new Client -> Server API call for object creation

#### Extending Client/Server API

To pass the object ID from Client -> Server, another option is to extend the Client/Server API.

To do this, a new object ID parameter should be added to all Client -> Server API calls and subsequent method call chains.

An approximate number of API changes:
- ~ 30 codecs (and regenerate them for all Hazelcast client implementations)
- ~ 30 MessageTasks
- ~ 10 RaftOps

Pros:
- explicit and clear API
- better source maintainability
- no performance degradation expected

Cons:
- significant codebase changes on server side and for all Hazelcast client implementations
- new Client -> Server API call for object creation
- changing persisted Raft-log format
- efforts to provide persisted Raft-log compatibility (start 5.4 version with 5.3 log, possibly need some tool for Raft-log migration)
- efforts to provide client(s)-to-member compatibility (for codecs)
- efforts to provide member-to-member compatibility (for MessageTasks and RaftOps)
- extended testing for all changed API calls

### Name-mangling POC performance test

For testing differences in performance, the following rough test was used:

```java
        long start = System.currentTimeMillis();
        IAtomicLong aLong = client.getCPSubsystem().getAtomicLong("aLong@testGroup");
        for (int i = 0; i < 100_000; i++) {
            long val = aLong.get();
            aLong.set(val);
            aLong.getAndAdd(i);
        }

        long res = aLong.get();
        long finish = System.currentTimeMillis();
```

On the local dev environment, the POC version is 4-5% slower than the existing one 5.4.0-SNAPSHOT.


### Name-mangling Compatibility
For 5.3 Client -> 5.4 Server compatibility, an existing "get-or-create" semantics have been saved. If there is no object UUID in a client request, the "get-or-create" approach is used:

```java
    public final V getAtomicValue(CPGroupId groupId, String name) {
        checkNotNull(groupId);
        checkNotNull(name);
        Optional<UUID> uuid = getUuidFromName(name);
        String objectName = getObjectName(name);
        BiTuple<CPGroupId, String> key = BiTuple.of(groupId, objectName);
        if (destroyedValues.contains(key)) {
            throw new DistributedObjectDestroyedException("AtomicValue[" + name + "] is already destroyed!");
        }
        V atomicValue = atomicValues.get(key);
        // For 5.3 compatibility
        if (atomicValue == null && uuid.isEmpty()) {
            atomicValue = newAtomicValue(groupId, name, null, ZERO_UUID);
            atomicValues.put(key, atomicValue);
        } else if (atomicValue == null || (uuid.isPresent() && !atomicValue.uuid().equals(uuid.get()))) {
            throw new DistributedObjectDestroyedException("AtomicValue[" + name + "] is already destroyed!");
        }
        return atomicValue;
    }
```

In that case as a new object with `ZERO_UUID` UUID will be created. This provides compatibility with 5.4 version. 