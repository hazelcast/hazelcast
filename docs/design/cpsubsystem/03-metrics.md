# CP Subsystem Metrics

|ℹ️ Since: 4.1| 
|-------------|

## Background

CP Subsystem statistics and metrics are not visible in the Management Center yet. Also there aren't any stats/metrics 
accessible via public API. There are only a few Raft group metrics published via [Metrics](https://docs.hazelcast.org/docs/latest/manual/html-single/index.html#metrics) 
system. Our goal in this work is to publish more metrics about CP Subsystem and CP data structures via Metrics system.
These metrics will be immediately accessible through [Diagnostics](https://docs.hazelcast.org/docs/latest/manual/html-single/index.html#diagnostics) 
and Jmx and in a later step will be exposed through the Management Center.

## Technical Design

### CP Subsystem Metrics

Some of the CP Subsystem metrics are registered statically, such as;

- Number of local Raft nodes
- Number of active CP groups
- Number of destroyed CP groups
- Number of active CP members 
- Number of missing CP members

Prefixes are `raft.` and `raft.metadata.` for these metrics.

```                          
Metric[[unit=count,metric=raft.nodes]=4]        
Metric[[unit=count,metric=raft.metadata.groups]=4]          
Metric[[unit=count,metric=raft.destroyedGroupIds]=0]      
Metric[[unit=count,metric=raft.metadata.activeMembers]=3]
Metric[[unit=count,metric=raft.missingMembers]=0]
Metric[[unit=count,metric=raft.metadata.activeMembersCommitIndex]=4]
```

Details of active CP groups (Raft nodes) are published dynamically using `DynamicMetricsProvider` abstraction. Metrics 
related to Raft nodes include;

- Raft group member count
- Raft term
- Raft commit index 
- Raft last applied index
- Raft last log term
- Raft last log index
- Raft snapshot index
- Available log capacity

`raft.group.` is used as metric name prefix. Discriminator of these metrics will be ID of the CP group (`groupId`) 
and group name (`name`) will be added as a tag. Additionally Raft role of the local node will be added as another tag (`role`), 
since we cannot publish non-numeric values as a metric yet.   

```
Metric[[groupId=0,unit=count,metric=raft.group.memberCount,name=METADATA,role=FOLLOWER]=3]
Metric[[groupId=0,unit=count,metric=raft.group.term,name=METADATA,role=FOLLOWER]=1]
Metric[[groupId=0,unit=count,metric=raft.group.commitIndex,name=METADATA,role=FOLLOWER]=7]
Metric[[groupId=0,unit=count,metric=raft.group.lastApplied,name=METADATA,role=FOLLOWER]=7]
Metric[[groupId=0,unit=count,metric=raft.group.lastLogTerm,name=METADATA,role=FOLLOWER]=1]
Metric[[groupId=0,unit=count,metric=raft.group.snapshotIndex,name=METADATA,role=FOLLOWER]=0]
Metric[[groupId=0,unit=count,metric=raft.group.lastLogIndex,name=METADATA,role=FOLLOWER]=7]
Metric[[groupId=0,unit=count,metric=raft.group.availableLogCapacity,name=METADATA,role=FOLLOWER]=11093]
```    

```
Metric[[groupId=2591,unit=count,metric=raft.group.memberCount,name=istanbul,role=LEADER]=3]
Metric[[groupId=2591,unit=count,metric=raft.group.term,name=istanbul,role=LEADER]=1]
Metric[[groupId=2591,unit=count,metric=raft.group.commitIndex,name=istanbul,role=LEADER]=2]
Metric[[groupId=2591,unit=count,metric=raft.group.lastApplied,name=istanbul,role=LEADER]=2]
Metric[[groupId=2591,unit=count,metric=raft.group.lastLogTerm,name=istanbul,role=LEADER]=1]
Metric[[groupId=2591,unit=count,metric=raft.group.snapshotIndex,name=istanbul,role=LEADER]=0]
Metric[[groupId=2591,unit=count,metric=raft.group.lastLogIndex,name=istanbul,role=LEADER]=2]
Metric[[groupId=2591,unit=count,metric=raft.group.availableLogCapacity,name=istanbul,role=LEADER]=11098]
```

Normally we cannot read Raft node data outside of the specific partition thread. To be able to access these Raft node 
metrics, a task named `PublishNodeMetricsTask` is scheduled periodically. `PublishNodeMetricsTask` iterates over local
Raft nodes and submits a `Runnable` to each Raft node. This `Runnable` task reads local Raft state and publishes 
an immutable `RaftNodeMetrics` object which is a snapshot of Raft state metrics at a moment. 
When `RaftService.provideDynamicMetrics` method is called, it collects the published `RaftNodeMetrics`.  

### CP Data Structure Metrics

All CP data structure services will implement `DynamicMetricsProvider` interface. Since CP data are only written/read by
the specific partition thread of the CP group, some fields of the CP data structure classes are marked as `volatile`.


#### IAtomicLong

`IAtomicLong` has only single metric: _current long value_. Metric prefix will be `cp.atomiclong.`, metric discriminator
will be in form of `id=name@groupId`. Also there will be `name` and `group` tags to show `IAtomicLong`'s name 
and CP group's name. `IAtomicLong`'s `value` field will be marked as `volatile` to be able to read from metrics collector 
thread.   

```
Metric[[id=total@default,metric=cp.atomiclong.value,name=total,group=default]=123]

Metric[[id=number@istanbul,metric=cp.atomiclong.value,name=number,group=istanbul]=34]

Metric[[id=number@ankara,metric=cp.atomiclong.value,name=number,group=ankara]=6]
```

#### IAtomicReference

`IAtomicReference` has no metrics to publish apart from its name. That's why a dummy metric with value equal to `0` will 
be published. Metric prefix will be `cp.atomicref.`, metric discriminator will be in form of `id=name@groupId`. 
There will be `name` and `group` tags to show `IAtomicReference`'s name and CP group's name. 

```
Metric[[id=ref@default,metric=cp.atomicref.dummy,name=ref,group=default]=0]

Metric[[id=ref@ankara,metric=cp.atomicref.dummy,name=ref,group=ankara]=0]

Metric[[id=object@default,metric=cp.atomicref.dummy,name=object,group=default]=0]
``` 

#### FencedLock

`FencedLock` has two fixed metrics; `lockCount` and `acquireLimit`. `lockCount` shows how many times the lock acquired 
and `acquireLimit` show the max number of reentrant acquires. 

Also if lock is held by an endpoint, there'll be two additional metrics; `ownerSessionId` and `owner`. `ownerSessionId` 
shows the sessionId of the lock owner. Session details can be observed with session metrics explained below. `owner` 
metric shows the address of the lock owner with a tag and will always have value `0`.   

Metric prefix will be `cp.lock.`, metric discriminator will be in form of `id=name@groupId`. 
There will be `name` and `group` tags to show `FencedLock`'s name and CP group's name.

Similarly `lockCount` and `owner` fields of the `Lock` class are marked as `volatile`. 

```                                                                                   
# FencedLock: not acquired state 
Metric[[id=blacklock@default,unit=count,metric=cp.lock.acquireLimit,name=blacklock,group=default]=2147483647]
Metric[[id=blacklock@default,unit=count,metric=cp.lock.lockCount,name=blacklock,group=default]=0]
                                
# FencedLock: acquired by "[127.0.0.1]:5703]"
Metric[[id=redlock@default,unit=count,metric=cp.lock.acquireLimit,name=redlock,group=default]=2147483647]
Metric[[id=redlock@default,unit=count,metric=cp.lock.lockCount,name=redlock,group=default]=2]
Metric[[id=redlock@default,metric=cp.lock.ownerSessionId,name=redlock,group=default]=1]
Metric[[id=redlock@default,metric=cp.lock.owner,name=redlock,group=default,owner=[127.0.0.1]:5703]=0]
```

#### ISemaphore

`ISemaphore` has two metrics; `initialized` and `available`. `initialized` is normally a boolean state, which shows
whether semaphore is initialized with a value or not. In exposed metrics it will be `0` when semaphore is not initialized,
a positive value otherwise. `available` shows number of the remaining permits. 

Metric prefix will be `cp.semaphore.`, metric discriminator will be in form of `id=name@groupId`. 
There will be `name` and `group` tags to show `Semaphore`'s name and CP group's name.

`initialized` and `available` fields of the `Semaphore` class are marked as `volatile`.

```
Metric[[id=semaz@default,metric=cp.semaphore.initialized,name=semaz,group=default]=0]
Metric[[id=semaz@default,unit=count,metric=cp.semaphore.available,name=semaz,group=default]=0] 

Metric[[id=semax@default,metric=cp.semaphore.initialized,name=semax,group=default]=1]
Metric[[id=semax@default,unit=count,metric=cp.semaphore.available,name=semax,group=default]=89]
```                         


#### ICountDownLatch

`ICountDownLatch` has three metrics; `round`, `count` and `remaining`. `round` shows the round number of the `ICountDownLatch`,
each time `ICountDownLatch` is initialized with a new count after it downs to zero, a new round begins. 
`count` shows the initial count of the `ICountDownLatch` and `remaining` shows the remaining number of expected count-downs. 

Metric prefix will be `cp.countdownlatch.`, metric discriminator will be in form of `id=name@groupId`. 
There will be `name` and `group` tags to show `Semaphore`'s name and CP group's name.

`round` and `countDownFrom` fields of the `CountDownLatch` class are marked as `volatile`, also a new transient & volatile
`remaining` field is added to keep track of remaining counts. Before number of count-downs was calculated by the size
of a `HashSet` containing UUIDs of counting down endpoints. 

```
Metric[[id=controlLatch@default,metric=cp.countdownlatch.round,name=controlLatch,group=default]=1]
Metric[[id=controlLatch@default,unit=count,metric=cp.countdownlatch.count,name=controlLatch,group=default]=13]
Metric[[id=controlLatch@default,unit=count,metric=cp.countdownlatch.remaining,name=controlLatch,group=default]=11]
```

### CP Session Metrics

Alongside with CP data structures, we will expose CP session metrics too. Each CP session has 5 metrics; `endpoint`,
`endpointType`, `version`, `creationTime` and `expirationTime`. 

`endpoint` and `endpointType` are string metrics, that's why their values are always `0` and they will have `endpoint` 
and `endpointType` tags to show the address of the endpoint which session belongs to and type of the endpoint; either 
`SERVER` or `CLIENT`. 

`version` is the version number of the session, basically it shows how many times a session heartbeat is received. 
`creationTime` and `expirationTime` are timestamp metrics in millis, they show creation-time and expiration-time of
the session as their names suggest.

Additionally a `sessionId` tag will be available on all session metrics. Metric prefix will be `cp.session.`, metric 
discriminator will be in form of `id=sessionId@groupId`. There will be also a `group` tag to show CP group's name.   

```
controlLatchMetric[[id=1@default,metric=cp.session.endpoint,sessionId=1,group=default,endpoint=[127.0.0.1]:5703]=0]
controlLatchMetric[[id=1@default,metric=cp.session.endpointType,sessionId=1,group=default,endpointType=SERVER]=0]
controlLatchMetric[[id=1@default,metric=cp.session.version,sessionId=1,group=default]=11]
controlLatchMetric[[id=1@default,unit=ms,metric=cp.session.creationTime,sessionId=1,group=default]=1597399003264]
controlLatchMetric[[id=1@default,unit=ms,metric=cp.session.expirationTime,sessionId=1,group=default]=1597399328271]
```
