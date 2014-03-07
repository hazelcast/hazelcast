
## Hazelcast MapReduce Architecture

### Node Interoperation Example

To understand the following technical internals we first will have a short look at what happens in terms
of an example workflow.

So due to the above example code we have string keys and integer values and the resulting emitted values
are the same. Imagine you have a three node cluster and initiate the map reduce job on the first node. After
you requested the JobTracker from your running / connected Hazelcast we submit the task and retrieve the 
ICompletableFuture which gives us a chance of waiting for the result to be calculated or adding a callback 
to go a more reactive way.

The example expects that the chunk size is 0 or 1 so an emitted value is directly send to the reducers.
Internally the job is prepared, started and executed on all nodes as shown below whereas the first node
acts as the job owner (job emitter):

```plain
Node1 starts map reduce job
Node1 emits key=Foo, value=1
Node1 does PartitionService::getKeyOwner(Foo) => results in Node3
 
Node2 emits key=Foo, value=14
Node2 asks jobOwner (Node1) for keyOwner of Foo => results in Node3
 
Node1 sends chunk for key=Foo to Node3
 
Node3 receives chunk for key=Foo and looks if there is already a Reducer,
      if not creates one for key=Foo
Node3 processes chunk for key=Foo
 
Node2 sends chunk for key=Foo to Node3
 
Node3 receives chunk for key=Foo and looks if there is already a Reducer and uses
      the previous one
Node3 processes chunk for key=Foo
 
Node1 send LastChunk information to Node3 because processing local values finished
 
Node2 emits key=Foo, value=27
Node2 has cached keyOwner of Foo => results in Node3
Node2 sends chunk for key=Foo to Node3
 
Node3 receives chunk for key=Foo and looks if there is already a Reducer and uses
      the previous one
Node3 processes chunk for key=Foo
 
Node2 send LastChunk information to Node3 because processing local values finished
 
Node3 finishes reducing for key=Foo 
 
Node1 registers its local partitions are processed
Node2 registers its local partitions are processed
 
Node1 sees all partitions processed and requests reducing from all nodes
 
Node1 merges all reduced results together in a final structure and returns it
```

As you can see the flow is quite complex but extremely powerful since everything is executed in
parallel. Reducers do not wait until all values are emitted but immediately begin to reduce (when
first chunk for an emitted key arrives).

### Internal Architecture

Beginning with the package level there is one basic package com.hazelcast.mapreduce. This includes
the external API and the impl package which itself contains the internal implementation.

 - The impl package contains all the default KeyValueSource implementations and abstract base and support classes for exposed API
 - The client package contains all classes that are needed on client and server (node) side when a map reduce job is offered from a client
 - The notification package contains all "notification" or event classes that are used to notify other members about progress on operations
 - The operation package contains all operations that are used by the workers or job owner to coordinate work and sync partition or reducer processing
 - The task package contains all classes that execute the actual map reduce operation. It features the supervisor, mapping phase implementation and mapping and reducing tasks

And now to the technical walk-through.
As stated above a map reduce Job is always retrieved from a named JobTracker which in case is
implemented in NodeJobTracker (extends AbstractJobTracker) and is configured using the configuration
DSL. All of the internal implementation is completely ICompletableFuture driven and mostly non
blocking in design.

On submit the Job creates a unique UUID which afterwards acts as a jobId and is combined with the
JobTrackers name to be uniquely identifiable inside the cluster. Then the preparation is send around
the cluster and every member prepares it's execution by creating a creating a JobSupervisor, a
MapCombineTask and a ReducerTask. The job emitting JobSupervisor gains special capabilities to
synchronize and control JobSupervisors on other nodes for the same job.

If preparation finished on all nodes the job itself is started by executing a StartProcessingJobOperation
on every node. This initiates a MappingPhase implementation (defaults to KeyValueSourceMappingPhase) and
starts the actual mapping on the nodes.

The mapping process is currently a single threaded operation per node but will be extended to run in
parallel on multiple partitions (configurable per Job) in future versions. The Mapper is now called on
every available value on the partition and eventually emits values. For every emitted value a maybe
configured CombinerFactory is called to created a Combiner or a cached one is used (or the default
CollectingCombinerFactory is used to create Combiners). When the chunk limit is reached on a node a
IntermediateChunkNotification is prepared by collecting emitted keys to their corresponding nodes. This
is either done by asking the job owner to assign members or by an already cached assignment. In later
versions a PartitionStrategy might be configurable, too.

The IntermediateChunkNotification is then send to the reducers (containing only values for this node)
and is offered to the ReducerTask. On every offer the ReducerTask checks if it is already running and
if not it submits itself to the configured ExecutorService (from the JobTracker configuration).

If reducer queue runs out of work the ReducerTask is removed from the ExecutorService to not block threads
but eventually will be resubmitted on next chunk of work.

On every phase change the partition state is changed to keep track of the currently running operations.
A JobPartitionState can be in one of the following states `[WAITING, MAPPING, REDUCING, PROCESSED, CANCELLED]`
where I think the names are fine without any further definition. On deeper interest of the states look at
the Javadoc.

- Node asks for new partition to process: WAITING => MAPPING
- Node emits first chunk to a reducer: MAPPING => REDUCING
- All nodes signaled they finished mapping phase and reducing is finished, too: REDUCING => PROCESSED

Eventually (or hopefully) all JobPartitionStates are reached the state PROCESSED and the job emitters
JobSupervisor asks all nodes for their reduced results and executes a potentially offered Collator to
calculate the overall result before removing itself from the JobTracker, doing some final cleanup and
returning the result to the requester (using the internal TrackableJobFuture).

If a job is canceled while execution all partitions are immediately set to CANCELLED state and a
CancelJobSupervisorOperation is executed on all nodes to kill the running processes.

While the operation is running in addition to the default operations some more are like
ProcessStatsUpdateOperation (updates processed records statistics) or NotifyRemoteExceptionOperation
(notifies the nodes that the sending node encountered an unrecoverable situation and the Job needs to
be canceled - e.g. NullPointerException inside of a Mapper) are executed against the job owner to keep
track of the process.

