

# Common Exception Types

You may see the following exceptions in any Hazelcast operation when the following situations occur:

- `HazelcastInstanceNotActiveException`: Thrown when `HazelcastInstance` is not active (already shutdown or being shutdown) during an invocation. 

- `HazelcastOverloadException`: Thrown when the system will not handle more load due to an overload. This exception is thrown when back pressure is enabled.

- `DistributedObjectDestroyedException`: Thrown when an already destroyed `DistributedObject` (IMap, IQueue, etc.) is accessed and when a method call is done over a destroyed object.

- `MemberLeftException`: Thrown when a member leaves during an invocation or execution.

Hazelcast also throws the following exceptions in the cases of overall system problems such as networking issues and long pauses:

- `PartitionMigratingException`: Thrown when an operation is executed on a partition, but that partition is currently being moved around.

- `TargetNotMemberException`: Thrown when an operation is sent to a machine that is not a member of the cluster.

- `CallerNotMemberException`: Thrown when an operation was sent by a machine which is not a member in the cluster when the operation is executed.

- `WrongTargetException`: Thrown when an operation is executed on the wrong machine, mostly because the partition that operation belongs to is already moved to some other member.





