
### Breakage of the Map-Contract

ReplicatedMap does offer a distributed `java.util.Map::clear` implementation, but due to the asynchronous nature and the
weakly consistency of it, there is no point in time where you can say the map is empty. Every node on itself
applies it to its local dataset in "a near point in time".
If you need a definite point in time to empty the map, you may want to consider using a lock around the `clear` operation.

There are ways to simulate this method by locking your user codebase and executing a remote operation that will
utilize `DistributedObject::destroy` to destroy the node's own proxy and storage of the ReplicatedMap. A new proxy instance
and storage will be created on next retrieval of the ReplicatedMap using `HazelcastInstance::getReplicatedMap`.
That means, you have to reallocate the ReplicatedMap in your code. Afterwards, just release the lock when finished.
