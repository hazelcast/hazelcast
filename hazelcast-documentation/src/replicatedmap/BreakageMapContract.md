
### Breakage of the Map-Contract

The ReplicatedMap does not offer a distributed `java.util.Map::clear` implementation since due to the asynchronous
behaviour there is no way to guarantee a consistent state at the end of the operation. Instead it will throw an
`java.lang.UnsupportedOperationException`.

Anyways there are ways to simulate this method by locking your user codebase and executing a remote operation that will
utilize `DistributedObject::destroy` to destroy the nodes own proxy and storage of the ReplicatedMap. A new proxy instance
and storage will be created on next retrieval of the ReplicatedMap using `HazelcastInstance::getReplicatedMap`.
That means you have to reallocate the ReplicatedMap in your code. Afterwards just releasing the lock when finished.
