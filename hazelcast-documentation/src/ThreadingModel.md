


## Threading Model

Your application server will have its own threads.

Hazelcast manages its own threads.

It will have:

* ExecutorService threads
* NIO Threads
* Partition Threads

Because execution can happen with ExecutorService on the Members, it is important to lock data.

A partition on a member only has one Partition Thread. Operations on one partition are queued.

A Member will be using a thread pool, so partition work may have to wait for
