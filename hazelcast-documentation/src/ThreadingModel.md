## Threading Model

Your application server has its own threads. Hazelcast does not use these - it manages its own threads.

### I/O Threading

Hazelcast uses a pool of threads for I/O. A single thread does not do all the IO: instead, multiple threads do the IO. On each cluster member, the IO-threading is split up in 3 types of IO-threads:

* IO-thread that takes care of accept requests,
* IO-threads that take care of reading data from other members/clients,
* IO-threads that take care of writing data to other members/clients.

You can configure the number of IO-threads using the `hazelcast.io.thread.count` system property. Its default value is 3 per member. 
This means that if 3 is used, in total there are 7 IO-threads; 1 accept-IO-thread, 3 read-IO-threads, and 3 write-IO-threads. Each IO-thread has its own Selector instance and waits on `Selector.select` if there is nothing to do.

In case of the read-IO-thread, when sufficient bytes for a packet have been received, the Packet object is created. This Packet is 
then sent to the System where it is de-multiplexed. If the Packet header signals that it is an operation/response, the Packet is handed 
over to the operation service (please see [Operation Threading](#operation-threading)). If the Packet is an event, it is handed 
over to the event service (please see [Event Threading](#event-threading)). 

