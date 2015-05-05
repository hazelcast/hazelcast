## Back Pressure

Hazelcast uses operations to make remote calls. For example, a `map.get` is an operation and a `map.put` is one operation for the primary 
and one operation for each of the backups, i.e. `map.put` is executed for the primary and also for each backup. In most cases, there will be a natural balance between the number of threads performing operations
and the number of operations being executed. However, there are two situations where this balance and operations 
can pile up and eventually lead to Out of Memory Exception (OOME):

- Asynchronous calls: With async calls, the system may be flooded with the requests.

- Asynchronous backups: The asynchronous backups may be piling up.

To prevent the system from crashing, Hazelcast provides back pressure. Back pressure works by:
 
- limiting the number of concurrent operation invocations, 

- periodically making an async backup sync.

Back pressure is disabled by default and you can enable it using the following system property:

`hazelcast.backpressure.enabled`

To control the number of concurrent invocations, you can configure the number of invocations allowed per partition using the 
following system property:

`hazelcast.backpressure.max.concurrent.invocations.per.partition`

This system property's default value is 100 and using a default configuration, a system is allowed to have (271+1)*100=27200 concurrent invocations.
 
Back pressure is only applied to normal operations. System operations like heart beats and partition migration operations 
are not influenced by back pressure. 27200 invocations might seem like a lot, but keep in mind that executing a task on `IExecutor` 
or acquiring a lock also requires an operation.

If the maximum number of invocations has been reached, Hazelcast will automatically apply an exponential back off policy. This
gives the system some time to deal with the load. Using the following system property, you can configure the maximum time to wait before a `HazelcastOverloadException` is thrown:

`hazelcast.backpressure.backoff.timeout.millis`

This system property's default value is 60000 ms.

The Health Monitor keeps an eye on the usage of the invocations. If it sees a member has consumed 70% or more of the invocations, it starts to log health messages.

Apart from controlling the number of invocations, you also need to control the number of pending async backups. This is done
by periodically making these backups sync instead of async. This forces all pending backups to get drained. For this, Hazelcast tracks the number of asynchronous backups for each partition. At every **Nth** call, one synchronization is forced. This **N** is 
controlled through the following property:

`hazelcast.backpressure.syncwindow`

This system property's default value is 100. It means, out of 100 *asynchronous* backups, Hazelcast makes 1 of them a *synchronous* one. A randomization is added; the sync window with default configuration (100) will be between 75 and 125 
invocations. 

***RELATED INFORMATION***

*Please refer to the [System Properties section](#system-properties) to learn how to configure the system properties.*

<br> </br>

