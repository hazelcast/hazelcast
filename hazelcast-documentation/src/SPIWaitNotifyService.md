
## WaitNotifyService

`WaitNotifyService` is an interface offered by SPI for the objects (e.g. Lock, Semaphore) to be used when a thread needs to wait for a lock to be released.

This service keeps a list of waiters. For each notify operation;

- it looks for a waiter,
- asks the waiter whether it wants to keep waiting,
- if the waiter responds *no*, the service executes its registered operation (operation itself knows where to send a response),
- rinses and repeats until a waiter wants to keep waiting.


Each waiter can sit on wait-notify queue at most its operation's call timeout. For example, by default, each waiter can wait here for at most 1 minute. There is a continuous task that scans expired/timed-out waiters and invalidates them with `CallTimeoutException`. Each waiter on the remote side should retry and keep waiting if it still wants to wait. This is a liveness check for remote waiters. 

This way, it is possible to distinguish an unresponsive node and a long (~infinite) wait. On the caller side, if waiting thread does not get a response for either a call timeout or for more than *2 times the call-timeout*, it will exit with `OperationTimeoutException`.  

As can be noticed, this behavior breaks the fairness. Hazelcast does not support fairness for any of the data structures with blocking operations (i.e. lock and semaphore).