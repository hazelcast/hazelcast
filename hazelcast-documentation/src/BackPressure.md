
## Back Pressure

Hazelcast 3.4 provides the back pressure feature for synchronous calls with asynchronous backups. Using this feature, you can prevent the overload caused by pending asynchronous backups. 

An example scenario is a map, configured with a single
asynchronous backup and where a put operation is executed. Without back pressure, the system may produce the new asynchronous backups faster than they are processed, i.e. producing asynchronous backups may happen at a higher rate than the consumption of the backups. This can
lead to problems like out of memory exceptions. When you use the back pressure feature, you can prevent these problems from occurring.

Back pressure is disabled by default. You can enable it by setting the system property
`hazelcast.backpressure.enabled` to `true`. There is no overhead for the calls without backups or the calls with synchronous backups. Therefore, back pressure does 
not have an impact on the performance for these calls (the ones without backups or with synchronous backups).

Currently, back pressure is implemented by transforming an asynchronous backup operation to a synchronous one. This prevents the accumulation
of the asynchronous backup operations since the caller needs to wait for the queue to drain. 

By default, the value of the sync window is `100`. This means, one call out of 100 calls will be transformed to a synchronous call. You can configure the sync window
using the system property `hazelcast.backpressure.syncwindow`. Each member tracks  each connection and each partition for their
sync-delay and decrements this delay for every asynchronous backups. Once the sync-delay reaches null, the call is transformed into a synchronous call and a new sync-delay is calculated using the formula *0.75 * sync-window + random(0.5 \* sync-window)*. For a sync window with the value 100, the sync-delay will be between
75 and 125. This randomness is to prevent resonance.

### Future improvements

In Hazelcast 3.4.1, the same technique is going to be applied for regular asynchronous calls.

In the future, a back pressure based on a TCP/IP based congestion control will be added. This requires changes since we need
to use multiple channels. The TCP/IP buffer cannot be consumed because a single channel is used for regular operations,
responses and system operations like heartbeats.



