
## Back Pressure

Hazelcast 3.4 provides the back pressure feature for synchronous calls with asynchronous backups. Using the back pressure feature, you can prevent the overload caused by pending asynchronous backups. 

An example scenario is a map that is configured with a single
asynchronous backup, and a `put` operation is executed on the map. Without back pressure, the system may produce the new asynchronous backups faster than they are processed; asynchronous backups may be produced at a higher rate than the consumption of the backups. This can
lead to problems like out of memory exceptions. When you use the back pressure feature, you can prevent these problems from occurring.

Back pressure is disabled by default. To enable it, set the system property
`hazelcast.backpressure.enabled` to `true`. There is no overhead for calls without backups or calls with synchronous backups. Therefore, back pressure does not impact performance for those calls.

Currently, back pressure is implemented by transforming an asynchronous backup operation to a synchronous one. This prevents the accumulation
of the asynchronous backup operations since the caller needs to wait for the queue to drain.

By default, the value of the sync window is 100. This means that 1 call out of 100 calls will be transformed to a synchronous call. You can configure the sync window
using the system property `hazelcast.backpressure.syncwindow`. Each member tracks each connection and each partition for their
sync-delay, and each member decrements this delay for every asynchronous backup. Once the sync-delay reaches null, the call is transformed into a synchronous call and a new sync-delay is calculated using the formula *0.75 * sync-window + random(0.5 \* sync-window)*. For a sync window with the value 100, the sync-delay will be between
75 and 125. This randomness prevents resonance.

### Future Improvements

In Hazelcast 3.4.1, the same technique will be applied for regular asynchronous calls.

In the future, a back pressure based on a TCP/IP based congestion control will be added. This requires changes since we need
to use multiple channels. The TCP/IP buffer cannot be consumed because a single channel is used for regular operations,
responses and system operations like heartbeats.



