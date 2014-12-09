
# Back Pressure

Hazelcast 3.4 provides back pressure for synchronous calls with asynchronous backups; for example a map configured with a single
asynchronous backup and where a put is executed.

Without back pressure it can happen that the system produces new asynchronous backups faster than they are processed and this can
eventually lead to problems like OOME. Back pressure is disabled by default and can be configured using property
'hazelcast.backpressure.enabled'. There is no overhead for calls without backups and calls with synchronous backups and will therefor
not have a performance impact.

Currently back pressure is implemented by transforming a asynchronous backup to a synchronous backup. This prevents the accumulation
of asynchronous backup operations since the caller needs to wait for the queue's to drain. By default there is a sync window of 100
which can be configured using property 'hazelcast.backpressure.syncwindow'. Each member tracks for each connection and each partition
the sync-delay and decrements this for every asynchronous backups. Once the sync-delay reaches null, we make the call sync and calculate
a new sync delay using '0.75 * sync-window + random(0.5*sync-window)'. So for a sync-window of 100, the sync-delay will be between
75 and 125. We use a random to prevent resonance.

## Future improvements

In Hazelcast 3.4.1 the same technique is going to be applied for regular async-calls.

In the future we'll add a TCP/IP based congestion control based back pressure. But this requires quite a few changes since we need
to use multiple channels because we can't simply not consume the TCP/IP buffer because a single channel is used for regular operations,
responses and system operations like heart-beats.



