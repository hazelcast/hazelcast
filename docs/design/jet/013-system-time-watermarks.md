---
title: 013 - Resolving Sparse Events Issue with System-Time Watermarks
description: Make the time progress independently from the event rate
---

*Since*: 4.3

## The _Sparse Events Issue_

When using event time, time progresses only through the ingestion of new
events. If the events are sparse, time will effectively stop until a
newer event arrives. This causes high latency for time-sensitive
operations such as window aggregation. In addition, Jet tracks event
time for every source partition separately, and if just one partition
has sparse events, time progress in the whole job is hindered.

## Ingestion Time

When you use `withIngestionTime()` on the pipeline, Jet won't extract
the timestamp from the event, but will set it according to the local
system time on the server. This way of assigning timestamps is inferior
because the event isn't associated with the time at which it occurred,
but at which Jet received it, causing inaccuracy in the calculations. A
specifically bad problem is that, if the job restarts due to a crash,
Jet must receive again some of the events it already received and
processed before the crash. These events will now get different
timestamps, affecting a different piece of the aggregation state.

On the other hand, this timestamping approach provides the best latency:
the job will never wait for delayed items, there's no need to allow any
event lag as with event-time processing.

## The Improvement

We first implemented `WatermarkPolicy.limitingRealTimeLag()`. This
policy emits a watermark that lags behind the system clock by a fixed
amount. This policy doesn't depend on the events processed, it
depends solely on the system clock. Currently, this policy is available
only in Core API.

Second, we made `StreamSourceStage.withIngestionTimestamps()` use this
policy with zero lag. We can do this because the events are assigned
with system time and we're sure that there will never be an event with
an older timestamp.

Further work is to allow the use of real-time watermarks in Pipeline API
even for event-time processing: this will solve the sparse events issue
and will give fixed latency relative to the real time, but in case the
job is down for more than the real time lag, some events will be
dropped as late.
