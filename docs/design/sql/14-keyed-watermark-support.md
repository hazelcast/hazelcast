# Keyed WM support

|||
|---|---|
|Related Jira|[HZ-986](https://hazelcast.atlassian.net/browse/HZ-986)|
|Document Status / Completeness|DRAFT|
|Requirement owner|Sandeep Akhouri|
|Developer(s)|Sasha Syrotenko|
|Quality Engineer|Bartlomiej Poplawski|
|Technical Reviewers|Viliam Durina|

### Background

For stream-to-stream join we need to be able to support multiple WMs (WM). For
the explanation why it's needed, see [15-stream-to-stream-join.md]().

This document is about changes in Jet engine needed to support multiple WMs.

#### Goals

- define the behavior of WM coalescing for keyed WMs
- define default behavior for processors that don't deal with WMs (implemented
  in Jet engine)
- define changes needed to processors that deal with WMs

Non-goal is to discuss stream-to-stream join.

### Terminology

In Jet we used the term _watermark_ for two things:
- a broadcast stream item carrying a value
- the set of all watermarks in the stream

In this document we'll use the term _watermark instance_ for a stream item
carrying the value, and the term _watermark_ (WM) only for the set of all
watermarks in a stream. If we say there are _two watermarks in the stream_, it
means there are two independent sets of watermarks, differentiated by a key.

### Description of current behavior

A WM with value `M` means that any event with value less than `M` can be
considered _late_ and be ignored. It's very important in stream processing as it
allows the processors to reason about possible future events and to remove data
from state.

So-called _WM coalescing_ happens when there are two inputs that are
merged into one stream - the minimum of WMs received from each input is
forwarded.

#### Example

We show only WM items on the streams, as the events are irrelevant to
coalescing. `in: wm(N, M)` denotes an input WM from input `N` with a value of
`M`. `out: wm(M)` denotes an output WM. All output WMs go to all output streams,
therefore we don't specify the output number in the example.

```
in: wm(0, 10)
in: wm(1, 12)
out: wm(10)
in: wm(0, 11)
out: wm(11)
in: wm(1, 13)
## no new output now as the minimum is still 11
in: wm(0, 14)
out: wm(13)
```

#### Idle message

An input can contain a special `IDLE_MESSAGE` that excludes the input stream on
which it appeared from coalescing. When an `IDLE_MESAGE` is received from an
input, that input is excluded from coalescing and the WM is output based on the
minimum of the remaining non-idle inputs. Any new WM or event from that input
clears the idle status and includes that input into coalescing again.

The `IDLE_MESSAGE` is implemented as a WM with a `Long.MAX_VALUE` value, but
it's not really a WM, we just piggyback on the WM broadcast mechanism.

#### WM generation

Any processor can generate watermarks based on any logic. The processor can:
- generate new WMs
- forward WMs from input
- drop all WMs from input
- anything else

These are the typical rules:

- the source emits WMs based on lag after the newest item it observed
 
- if the source emits from multiple remote partitions, it coalesces the WM from
  each partition

- non-source processors forward input watermarks

- processors that delay input items, also delay watermarks by the same amount
  (e.g. async map processor or window aggregation processor)

But WMs must follow certain rules:

#### WM rules

1. **Monotonicity** (strict rule): a WM can never go back. For efficiency, we
   require a strict monotonicity, because it's a waste of resources to emit a WM
   with the same value twice. Violation will cause a job failure.

2. **No overtaking** (conventional rule): an input item that is not late, should
   not be late on the processor output

3. **No unnecessary latency** (conventional rule): The processor should emit
   the WM as soon as it knows that no item that would be late will follow.

Only the 1st rule is a requirement. The other are conventions and the processors
can break them. `AbstractProcessor` implements them for simple processors that
forward WMs immediately.

### Extending the behavior for multiple keys

#### `Watermark` class

We'll add a new `byte key` field to the `Watermark` class. Two WM instances with a
different key are handled independently.

#### Processor API

Currently the `Processor` has this method:

```java
boolean tryProcessWatermark(@Nonnull Watermark watermark);
```

No change is needed as the argument includes the key. No change is needed in
`AbstractProcessor`, this class directly emits each received WM. It's sufficient
also for the join processor, as it can tell apart watermarks from different
inputs by looking at the key.

Even though the method signature didn't change, it's not fully b-w compatible. A
processor that didn't simply forward every received WM will now need to take the
key into account, otherwise it could observe non-monotonic WMs and fail.

#### Coalescing of WMs with different keys

Such WMs are coalesced completely independently. A wm with key=K is coalesced
with wm with the same key from other inputs independently, as if it was two
separate streams.

#### Changes to union/merge processor

There's no union processor in Jet, we use `map(identity())` with multiple inputs
to do it. This setup assumes that all inputs have the same WM, and the same WM
key is related to the same field in the items. If some WM key is only on one
input, but not on others, such WM won't be forwarded at all, because coalescing
will wait forever for a WM with the same key from the other side.

So for correct functionality the user has to properly choose WM key and use the
same key if it's the same field and a different key, if it's not.   

#### Changes to async-transform processor

##### Ordered

The processor currently uses a queue to which it adds both events and
watermarks, as they are received. This preserves the original order of events
w.r.t. watermarks, so no change is needed.

##### Unordered

This processor emits responses to async operations in the order the responses
arrive. To handle watermarks, it tracks counts of events received between each
watermark, and for each event with a pending response it stores after which WM
it was received. When a response arrives, it decrements the count. When the
count reaches zero, it checks the watermark counts from oldest to newest, and
emits the watermark up to the first non-zero count.

This processor must be modified to take the WM key into account. Instead of
storing just the WM value for each event, we need to store a tuple `{wmKey,
wmValue}`, or the whole `Watermark` instance which contains exactly these two
fields. Instead of a `TreeMap` where the key was a WM value we should use a
`List` or a `Deque`, because `Watermark` instances with a key don't have an
inherent order, we need to use the reception order.

#### Changes to sliding window processor

This processor forwards the input watermarks, but before doing so, it emits
windows for which the all events were received.

The processor receives a list of timestamp functions, one for each input ordinal.
It extracts the timestamp from the events using these functions, and uses them
to assign windows.

For example, consider a stream with two watermarked timestamps: `time1` and
`time2`. Then this query:

```sql
select 
  window_start window_start_inner, 
  window_end window_end_inner,
  time2,
  count(*)
from tumble(my_stream, descriptor(time1), 5 seconds)
group by 1, 2, 3
```

would be a valid one. It groups by windows calculated using `time1`, but also
groups by `time2`, which should be watermarked on output.

As we can see, the processor can group only by a single watermarked value.
Another watermarked value can be a part of the grouping key. If it's not a part
of the grouping key, then the value isn't on output and the watermark can be
dropped.

To implement it, after emitting a window the processor will have to iterate all
in-progress windows and their keys and find the minimum and emit the minimum of
those and of the input WM for that key as the output WM. After items are removed
from the set, to calculate the minimum the remaining items have to be iterated,
so this will be a lot of extra work. This is the reason why we will not
implement this. It's very rare to group by a watermarked column. The sliding
window processor will drop all input watermarks, the output will have watermarks
only on the window bounds.




A stream item typically carries many values. A watermark is tied to one
particular field. It makes sense to have multiple WMs, tied to different fields.

Watermark instances with a different key is coalesced independently. Processors
delaying the watermark must be modified to ensure that the rules are not broken
for any watermark.

As an example we'll discuss a processor that emits the input items without a
change, but after a delay.

```
in: event(t1=100, t2=110)
# event stored in buffer for later emission
in: wm(key=t1, v=102)
# watermark instance cannot be forwarded before the event from the buffer is
# forwarded. But we can forward an older instance, because the processor can
# emit only what's in the buffer, or what it can further receive.
out: wm(key=t1, v=100)
in: 
```