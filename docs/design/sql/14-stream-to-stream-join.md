# Stream to stream JOIN

### Table of Contents

+ [Background](#background)
    * [Goals](#goals)
+ [Functional Design](#functional-design)
    * [Summary of Functionality](#summary-of-functionality)
+ [Technical Design](#technical-design)
    + [Overall Design](#overall-design)
    + [JOIN processor design and algorithm description](#join-processor-design-and-algorithm-description)
    + [Watermarks](#watermarks)
    + [Memory management](#memory-management)
+ [Testing Criteria](#testing-criteria)

|||
|---|---|
|Related Jira|[HZ-986](https://hazelcast.atlassian.net/browse/HZ-986)|
|Document Status / Completeness|DRAFT|
|Requirement owner|Sandeep Akhouri|
|Developer(s)|Sasha Syrotenko|
|Quality Engineer|Bartlomiej Poplawski|
|Technical Reviewers|Viliam Durina|

### Background

In streaming data processing, there are several scenarios for joins:

- batch to stream: one source is batch (e.g. a table), and the other source is streaming.
- stream to stream: both data sources are streaming (e.g. messaging topics) that need to be joined.

#### Goals

Hazelcast already supports batch to stream joins. This work aims to introduce stream to stream joins. The main
goals are:

- SQL based / Non-Java friendly: easy to use format also for non-Java developers to use stream to stream joins
- Core API also available. Pipeline API nice to have.
- different join types (`INNER` , `LEFT`/`RIGHT` `OUTER` `JOIN`s).

The state of the join processor must be bounded (i.e. no support for stream-to-stream joins without a time bound)
and the latency must be minimal (i.e. emit the joined row as soon as the processor receives them.

### Functional Design

#### Summary of Functionality

Semantically, the join operation, as specified by SQL, is easy to apply to streams: The query should output all items
from the left input, joined with all items from the right input, which meet the join condition.

For example, have `orders` and `deliveries` stream events (note, `Stream` type does not exist, used just for example):

```sql
CREATE MAPPING orders (
    order_id BIGINT,
    order_time TIMESTAMP, 
    item_id BIGINT
 ) TYPE Stream;
 
CREATE MAPPING deliveries (
    delivery_id BIGINT,
    order_id BIGINT,
    delivery_time TIMESTAMP,
    address VARCHAR
) TYPE Stream;
```

Consider the following query:

```sql
 SELECT * 
 FROM orders_ordered AS o
 JOIN deliveries_ordered  AS d
     ON d.delivery_time BETWEEN o.order_time AND o.order_time + INTERVAL '1' HOUR
```

The `<table>_ordered` is a view on a containing the `IMPOSE_ORDER` function on top of `<table>`.

Example result set is shown in Table 1:

| order_id | order_time       | item_id | delivery_id | order_id | delivery_time    | address     |
|----------|------------------|---------|-------------|----------|------------------|-------------|
| 1        | 01.03.2022:10:00 | 100     | 1           | 1        | 01.03.2022:10:40 | 'address_1' |
| 2        | 01.03.2022:11:00 | 100     | 2           | 2        | 01.03.2022:11:20 | 'address_2' |
| ---      | ---              | ---     | ---         | ---      | ---              | ---         |

__Table 1__

### Technical Design

SQL engine should use a specialized Jet processor to perform JOIN operation for two input streams. The join condition
must constrain the required buffering time of events from both inputs; otherwise, the query will be rejected from 
execution because the engine would have to keep ever-growing state.

#### JOIN processor design and algorithm

##### Simple case for two inputs

In order to determine for how long the processor needs to buffer events from each side, the engine will extract
time bounds from the join condition in the following form:

```
inputLeft.time >= inputRight.time - constant
inputRight.time >= inputLeft.time - constant
```

We must be able to extract both of these conditions. We'll decompose
conjunctions in the join condition (the ANDed expressions). There must be at
least one such inequation for each input. If there are multiple inequations of
this for either input, we'll use the lowest constant. If we can't convert the
inequation into this form, we'll not use that part of the join condition for a
time bound. Obviously, we'll apply the whole condition after joining. The point
here is only to extract the part needed to bound the buffering time.

The "constant" is a time for which the processor has to buffer items from
`inputX`, after items from `inputY`. It's also the time by which we postpone the
watermark for respective input - that's why we call it "postpone time".

Example:
Consider having query:

```
SELECT * from input1 i1
JOIN input2 i2 ON i2.time BETWEEN i1.time - 1 AND i1.time + 4
```

Extracted conditions:

```
i1.time >= i2.time - 4
i2.time >= i1.time - 1
```

Based on these conditions, the processor will buffer events from `i1` for 4 time
units after the `i2` watermark. For example, when the watermark from `i2`
reaches the value `10`, a buffered event from `i1` with `i1.time = 5` will never
fulfill the join condition of `i1.time >= i2.time - 4` for any new non-late `i2`
event we can possibly receive, because `5` is not greater than or equal to `10 -
4`. Therefore, we can remove all events with `i1.time <= 5` from the buffer, and
emit the watermark with time=6 to the output.

The second condition allows the processor to remove items from the other buffer.

However, this scenario is not easy to extend to inputs with multiple timestamps,
let's look at that.

##### Generalizing the behavior for multiple watermarks

As you can see from the previous example, the output of the processor contains
two watermarks: one for `i1` and one for `i2`. If the output of this processor
is input to another join processor (e.g. in case of a join of three streams),
there will be two watermarks on one input.

Currently, Jet doesn't support this. To support it, we'll add the `key` field to the 
`Watermark` class:

```java
public final class Watermark implements BroadcastItem {
    private final int key;        // <-- new field
    private final long timestamp;
    // ...
}
```

This way a stream of events can have multiple watermarked events, each watermark
can have a different value.

Now the rules for removing items from buffer and for watermark forwarding from
the previous case aren't sufficient. A basic rule for watermark processing is
that a watermark must never overtake an event coming before it. If there are two
watermarks in a stream, and we postpone the event, we must postpone both
watermarks so that that event is not late when the processor eventually emits
it.

Let's look at an example of a processor with a single input, but with 2
watermarks in it, one for `time0` and one for `time1`. The processor postpones
events until they would be late. The processor receives the following item and
stores it in its buffer:

```
event(time0=12, time1=20)
```

Later the processor receives `wm(time0=15)`. We cannot forward the watermark
yet, because even though the event is late according to `time0`, it's not late
according to `time1` (no wm for `time1` was received yet). We can emit
`wm(time0=12)`, because that's the oldest event in our buffer that we will emit,
and we will not receive any new event where `time0` would be less than 15.

Later on, the processor receives `wm(time1=21)`. It emits `event(time0=12,
time1=20)` and removes it from the buffer. At this moment, the processor can
emit `wm(time0=15)`, because that's what we received from the input, and there's
nothing older held in the buffer that we could potentially emit. It can also
emit `wm(time1=21)`, as now there's no event held back in the buffer.

##### Implementation of multiple watermarks in the JOIN processor

Above we have shown how to solve two watermarks in one input. The postponing
processor did exactly what the join processor does with the join buffers. It
holds back the events for as long as there can be a matching event received from
the other input, and postpones watermarks for as long as it holds back the
events. And when it removes the event from the buffer, it can emit watermarks.

We still don't know how to determine, when the processor can remove rows from
buffer in case of multiple watermarks on either input. The processor can remove
an event from buffer, when all the time-bound constraints from the other side
are false.

To store the postponing information, we now need to use a map:
```
inputWmKey -> List<{outputWmKey.key, postponeTime}>
```

In the canonicalized time-bound representation:
```
timeA >= timeB - constant
```

the `timeA`'s watermark key is the `outputWmKey`, the `timeB`'s watermark key is
the `inputWmKey` and the `constant` is the `postponeTime`. We need to map each
`outputWmKey` to a list, because for each key there can be multiple keys on the
other side. For implementation reasons, we'll also add `0` `postponeTime` for
self-reference. It will represent the canonicalized time bound of `timeA >=
timeA - 0`.

##### Example

Let's look at an example. 
```sql
select *
from orders o
join deliveries d ON d.time BETWEEN o.time - 1 and o.time + 3
join returns r ON r.time BETWEEN d.time - 1 AND d.time + 4
```

This query is converted to this execution plan:
```
-join2[condition=r.time BETWEEN d.time - 1 AND d.time + 4]
--join1[condition=d.time BETWEEN o.time - 1 and o.time + 3]
---scan[orders]
---scan[deliveries]
--scan[returns]
```

We will be looking at the processor backing the `join2` node.

Extracted conditions:
```
r.time >= d.time - 1
d.time >= r.time - 4
```

There is no time bound between `order` and `delivery` events, though it exists
in the query, but it's used only in `join1`. There's also no time bound between
`order` and `return` events - this one isn't present in the query at all. The
conditions are sufficient to keep the state bounded, because there's at least
one condition for each input, involving a watermarked field from the other
input.

Watermark keys:
```
0: o.time (left input)
1: d.time (left input)
2: r.time (right input)
```

`postponeTimeMap`:

```
0 -> [{0, 0}]
1 -> [{1, 0}, {2, 1}]
2 -> [{1, 4}, {2, 0}]
```

Note that the entries for relation between `order` and `delivery` and between
`order` and `return` are not included in the list, as noted above.

We'll track the received watermarks in the following `wmState` nested map:

```
outputWmKey -> inputWmKey -> wmValue
```

The `List` elements will match those of `postponeTimeMap`. In our example that
is for output WM key `0`, the list will have one element with the WM value for
input WM key 0 etc. Initially, we'll store `-inf` for all values.

```
initial wm state: 0:{0:-inf}, 1:{1:-inf, 2:-inf}, 2:{1:-inf, 2:-inf}
in:  l{o.time=102, d.time=101}
-> leftBuffer: [l{o.time=102, d.time=101}]
in:  l{o.time=102, d.time=103}
-> leftBuffer: [l{o.time=102, d.time=101}, l{o.time=102, d.time=103}]
in:  wm0(103)
-> wm state: 0:{0:103}, 1:{1:-inf, 2:-inf}, 2:{1:-inf, 2:-inf}
# 103 is the new minimum for wm0, but we can't emit it now as it would render items in
leftBuffer late. We also can't remove them from the buffer, as they're not late due
their d.time. We can emit wm0(102) as that will not render any items in buffers late
out: wm0(102)
in:  r{r.time=99}
out: joined{o.time=102, d.time=101, r.time=99}
# not emitting joined{o.time=102, d.time=103, r.time=99}, as the join condition is false,
even though we have these left and right rows in the buffers
-> rightBuffer: [r{r.time=99}]
in:  wm1(102)
-> wm state: 0:{0:103}, 1:{1:102, 2:-inf}, 2:{1:101, 2:-inf}
in:  wm2(110)
-> wm state: 0:{0:103}, 1:{1:102, 2:106}, 2:{1:101, 2:110}
# new minimum in key=1 is 102 and in key=2 is 101. Now we remove anything that is late
from buffers according to all their watermarked timestamps
-> leftBuffer: [l{o.time=102, d.time=103}]
-> rightBuffer: []
```

To prove that the example is correct, now there must not be anything that the processor
can possibly receive, that would join with the rows that we removed from the buffers.

```
in:  l{o.time=103, d.time=102}  # the oldest possible event in the left input
# this wouldn't join with the removed r{r.time=99}, not even with r{r.time=100}
in:  r{r.time=110}  # the oldest possible event in the right input
# this wouldn't join with the removed l{o.time=102, d.time=101}
```

##### Processor design

The processor should be independent from SQL and be available as public Core
API. Pipeline API is nice to have for 5.2. It's code will be in the core module
and cannot depend on Apache Calcite objects.

Parameters:

- the join condition
- extracted postpone time map from the join condition
- list of left input stream event timestamp extraction functions (one for each watermarked column on left input stream)
- list of right input stream event timestamp extraction functions (one for each watermarked column on right input stream)

##### Algorithm

Consider having two input streams __S1__ and __S2__. Each input stream **must** contain at least one watermark with
defined watermark key.

1. Perform query analysis, detect timestamp column from both input stream schemas
2. Produce JOIN condition, timestamp extraction functions and postpone maps.
3. Prepare a **watermark state** data structure (`long[][]`) - time limit for each watermark keys relation.
4. Prepare two buffers : `leftBuffer` to store input events from ordinal 0 and `rightBuffer` to store input events from ordinal 1.
5. If a  watermark with key `key` is received:
   1. Iterate the value of `postponeTimeMap` for the watermark's key and update the same input/output
       WM keys in the `wmState`, postponed by the `postponeTime`
   2. Compute minimum for each output WM in the `wmState`.
   3. Remove all _expired_ events in left/right buffers: _Expired_ items are all items with all watermarked 
      timestamps less than minimum computed in the previous step.
   4. If doing an outer join, emit events removed from the buffer, with `null`s for the other side, if the
      event was never joined.
   5. From the remaining elements in the buffer, compute the minimum time value in each watermark
      timestamp column.
   6. Emit as watermarks as the minimum computed in steps 2 and 5, for each output WM key.
6. If an event is received:
    1. Store the event in left/right buffer.
    2. For each event in opposite buffer emit the joined event (if the whole join condition is `true`).

### Questions

_Q: Time bounds should be constant or variable size? Example:_

```sql
SELECT * FROM orders o
JOIN deliveries d ON d.time BETWEEN o.time 
                  AND o.time + o.delivery_deadline + interval '1' day 
```

A: We cannot support non-constant bounds because the processor won't be able to determine when it can remove an event
from the buffer. For the above example, the processor doesn't know when it is safe to remove `delivery` event from the
buffer, because it can always receive an order event with `delivery_deadline` large enough to join with any `delivery` event,
hence the state would be unbounded, which is not allowed.

#### Memory management

The processor will maintain upper bound of stored events configured in
`JobConfig.setMaxProcessorAccumulatedRecords()`.

### Testing Criteria

The following tests will be created:

- functional automated tests which will verify functional capabilities `INNER`, `LEFT`/`RIGHT`, `FULL` `OUTER` joins.
- automated integration tests as a subset of above tests which will be run using Kafka streams.
- SOAK durability tests which will verify stability over time.