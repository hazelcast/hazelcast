# Stream to stream JOIN

|||
|---|---|
|Related Jira|[HZ-986](https://hazelcast.atlassian.net/browse/HZ-986)|
|Document Status / Completeness|DONE|
|Requirement owner|Sandeep Akhouri|
|Developer(s)|Sasha Syrotenko|
|Quality Engineer|Bartlomiej Poplawski|
|Technical Reviewers|Viliam Durina|

### Background

In streaming data processing, there are several scenarios for joins:

- batch to stream: one source is batch (e.g. a table), and the other source is
  streaming.
- stream to stream: both data sources are streaming (e.g. messaging topics) that
  need to be joined.

#### Goals

Hazelcast already supports batch to stream joins. This work aims to introduce
stream to stream joins. The main goals are:

- SQL based / Non-Java friendly: easy to use format also for non-Java developers
  to use stream to stream joins
- Core API also available. Pipeline API nice to have.
- different join types (`INNER` , `LEFT`/`RIGHT` `OUTER` `JOIN`s).

The state of the join processor must be bounded (i.e. no support for
stream-to-stream joins without time bounds) and the latency must be minimal
(i.e. emit the joined row as soon as the processor receives them).

### Functional Design

#### Summary of Functionality

Semantically, the join operation, as specified by SQL, is easy to apply to
streams: The query should output all items from the left input, joined with all
items from the right input, which meet the join condition.

For example, consider `orders` and `deliveries` stream events (note, `Stream`
type does not exist, used just for example):

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
     ON d.delivery_time BETWEEN o.order_time 
         AND o.order_time + INTERVAL '1' HOUR
```

The `<table>_ordered` is a view containing the `IMPOSE_ORDER` function on top of
`<table>`.

Example result set is shown in Table 1:

| order_id | order_time       | item_id | delivery_id | order_id | delivery_time    | address     |
|----------|------------------|---------|-------------|----------|------------------|-------------|
| 1        | 01.03.2022:10:00 | 100     | 1           | 1        | 01.03.2022:10:40 | 'address_1' |
| 2        | 01.03.2022:11:00 | 100     | 2           | 2        | 01.03.2022:11:20 | 'address_2' |
| ---      | ---              | ---     | ---         | ---      | ---              | ---         |

__Table 1__

### Technical Design

SQL engine should use a specialized Jet processor to perform JOIN operation for
two input streams. The join condition must constrain the required buffering time
of events from both inputs; otherwise, the query will be rejected from execution
because the engine would have to keep ever-growing state.

#### JOIN processor design and algorithm

##### Simple case for two inputs

In order to determine for how long the processor needs to buffer events from
each side, the engine will extract time bounds from the join condition in the
following form:

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
here is only to extract the part needed to curb the buffering time. If there's a
disjunction (OR expression), we will not implement such query, even if all
disjunct parts contain the needed time bounds.

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
satisfy the join condition of `i1.time >= i2.time - 4` for any new non-late `i2`
event we can possibly receive, because `5` is not greater than or equal to `10 -
4`. Therefore, we can remove all events with `i1.time <= 5` from the buffer, and
emit the watermark with `time=6` to the output.

The second condition allows the processor to remove items from the other buffer.

This scenario is straightforward to generalize to inputs with multiple
timestamps, let's look at that.

##### Generalizing the behavior for multiple watermarks

As you can see from the previous example, the output of the processor contains
two watermarks: one for `i1` and one for `i2`. If the output of this processor
is input to another join processor (e.g. in case of a join of three streams),
there will be two watermarks on one input.

Currently, Jet doesn't support this. To support it, we'll add the `key` field to
the `Watermark` class. This topic is covered in [a separate
TDD](14-keyed-watermark-support.md).

With keyed watermarks, a stream of events can have multiple watermarked fields,
each watermark can have a different value.

Since the individual time bounds must all be satisfied (they were connected with
AND in the join condition), if any of them isn't we can eliminate the item.

##### Implementation of multiple watermarks in the JOIN processor

The processor holds back the events for as long as there can be a matching event
received from the other input, and postpones watermarks for as long as it holds
back the events. When it removes the event from the buffer, it can emit
watermarks.

We still don't know how to determine when the processor can remove rows from a
buffer in case of multiple watermarks on either input. The processor can remove
an event from the buffer, when any time-bound constraint is false.

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
other side.

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
inputWmKey -> [{outputWmKey, postponeTime}, ...]
0 -> []
1 -> [{2, 1}]
2 -> [{1, 4}]
```

Note that the entries for relation between `order` and `delivery` and between
`order` and `return` are not included in the lists, as noted above.

We'll track the received watermarks in the following `wmState` nested map:

```
outputWmKey -> inputWmKey -> wmValue
```

The inner map elements will match the elements of `postponeTimeMap`. In our
example that is for output WM key `0` the inner map will be empty, for output WM
key `1` the inner map will have one entry for key `2` etc. The initial values
will be `-inf`.

```
initial wm state: 0:{}, 1:{2:-inf}, 2:{1:-inf}
in:  l{o.time=102, d.time=101}
-> leftBuffer: [l{o.time=102, d.time=101}]
in:  l{o.time=102, d.time=103}
-> leftBuffer: [l{o.time=102, d.time=101}, l{o.time=102, d.time=103}]
in:  wm0(103)
-> wm state: 0:{}, 1:{2:-inf}, 2:{1:-inf}
# the wm0 doesn't affect the wmState
# 103 is the new value for wm0, but we can't emit it now as it would render items in
  the buffers late. We also can't remove those items from the buffer, as a matching 
  row can still be received. We can emit wm0(102) as that will not render any items 
  in buffers late
out: wm0(102)
in:  r{r.time=100}
out: joined{o.time=102, d.time=101, r.time=100}
-> rightBuffer: [r{r.time=100}]
in:  wm1(102)
-> wm state: 0:{}, 1:{2:-inf}, 2:{1:101}
# New maximum in wmState[2] is 101, it means we can eliminate anything with r.time<101.
# In plain words, the join condition will be false for r.time=100 for any new row.
-> rightBuffer: []
# For wm1, we can now output wm1(101), as the value 102 would render items in leftBuffer late.
out: wm1(101)
in:  wm2(110)
-> wm state: 0:{}, 1:{2:106}, 2:{1:101}
# new maximum in key=1 is 106. Now we remove anything from buffers with d.time<106
-> leftBuffer: []
-> rightBuffer: []
# we can also emit all watermarks up to the last received value, as the buffers are now empty
out: wm0(103)
out: wm1(102)
out: wm2(110)
```

To prove that the example is correct, there:
1. must not be anything that the processor can possibly receive, that would join
   with the rows that we removed from the buffers (i.e. "we didn't remove too
   much")
2. must not be anything in the buffer what would not join with the lowest row
   the processor could possibly receive in the future (i.e. "we don't store too
   much")

```
Rule 1:
in:  l{o.time=103, d.time=102}
# this wouldn't join with the removed r{r.time=99}, not even with r{r.time=100}
in:  r{r.time=110}
# this wouldn't join with the removed l{o.time=102, d.time=101}

Rule 2:
Buffers are empty, so it's obviously true. But let's evaluate the lowest items that we would
not remove:
leftBuffer: l{o.time=-inf, d.time=106}
rightBuffer: r{r.time=101}
in:  l{o.time=103, d.time=102}
# this would join with r{r.time=101}
in:  r{r.time=106}
# this wouldn't join with l{o.time=103, d.time=102}
```

##### Processor design

The processor should be independent of SQL and be available as public Core API.
Pipeline API is nice to have for 5.2. It's code will be in the core module and
cannot depend on Apache Calcite objects.

Parameters:

- the join condition
- extracted postpone time map from the join condition
- list of left input stream event timestamp extraction functions (one for each
  watermarked column on left input stream)
- list of right input stream event timestamp extraction functions (one for each
  watermarked column on right input stream)

##### Algorithm

Consider having two input streams __S1__ and __S2__. Each input stream **must**
contain at least one watermark with defined watermark key.

1. Perform query analysis, detect timestamp column from both input stream
   schemas
2. Produce JOIN condition, timestamp extraction functions and postpone maps.
3. Prepare a `wmState` data structure - time limit for each watermark keys
   relation.
4. Prepare two buffers : `leftBuffer` to store input events from ordinal 0 and
   `rightBuffer` to store input events from ordinal 1.
5. If a watermark with key `key` is received:
   1. Iterate the value of `postponeTimeMap` for the watermark's key and update the
      same input/output WM keys in the `wmState`, postponed by the
      `postponeTime`
   2. Compute new maximum for each output WM in the `wmState`.
   3. Remove all _expired_ events in left & right buffers: _Expired_ items are all
      items with any watermarked timestamp less than the maximum computed in the
      previous step.
   4. doing an outer join, emit events removed from the buffer, with `null`s for
      the other side, if the event was never joined.
   5. From the remaining elements in the buffer, compute the minimum time value in
      each watermark timestamp column.
   6. For each WM key, emit a new watermark as the minimum of value computed in step
      5 and of the last received value for that WM key.
6. If an event is received:
   1. If the event is late according to any last received watermark, drop it.
   2. If the event is out of bounds according to `wmState`, join it with nulls (if
      outer-joining) and stop processing it
   3. Store the event in left/right buffer.
   4. For each event in the opposite buffer emit the joined event (if the whole join
      condition is `true`).

### Questions

_Q: Time bounds should be constant or variable size? Example:_

```sql
SELECT * FROM orders o
JOIN deliveries d ON d.time BETWEEN o.time 
                  AND o.time + o.delivery_deadline + interval '1' day 
```

A: We cannot support non-constant bounds because the processor won't be able to
determine when it can remove an event from the buffer. For the above example,
the processor doesn't know when it is safe to remove `delivery` event from the
buffer, because it can always receive an order event with `delivery_deadline`
large enough to join with any `delivery` event, hence the state would be
unbounded, which is not allowed.

##### Edge cases

- There can be a time bound between timestamps on the same input, which we
  should take into account.

Example: TODO

- Reception of one WM can cause emission of multiple WMs. Example: see the end
  of the example above.

- A received row might not be late, but still can't possibly join any future
  rows from the other input. But it still can join rows already in the other
  buffer. In this case it's not added to the buffers, but it's joined with
  buffered rows, a null-padded row is output if there's no match and it's an
  outer join.

```
Example1:
in: l{l.time=0}
-> leftBuffer=[l{l.time=0}:unused]
in: r{r.time=0}
-> rightBuffer=[r{r.time=0}:unused]
out: j{l.time=0, r.time=0}
-> leftBuffer=[l{l.time=0}:used]
-> rightBuffer=[r{r.time=0}:used]
in: wm(l.time=1)
-> rightBuffer=[]
in: r{r.time=0}
out: j{l.time=0, r.time=0}
# rightBuffer remains empty
```

```
Example2, join condition: l.time = r.time:
in: l{l.time=0}
-> leftBuffer=[l{l.time=0}:unused]
in: wm(l.time=1)
out: wm(l.time=0)
in: r{r.time=0}
out: j{l.time=0, r.time=0}
# rightBuffer remains empty
```

**Join conditions involving only one input**

If the join condition contains an AND-joined condition applying only to one
input, those rows should never be buffered. For example:

```sql
SELECT *
FROM l 
LEFT JOIN r ON
    l.field>10
    AND l.time BETWEEN r.time - 1 AND r.time + 1
```

Here, the `l.field>10` condition applies only to the left input. If we receive a
row where `l.field == 9`, such row will never join anything. Perhaps Calcite
will take care of this by pulling up a FilterRel to remove these rows. But if
we're doing a left join, as in the example above, the output should contain all
rows from the left input, and for those that have no matching row on right, they
should be null-padded.

Our processor will work correctly without taking special steps. It will buffer
those rows, the join condition will be always false for them, and then, when
that row is removed due to a WM from the buffer, null-padded row will be
emitted. But we can do better: we can extract those conditions, and if they
evaluate to false, don't add such row to the buffer, and if outer-joining,
immediately emit a null-padded row.

It might seem that we can extend this to a time bound between two timestamps on
the same input. For example `ON l.time1 >= l.time2 + 10`. One can think that if
we receive a watermark for `l.time2`, we can remove rows with `time1 <
wm(time2) + 10` from the buffer. But this is wrong, because we don't join two
left rows. This condition is true or false for each individual left row.

#### Memory management

The processor will maintain upper bound of stored events configured in
`JobConfig.setMaxProcessorAccumulatedRecords()`.

### Testing Criteria

The following tests will be created:

- functional automated tests which will verify functional capabilities `INNER`,
  `LEFT`/`RIGHT`, `FULL` `OUTER` joins.
- automated integration tests as a subset of above tests which will be run using
  Kafka streams.
- SOAK durability tests which will verify stability over time.

### Fault tolerance

Since: HZ 5.3

For fault tolerance, we need to save the contents of the buffers, and also
ensure that if a null-padded row was emitted, after restore, a matching row
that's _not late by chance_ isn't emitted, and conversely, if some joined row
was emitted, a null-padded row is not emitted after restart. To ensure the
latter, we need to save a flag whether a row was unused along with the row.

The processor uses two routing schemes:

- for equi-join, we globally partition both inputs using the equi-join fields.
  To save the buffers, we'll use the partitioning key with each snapshot entry.
  After restore, items will be repartitioned to the new set of processors.

- for the broadcast-unicast case, we'll save the broadcast side using a
  `broadcastKey`, but we'll do it only on processor with global index 0. This way
  we'll ensure that only one copy is saved, and the one copy will be re-broadcast
  after restore. For the unicast side, each processor will save all its items, but
  we have to ensure that the entry is saved to some local partition, to avoid
  sending the primary copy of the snapshot data over the network.

Along with the buffer data, we need to save additional information to ensure
that items that were already removed from the buffer, and for which a
null-padded row was already emitted aren't joined with a row that is not late
after a restart. This can happen because watermarks after the restart can be
delayed differently than they were before the restart, because, for example,
some source takes longer time to initialize, and watermarks are coalesced.

Example:
```
Join condition: l.time=r.time
LEFT JOIN

in: l1(time=10)
in: r1(time=10)
out: joined(l1, r1)
in: wm(l.time=15)
out: wm(l.time=10)
--> row `r1` removed from buffer
# processor restarted, state saved and restored
in: l2(time=10)
--> Row `l2` would be late before the snapshot, but now isn't late.
     However we already removed the matching row. Later we'll emit
     `joined(l2, null)`, and the user will see both `l2` and `r1` in the output,
     but not correctly joined.
```

To avoid the issue in the example, it seems we need to save `lastEmittedWm` for
each WM key to the snapshot. By using approach similar to `SlidingWindowP`, we
can use `broadcastKey`, and when restoring, take the minimum of all the restored
values. But in `SlidingWindowP`, in exactly-once mode, all the values are
guaranteed to be equal, because when a WM is broadcast through a distributed
edge, since all processors receive the same set of watermarks before receiving
the barrier, they will be coalesced to the same value for each processor. This
does not hold in at-least-once mode, because watermarks can overtake barriers,
but let's put this issue aside.

This is not the case in `StreamToStreamJoinP`, because the `lastEmittedWm` value
doesn't depend solely on the input WM, as in `SlidingWindowP`, but also on the
`minBufferTime` value. Currently, the formula for output WM is:
```
outputWm = min(lastReceivedWm, minBufferTime)
```

Since the `lastEmittedWm` can effectively go back after restart even in
exactly-once mode, the same issue as in the example above is possible, because
what was late before the restart, might not be late after the restart.

The `minBufferTime` is the lower bound for the time value _actually_ in the
buffer. It depends on the actual buffered rows in each processor, and that's
different in each processor. Instead of `minBufferTime` we could use `wmState`,
which is the lower bound for the time value _possibly_ in the buffer. This value
is the same in each processor (in ex-once mode), because it depends solely on
the received watermarks and the postpone times. Therefore, if we change the
formula to this:

```
outputWm = min(lastReceivedWm, wmState)
```

The `lastEmittedWm` will now be equal in all processors, because
`lastReceivedWm` and `wmState` are also equal.

This will slightly increase the output WM latency. Thanks to coalescing, the
processors downstream from this processor would receive the WM capped by the
minimum time in buffers in all processors, while after this change they will
receive the lowest possible value. We consider this issue to be minor, because
if there are sufficient events, the difference is small. If there's an event for
every instant, the difference is zero. And, most importantly, it fixes the issue
with incorrect results after a restart.

An alternative solution would be to continue emitting WMs using the minimum
buffer time, but we would need to store the `lastEmittedWm` with each key saved
to the state, and also, when restoring, we would need to track the value for
each key so that we can correctly evaluate whether a row is late or not. So
we'll have a larger snapshot, and also larger runtime state and a lot more
complex code, and we don't consider it worth for the benefit of lower latency in
low-traffic scenario. I didn't even consider how would this be implemented for
the broadcast-unicast case.
