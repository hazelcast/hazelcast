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

- batch to stream: one source can be batch (e.g. table), and the other source can be streaming data source.
- stream to stream, both data sources can be streaming (e.g. messaging topics) that need to be joined.

#### Goals

Hazelcast already supports static to stream joins. This work is intended to introduce stream to stream joins. The main
goals are:

- SQL based / Non-Java friendly: easy to use format also for non-Java developers to use stream to stream joins
- different join types (`INNER` , `LEFT`/`RIGHT` `OUTER` `JOIN`s.

It should be possible to efficiently manage state for these joins using watermarks or other mechanism(s).

### Functional Design

#### Summary of Functionality

User may execute stream-to-stream `JOIN` SQL query in the similar manner as usual JOIN. Let's define `orders`
and `deliveries` stream events (note, `Stream` type does not exist, used just for example) :

```sql
CREATE MAPPING orders (order_id BIGINT, order_time TIMESTAMP, item_id BIGINT) TYPE Stream
CREATE MAPPING deliveries (delivery_id BIGINT, order_id BIGINT, delivery_time TIMESTAMP, address VARCHAR) TYPE Stream
```

Consider the following query:

```sql
 CREATE VIEW fast_delivered_orders AS
 SELECT * FROM IMPOSE_ORDER(TABLE(orders), DESCRIPTOR(order_time), INTERVAL '1' MINUTE) AS o
 JOIN SELECT * FROM IMPOSE_ORDER(TABLE(deliveries), DESCRIPTOR(order_time), INTERVAL '1' MINUTE) AS d
 ON d.delivery_time BETWEEN o.order_time AND o.order_time + INTERVAL '1' HOUR
 
> SELECT * FROM fast_delivered_orders
```

Expected result set is represented in Table 1:

| order_id | order_time       | item_id | delivery_id | order_id | delivery_time    | address     |
|----------|------------------|---------|-------------|----------|------------------|-------------|
| 1        | 01.03.2022:10:00 | 100     | 1           | 1        | 01.03.2022:10:40 | 'address'   |
| 2        | 01.03.2022:11:00 | 100     | 2           | 2        | 01.03.2022:11:20 | 'address_2' |
| ---      | ---              | ---     | ---         | ---      | ---              | ---         |

__Table 1__

### Technical Design

_This is the technical portion of the design document. Explain the design in sufficient detail._

SQL engine should use a specialized Jet processor to perform JOIN operation for two input stream events. JOIN condition
allows suppose to be time-bounded. Non-time-bounded JOIN condition is not allowed to be used.

Consider having two streams - __S1__ and __S2__. There is only one payload in both streams event: conditional timestamp
of that event.

Let's describe all possible scenarios for joining these two streams:

| Behavior                                                       | Outcome                              |
|----------------------------------------------------------------|--------------------------------------|
| Both input streams don't have watermarks                       | exception during query processing    |
| At least one input stream doesn't have watermarks              | exception during query processing    |
| At least one input stream has zero lag                         | result set would be empty            |
| Both __S1__ and __S2__ have watermarks with non-zero lag times | result set would contain joined rows |

__Table 2__

#### JOIN processor design and algorithm description

##### Postpone map definition

There is a problem - processor cannot hold received events forever. We strongly require time-boundness in JOIN condition
exactly for that : processor should know which events are expired and then removes them. What is the most convenient way
represent time-bounded in JOIN condition? To solve this problem, we want to introduce the _canonical representation_ of
time bound conditions between different input event types. Proposed format is :

```
inputX.time >= inputY.time - constant
...
```

We use mathematical transformations to transform the fresh extracted condition to unified form, or, _canonical
representation_. We then translate this _canonical representation_ into a simple data structure called a `postpone map`.
For each input event key, we define a complete dependency map :

```
...
{inputX.key -> {{inputA.key, constant1}, ..., inputX.key, 0}, ..., {inputZ.key, constant2 }}
...
```

Example:
Consider having query:

```
SELECT * from input1 i1
JOIN input3 i2 ON i2.time BETWEEN i1.time - 1 AND i1.time + 4
```

Extracted conditions:

```
i2.time >= i1.time - 1
i1.time >= i2.time - 4
```

Postpone map:

```
0 -> [{0, 0}, {1, 1}]
1 -> [{0, 4}, {1, 0}]
```

##### Processor design

Items:

- join meta-information (`JetJoinInfo`)
- list of left input stream event timestamp extraction functions (according to watermarks count on left input stream)
- list of right input stream event timestamp extraction functions (according to watermarks count on right input stream)
- postpone map

##### Algorithm

Consider having two input streams __S1__ and __S2__. Each input stream **must** contain at least one watermark with
defined watermark key. Also, each input stream **may** contain

1. Perform query analysis, detect timestamp column from both input stream schemas
2. Produce JOIN condition, timestamp extraction functions and postpone maps.
3. Prepare a **watermark state** data structure (`long[][]`) - time limit for each watermark keys relation.
4. Prepare two buffers : `B0` to store input events from ordinal 0 and `B1` to store input events from ordinal 1.
5. Receive event `E` from the ordinal.
    1. If received event is watermark with key `key` :
        1. Offer changes to **watermark state**: redefine minimum available time for each join condition written in WM
           state.
        2. Try to clean all _expired_ events in related buffer:
            1. _Expired_ items are all items with watermark timestamp less than value in corresponding watermark state.
        3. Compute `minTime` as minimum timestamp of items in related buffer to watermark key.
        4. Emit `minTime` as watermark event with `key` to the outbox.
    2. Else:
        1. Extract timestamp from event E.
        2. Store E to the buffer B0, if received from ordinal 0, store to B1 otherwise.
        3. For each event in opposite buffer
            1. Perform JOIN operation for each event in 'opposite' buffer.
            3. If the join type is `OUTER JOIN`, we should fill empty side (no input events received) with NULL.
            4. Emit joined event.

#### Watermarks

JOIN processor would consume stream events from two input streams and join them with regard to JOIN condition. Result
will not be emitted in case one stream is producing events while the other produces no events for a period of time. We
have limited amount of memory to store input events. In that case, we require watermarks to used to drop late items and
to drop overdue items. For that, Hazelcast SQL engine supports `IMPOSE_ORDER` function to add watermarks to stream.
Non-watermarked streams are not allowed to be joined.

Separate JOIN processor should also emit watermarks. For that, we should extend `Watermark` class in such manner:

```java

@SuppressWarnings("ClassCanBeRecord")
public final class Watermark implements BroadcastItem {
    private final int key;        // <-- new field
    private final long timestamp;
    // ...
}
```

_Q: Time bounds should be constant or variable size? Example:_

```sql
SELECT * FROM orders o
JOIN deliveries d ON d.time BETWEEN o.time 
                  AND o.time + o.delivery_deadline + interval '1' day 
```

#### OUTER JOIN handling `[**]`

In case of OUTER JOIN, we should fill empty side with NULLs if no input events happened.

#### Memory management

Platform would manage fixed number of stored events in buffer.
__TODO: define limit amount of events__

### Testing Criteria

The following tests will be created:

- functional automated tests which will verify functional capabilities `INNER`, `LEFT`/`RIGHT`, `FULL` `OUTER` joins.
- automated integration tests as a subset of above tests which will be run using Kafka streams.
- SOAK durability tests which will verify stability over time.