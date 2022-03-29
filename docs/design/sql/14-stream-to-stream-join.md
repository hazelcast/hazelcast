# Stream to stream JOIN

### Table of Contents

+ [Background](#background)
    * [Goals](#goals)
+ [Functional Design](#functional-design)
    * [Summary of Functionality](#summary-of-functionality)
+ [Technical Design](#technical-design)
    + [Technical Design](#watermarks)
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
- different join types (`INNER` , `LEFT`/`RIGHT`/`FULL` `OUTER` `JOIN`s.

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

SQL engine should use a specialized Jet processor to perform JOIN operation for two input stream events.

#### Overall design

Consider having two streams - __S1__ and __S2__. There is only one payload in both streams event: conditional timestamp
of that event.

Let's describe all possible scenarios for joining these two streams:

| Behavior                                                   | Outcome                           |
|------------------------------------------------------------|-----------------------------------|
| Both input streams don't have watermarks                   | exception during query processing |
| At least one input stream doesn't have watermarks          | exception during query processing |
| At least one input stream has zero lag                     | result set would be empty         |
| Both __S1__ and __S2__ have watermarks with same lag times | Example in Table 3,4              |
| ---                                                        | ---                               |

__Table 2__

Let lag time be equal for both inputs. Consider streams have the following input:

| S1          | S2          |
|-------------|-------------|
| s1_event(1) | s2_event(1) |
| s1_wm(2)    | s2_wm(2)    |
| s1_event(3) | s2_event(3) |
| s1_wm(4)    | s2_wm(4)    |
| -           | s2_event(4) |
| -           | s2_wm(6)    |
| -           | s2_event(7) |
| -           | s2_wm(8)    |

__Table 3__

After applying the `CROSS JOIN` operation we expect next output:

| JOIN(S1, S2)               |
|----------------------------|
| [s1_event(1), s2_event(1)] |
| s1_wm(2)`[*]`              |
| s2_wm(2)`[*]`              |  
| [s1_event(3), s2_event(3)] |
| [s1_event(3), s2_event(4)] |
| s1_wm(4)                   |
| s2_wm(4)                   |  
| s2_wm(6)                   |  
| [NULL, s2_event(7)]`[**]`  |  
| s2_wm(8)                   |  

__Table 4__

#### Processor algorithm description

Consider having two input streams __S1__ and __S2__. Let's define the schema for __S1__ and __S2__ as

```sql
CREATE MAPPING S1 (id BIGINT, payload BIGINT, s1_time TIMESTAMP) TYPE Stream
CREATE MAPPING S2 (id BIGINT, payload VARCHAR, s2_time TIMESTAMP) TYPE Stream
```

Consider the following query:

```sql
> SELECT * FROM IMPOSE_ORDER(TABLE(S1), DESCRIPTOR(s1_time), INTERVAL '1' SECOND) AS s1
  INNER JOIN 
  SELECT * FROM IMPOSE_ORDER(TABLE(S2), DESCRIPTOR(s2_time), INTERVAL '1' SECOND) AS s2
  ON s2.s2_time BETWEEN s1.s1_time AND s1.s1_time + INTERVAL '2' SECOND 
```

1. Perform query analysis, detect timestamp column from both input stream schemas
2. Prepare two buffers : B0 to store input events from ordinal 0 (stream S1) and B1 to store input events from ordinal
   1 (stream S2).
3. Receive event E from the ordinal.
    1. If received event is watermark:
        1. Clear buffer B0, if received from ordinal 0, clear B1 otherwise.
        2. Emit watermark event to the outbox.
    2. Else:
        1. Extract timestamp from event E.
        2. If extract attempt was failed - throw an exception.
        3. Store E to the buffer B0, if received from ordinal 0, store to B1 otherwise.
        4. For each event in 'parallel' buffer
            1. Test the JOIN condition with received timestamps.
            2. Perform JOIN operation for each event in 'parallel' buffer, perform
                1. If test was not successful, retry step 3.
            3. If the join type is `OUTER JOIN`, we should fill empty side (no input events received) with NULL.
            4. Emit joined event.

#### Watermarks

JOIN processor would consume stream events from two input streams and join them with regard to JOIN condition. Result
will not be emitted in case one stream is producing events while the other produces no events for a period of time. We
have limited amount of memory to store input events. In that case, we require watermarks to used to drop late items and
to drop overdue items. For that, Hazelcast SQL engine supports `IMPOSE_ORDER` function to add watermarks to stream.
Non-watermarked streams are not allowed to be joined.

`[*]` __Note__: each watermark event should stay as a separate event in joined stream.

_Q: Should we support only timestamps as JOIN condition?_
**Answer: Yes**

_Q:How to convert the JOIN condition into deletion rule, if it does not touch timestamp?_

_Q: What semantics should we consider for queries with zero lag for both inputs?_

```sql
SELECT * FROM orders_with_0_lag o 
JOIN deliveries_with_0_lag d ON o.order_id = d.order_id
```

_Q: Time bounds should be constant or variable size? Example:_

```sql
SELECT * FROM orders o
JOIN deliveries d ON d.time BETWEEN o.time 
                  AND o.time + o.delivery_deadline + interval '1' day 
```

Separate JOIN processor should also emit watermarks. For that, we should extend `Watermark` class in such manner:

```java

@SuppressWarnings("ClassCanBeRecord")
public final class Watermark implements BroadcastItem {
    private final int key;        // <-- new field
    private final long timestamp;
    // ...
}
```

#### Processor API change to to handle multiple watermarks

We should also change/extend `Processor` API to handle multiple watermarks. Consider having that new method
within `Processor` class:

```java
interface Processor {
    // ...
    boolean tryProcessWatermark(int ordinal, @Nonnull Watermark watermark);
    // ...
}
```

#### OUTER JOIN handling `[**]`

In case of OUTER JOIN, we should fill empty side with NULLs if no input events happened.

#### Memory management

Fixed number of stored keys.

### Testing Criteria

The following tests will be created:

- functional automated tests which will verify functional capabilities `INNER`, `LEFT`/`RIGHT`, `FULL` `OUTER` joins.
- automated integration tests as a subset of above tests which will be run using Kafka streams.
- SOAK durability tests which will verify stability over time.