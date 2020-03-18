# SQL Protocol

## 1 Overview

This document describes the communication protocol of the Hazelcast Mustang engine, which is built on top of the networking
design [[1]].

## 2 Query ID

Query execution is a distributed process which spans one or more nodes. Every query execution is assigned with a unique
ID which is propagated with messages to participating members. The ID must be unique cluster-wide.

We thus define the query ID as follows.

*Snippet 1: Query ID*
```java
class QueryId {
    UUID initiatorMemberId; // ID of the initiator
    UUID localId;           // Unique ID
}
```

`initiatorMemberId` provides quick access to the identity of the member, which initiated the query. `localId` ensures that the
whole query ID is unique cluster-wide.

## 3 Query life cycle

This section describes the messages sent throughout the query life cycle, except for data exchange messages.

Let us define the following member types:
1. **Participant (P)** - a member participating in query execution
1. **Query Initiator (I)** - a participant, which started query execution, and which is responsible for coordination
1. **Cancel Initiator (C)** - a participant which initiated query cancellation

The following messages are used:
1. `execute` - starts query execution
1. `cancel` - cancels the query

### 3.1 Query start

Before the query is started, the engine determines the list of participants. This list is fixed during query execution.

Query execution starts with the `execute` message being sent from the initiator to all participants.

*Snippet 2: Execute protocol*
```
I                 P1  P2
|----execute->----|   |
|--------execute->----|
```

When the participant receives the `execute` message, it starts execution of the query **immediately**. There is no single
synchronization point, like in Hazelcast Jet, when either the initiator or participants know that all other participants
started execution of the query. This way we improve the latency, because participants do not need to wait for another
message to start actual query execution. On the other hand, it introduces inherent race conditions between query start,
cancel, and data arrival, which are discussed in the section 4.

The `execute` message has the following structure.

*Snippet 3: `execute` message structure*
```java
class QueryExecuteOperation {
    QueryId id;                // Unique ID of the query 
    QueryPlan plan;            // Query execution plan
    List<Object> parameters;   // Query parameters
    List<UUID> participantIds; // Fixed list of participants
}
```

### 2.2 Query cancel

Query cancel might be required for different reasons, such as user request, timeout, exception, participant leave. Any
participant may request the cancellation of the query at any time.

The `cancel` request is first sent from the cancel initiator to the query initiator. The query initiator then broadcasts
the request to all participants. If the cancel initiator is the query initiator, then only the broadcast to participants is
performed.

*Snippet 4: Cancel protocol*
```
C                I                P1  P2
|----cancel->----|                |   |
|                |----cancel->----|   |
|                |--------cancel->----|
```
*Snippet 5: Cancel flow when cancel initiator is query initiator*
```
C/I              P1  P2
|                |   |
|----cancel->----|   |
|--------cancel->----|
```
This two step process reduces the total number of cancel messages which might be sent in the worst case. Consider the case when
all participants initiate the query cancel at the same time, e.g. due to participant leave. If we implement the cancellation
as a broadcast from the cancel initiator to participants, then up to `N * N` of messages could be sent. With the two step approach
no more than `2 * N` messages are sent: N from all cancel initiators to query initiator, N from query initiator to participants.
The two step approach has higher latency, but the latency is not important for cancellation process.

The `cancel` message has the following structure.

*Snippet 6: `cancel` message structure*
```java
class QueryCancelOperation {
    QueryId id;             
    UUID cancelInitiatorId;
    int errorCode;      
    String errorMessage;
}
```

If the query coordinator is down, all participants will notice this eventually, and perform query cancellation locally without
sending any messages.

## 3 Data exchange

This section describes how data is exchanged between participants.

The original query plan is split into one or more **fragments**. The split occurs across relational operators which require
data movement between members, called **exchanges**.

Below is the example of a distributed sorting which requires data exchange and two fragments obtained after the split.

*Snippet 7: Plan before split*
```
-- MergeSort         // Perform final merge sort on a single participant
---- UnicastExchange // Send sorted streams from participants to a single participant
------ LocalSort     // Perform sort on the local node
```

*Snippet 8: Fragments after split*
```
Fragment 1:
-- MergeSort
---- Receive[edgeId=1]

Fragment 2:
---- Send[edgeId=1]
------ LocalSort
```

Data exchange between two operators from two fragments is called **stream** as described in [[1]]. A stream has two actors:
1. **Sender (S)** - sends data in batches
1. **Receiver (R)** - receives data and responds with flow control messages

The following messages are used:
1. `batch` - batch of rows sent from a sender to a receiver
1. `flowControl` - flow control message sent from a receiver to a sender

### 3.1 Batches

Data is first accumulated in batches on sender's side. The batch is then serialized and sent over the wire in `batch` message.
The sender doesn't expect an ack for every batch from the receiver. Multiple batches could be sent one after the other. The
last batch in the stream has special marker, denoting end of the stream.

*Snippet 9: Batch protocol*
```
S                                R
|                                |
|----batch_1->-------------------| 
|------batch_2->-----------------|
|--------batch_3[last=true]->----|  
```

The `batch` message has the following structure.

*Snippet 10: `batch` message structure*
```java
class QueryBatchOperation {
    QueryId id;             
    int edgeId;
    RowBatch batch;        // Actual data      
    boolean last;          // Last batch flag
    long remainingCredits; // Used in the flow control as explained below
}
```

### 3.2 Flow Control

The sender may produce new batches faster than the receiver could process them, so the flow control is needed. We implement
it using the credit-based approach:
1. Sender and receiver agree on the initial amount of credits
1. Whenever the sender sends the batch, it decreases the amount of credits by the value which depends on the batch size
1. When the amount of credits on the sender reaches zero, the sender stops sending data
1. Receiver periodically increases the amount of credits available to the sender and sends it back, so that the sender may
resume data exchange

One credit is equal to one byte because it is convenient for the memory management purposes. A row could not be used as a credit
becaise size of the rows may vary significantly.

Precise conditions which trigger the `flowControl` message, as well as the initial amount of credits are implementation-defined
and are not part of the protocol.

*Snippet 11: An example of control flow protocol*
```
S                         R
|                         |
|----batch[100]->---------| 
|------batch[80]->--------|
|--------batch[40]->------|
|                         |
|----<-flowControl[90]----|
|                         |
|----batch[60]->----------|  
```

*Snippet 12: `flowControl` message structure*
```java
class QueryBatchOperation {
    QueryId id;             
    int edgeId;
    long remainingCredits;
}
```

### 4 Cleanup

Given that there are no synchronization points between query start, batch arrival, and query finish (either normal or due to
cancel), various race conditions are possible. For example, a batch may arrive the the participant after the query has been
cancelled. Such batch must be **discarded**.

*Snippet 13: Race condition with a stale data batch*
```
I              P1            P2
|----start->---|             |
|------start->---------------|
|----cancel->--|
|              |--<-batch----|
|------cancel->--------------|
```

At the same time, a batch may arrive before the query execution is started. Such batch must be **retained**. The example below
demonstrates the problem.

*Snippet 14: Race condition with a data batch arriving before the query is started*
```
I              P1            P2
|------start->---------------|
|              |--<-batch----|
|----start->---|             |
```

When the batch arrives and there is no active query with the given ID, we cannot distinguish between the query which has not
started yet, and the query which has been cancelled. To mitigate this we introduce a a pair of messages to clean stale batches:

1. `check` - verifies whether queries with the given IDs are still active on the query initiator
1. `checkResponse` - a response to the `check` message with the subset of suspected query IDs, which are no longer active on the
query initiator

When a participant receives the batch and cannot find the active query with the given ID, it puts it into the pending queue.
The participant iterates over pending batches periodically and collects their query IDs. Then the participant sends `check`
messages to query initiators. The query initiator ID could be derived from the query ID. The query initiator then iterates
over the active set of active queries and determines the list of suspected query IDs which are no longer active. This list
is sent back to the participant in the `checkResponse` messages. The participant is then deletes the stale batches and all
other resources associated with the queries which are no longer active.

The protocol relies on the fact is that query start on the initiator happens-before any check message arrival containing the ID
of the started query. Therefore, if the query with the given ID is not active on the query initiator, then it may be considered
completed (cancelled).

*Snippet 15: Check protocol*
```
P                            I
|                            |
|----check[id1, id2]->-------| 
|                            |
|----<-checkResponse[id1]----|
```

*Snippet 16: `check` and `checkResponse` messages structure*
```java
class QueryCheckOperation {
    List<QueryId> suspectedQueryIds;
}

class QueryCheckResponseOperation {
    List<QueryId> inactiveQueryIds;
}
```

[1]: 03-networking.md "SQL Networking"
