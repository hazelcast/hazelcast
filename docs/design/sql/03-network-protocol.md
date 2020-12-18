# SQL Network Protocol

## Overview

Hazelcast Mustang is a distributed SQL engine. Network communication between nodes is required to produce the final result.
In this document, we describe the design of the communication protocol, that includes query start and cancel, data exchange,
and maintenance operations.

The remainder of this document is structured as follows. Section 1 describes the primary design considerations. Section 2
discusses the existing networking infrastructure of the Hazelcast cluster and whether it satisfies our design
principles. Section 3 explains the protocol.

## 1 Design Considerations

In this section, we summarize the fundamental principles that influence the protocol design:
1. Low latency
1. Fail-fast
1. Ordered data processing

### 1.1 Low Latency

Hazelcast Mustang is a modern distributed SQL engine that targets the OLTP workloads for in-memory data. We expect
that the engine will be used mostly for relatively short queries, taking milliseconds or seconds to complete. Because network
latency is still relatively high as of 2020, the protocol should be designed to minimize the number of blocking network calls,
when one member waits for the other.

### 1.2 Fail-Fast

Distributed systems must account for network and hardware failures. Query execution is a stateful process. Whenever a participant
of a distributed query goes down, there are two options: try tolerating the failure and continue execution, or fail it.

The system needs to track the progress of query execution to tolerate the failure of a query participant. An example of such a
system is Google Spanner [[1]], which uses restart tokens to track the progress of the query, hiding network failures from
users. Another approach is to persist intermediate state to disk, which is used in OLAP engines where query restart is not an
appropriate choice.

While the idea of hiding intermittent failures from users is compelling from the usability point of view, it comes at a massive
engineering cost, as mentioned by Google Spanner engineers. The Hazelcast Mustang engine is designed with the **fail-fast**
behavior in mind: whenever a failure occurs, the query is terminated, and a proper transient error is delivered to the user.
The user should restart the query manually then. The query will fail in the following scenarios:
1. Member executing the query (participant) has left the cluster
1. Temporal network problem between two members which resulted in a broken TCP connection

We understand that fail-fast behavior might not be the best solution for some use cases, including machine shutdowns in cloud
environments. We, therefore, do not reject the idea of implementing transparent handling of network errors in future releases.
Instead, we treat the fail-fast behavior as a starting point for future improvements, which provides a good trade-off between
the added value and implementation complexity.

### 1.3 Ordered Data Processing

The Hazelcast Mustang engine employs the **Volcano Model** approach, as described in [[2]]. Data flows from children relational
operators to parents operators. The order of rows transferred between operators is essential for correctness. For example, a
parent operator may expect the child operator to produce rows that are sorted on a particular attribute.

Adjacent operators may be located on different members, exchanging data through network requests. The implementation must ensure
that the order of rows is preserved during processing.

## 2 Existing Infrastructure

The Hazelcast Mustang engine is a part of the Hazelcast IMDG product, which is a distributed system with its own networking
infrastructure built on top of TCP/IP protocol. In this section, we discuss the key networking abstractions used Hazelcast IMDG.
which are `Packet`, `Connection` and `Operation`. We then analyze how they are used in Hazelcast Mustang.

### 2.1 Packet

`Packet` is a chunk of data sent over the wire. The packet has length and type. **Length** defines the boundaries of the
packet's data within the network stream of data. Several packets could be sent in a single network request. Likewise, a
single packet could span multiple network requests. The original request is restored on the receiver side and submitted for
processing. A packet has a **type** that defines the handler for the packet. Examples are partition handler, generic handler,
Hazelcast Jet handler.

### 2.2 Connection

`Connection` abstracts out a peer-to-peer connection between two members.

Connections are established lazily between communicating members. The connection could be closed on idle timeout or error and
re-established later. Connection to a particular member is provided by the `EndpointManager` interface through
`getConnection()` and `getOrConnect()` methods. Subsequent calls to these methods may return different connection objects.

The order of the delivery of messages sent over a single `Connection` object is **not defined** for the following reasons:
1. Multiple physical connections could be used internally.
1. An automatic reconnect could happen.

### 2.3 Operation

Most Hazelcast components don't operate on packets directly. Instead, the `Operation` abstraction is used, which encapsulates
the message and additional information such as the caller ID, timeouts, retries, etc.

Operations are submitted to the `OperationService` which obtains the connection, serializes operations to packets, and manage
completion futures, timeouts, and retries.

### 2.4 Discussion

The Hazelcast Mustang engine doesn't use `Operation` and `OperationService` abstractions for the following reasons:
1, To satisfy the low-latency principle, the engine doesn't use the request-response messaging pattern for query initiation 
and data exchange. 
1. Due to fail-fast design choice, the engine doesn't need to keep data chunks on the sender side waiting for ack
from the receiver. The fire-and-forget approach is used instead. 

Instead, `Packet` and `Connection` interfaces are used directly. For every request, we obtain the `Connection` object
from the connection manager. 

## 3 Protocol

This section describes the communication protocol of the Hazelcast Mustang engine. The protocol consists of 6 message types
which we discuss in details below:
1. `start`
1. `cancel`
1. `batch`
1. `flow_control`
1. `check`
1. `check_response`

### 3.1 Query ID

Query execution is a distributed process that spans one or more nodes. Every query execution has a cluster-wide unique
ID which is propagated to participating members. The query ID has the following structure.

*Snippet 1: Query ID*
```java
class QueryId {
    UUID initiatorMemberId; // ID of the initiator
    UUID localId;           // Unique ID
}
```

`initiatorMemberId` provides quick access to the identity of the member, which initiated the query. `localId` ensures that the
whole query ID is unique cluster-wide.

### 3.2 Query Start

Before the query is started, the engine determines the list of participants. This list is fixed during query execution.

Query execution starts with the `start` message being sent from the initiator to all participants.

*Snippet 2: Start protocol*
```
I               P1  P2
|----start->----|   |
|--------start->----|
```

When the participant receives the `start` message, it starts the execution of the query **immediately**. There is no single
synchronization point, like in Hazelcast Jet, when either the initiator or participants know that all other participants
started the execution of the query. This decreases latency because participants do not need to wait for another
message to start actual query execution. On the other hand, it introduces inherent race conditions between query start,
cancel, and data arrival, which are discussed in section 4.

The `start` message has the following structure.

*Snippet 3: `start` message structure*
```java
class QueryExecuteOperation {
    QueryId id;                // Unique ID of the query 
    QueryPlan plan;            // Query execution plan
    List<Object> parameters;   // Query parameters
    List<UUID> participantIds; // Fixed list of participants
}
```

### 3.3 Query Cancel

Query cancel stops the execution of the query on participants. Query cancel might be requested for different reasons, such as
user request, timeout, exception, participant failure. Participants may request the cancellation of the query at any time.

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
This two-step process reduces the total number of cancel messages, which might be sent in the worst case. Consider the case when
all participants initiate the query cancel at the same time, e.g., due to participant failure. If we implement the cancellation
as a broadcast from the cancel initiator to participants, then up to `N * N` of messages could be sent. With the two-step approach
no more than `2 * N` messages are sent: N from all cancel initiators to query initiator, N from query initiator to participants.
The two-step approach has higher latency, but latency is not important for the cancellation process.

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

If the query initiator is down, all participants will notice this eventually, and perform query cancellation locally without
sending any messages.

### 3.4 Data Exchange

This section describes how data is exchanged between participants.

The engine operates on **streams** of data. A stream is a sequence of rows sent from the child operator (upstream) to
the parent operator (downstream). A stream is uniquely identified by `[queryId, edgeId, senderMemberId, receiverMemberId]`,
where the `queryId` is a cluster-wide unique identifier of a query, and the `edgeId` is an identifier of the edge between
two remote operators, which is unique for the query.

A stream may produce zero, one or more batches with rows. A stream might be ordered or unordered depending on the plan. In 
the ordered stream, batches must be processed on the receiver in the same order as they produced on the sender. In the unordered
stream, batches might be processed in any order. To support the ordered case, every batch has a monotonically increasing 
ordinal, that is used by the receiver to define the correct processing order, as described in Section 1.3.

The original query plan is split into one or more **fragments**. The split occurs across relational operators that require
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
1. `batch` - a batch of rows sent from a sender to a receiver
1. `flow_control` - flow control message sent from a receiver to a sender

#### 3.4.1 Batches

Data is first accumulated in batches on the sender's side. The batch is then serialized and sent over the wire in the `batch`
message. The sender doesn't wait for an explicit ack for every batch from the receiver. Multiple batches could be sent one
after the other. The last batch in the stream has a marker, denoting the end of the stream.

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
    long ordinal;          // Monotonically increasing sequence number  
}
```

#### 3.4.2 Flow Control

The sender may produce new batches faster than the receiver could process them, so the flow control is needed. We implement
it using the credit-based approach:
1. Sender and receiver agree on the initial number of credits
1. Whenever the sender sends the batch, it decreases the number of credits by the value which depends on the batch size
1. When the number of credits on the sender reaches zero, the sender stops sending data
1. Receiver periodically increases the number of credits available to the sender and sends it back, so that the sender may
resume data exchange

One credit is equal to one byte because it is convenient for memory management purposes. A row could not be used as a unit of
credit because row size may vary significantly.

Precise conditions which trigger the `flow_control` message, as well as the initial amount of credits are implementation-defined
and are not part of the protocol.

Similarly to the `batch` message, the `flow_control` message has a monotonically increasing ordinal to handle out-of-order
delivery.

*Snippet 11: An example of control flow protocol*
```
S                          R
|                          |
|----batch[100]->----------| 
|------batch[80]->---------|
|--------batch[40]->-------|
|                          |
|----<-flow_control[90]----|
|                          |
|----batch[60]->-----------|  
```

*Snippet 12: `flow_control` message structure*
```java
class QueryFlowControlOperation {
    QueryId id;             
    int edgeId;
    long remainingCredits;
    long ordinal;            
}
```

### 3.5 Cleanup

The engine must ensure that the execution of a query is eventually completed on all members, either normally or due to an error.
Otherwise, resource leaks are possible. At some point in time, two members may have different views on whether the particular
query is active or not. This may happen due to network problems (including split-brain), or due to inherent race conditions
because query start and query finish are not synchronized between members. The following example demonstrates a race condition
which may lead to a memory leak on a participant.

#### 3.5.1 Example of a Race Condition

A batch may reach the participant after the query has been canceled. Such batch must be **discarded**.

*Snippet 13: Race condition with a stale data batch*
```
I              P1            P2
|----start->---|             |
|------start->---------------|
|----cancel->--|
|              |--<-batch----| // Batch for inactive query
|------cancel->--------------|
```

At the same time, a batch may arrive before the query execution is started. Such batch must be **retained**. The example below
demonstrates the problem.

*Snippet 14: Race condition with a data batch arriving before the query is started*
```
I              P1            P2
|------start->---------------|
|              |--<-batch----| // Batch for active query
|----start->---|             |
```

When the batch arrives, and there is no active query associated with the given ID, we cannot distinguish between the query
which has not started yet and the query which has been canceled. To mitigate this, we introduce a pair of messages to clean
the outdated query handles:

#### 3.5.2 Query Check

To ensure that a query is completed eventually on all members, we introduce a pair of messages:

1. `check` - verifies whether queries with the given IDs are still active on the query initiator
1. `check_response` - a response to the `check` message with the subset of suspected query IDs, which are no longer active on the
query initiator

A participant maintains a list of queries that are currently active. The participant may suspect that a query from the list is
already completed. In this case, the participant may send the `check` message with the list of suspected query IDs to the query
initiator. The query initiator ID could be derived from the query ID. The query initiator then iterates over the list of own
active queries and determines the list of suspected query IDs that are no longer active. This list of IDs of inactive queries is
sent back to the participant in the `check_response` message. The participant then releases the resources associated with
inactive queries.

The precise conditions when the `check` message is triggered are implementation-specific and are not part of the protocol.

*Snippet 15: Check protocol*
```
P                             I
|                             |
|----check[id1, id2]->--------| 
|                             |
|----<-check_response[id1]----|
```

*Snippet 16: `check` and `check_response` messages structure*
```java
class QueryCheckOperation {
    List<QueryId> suspectedQueryIds;
}

class QueryCheckResponseOperation {
    List<QueryId> inactiveQueryIds;
}
```

[1]: https://static.googleusercontent.com/media/research.google.com/en//pubs/archive/46103.pdf "Spanner: Becoming a SQL System"
[2]: 02-operator-interface.md "SQL Operator Interface"
