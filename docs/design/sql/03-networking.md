# SQL Protocol

## Overview
Hazelcast Mustang is a distributed SQL engine. Network communication between nodes is required to produce the final result.
In this document, we describe the design of the communication protocol. This includes query initiation, data exchange,
query cancellation, and maintenance operations.

The remainder of this document is structured as follows. Section 1 describes the existing networking infrastructure of the
Hazelcast cluster. Section 2 explains how the engine's networking is built on top of the existing infrastructure.

## 1. Existing Infrastructure

Hazelcast relies on TCP protocol for data exchange. Key abstractions are `Packet`, `Connection` and `Operation`.

### 1.1 Packet

`Packet` is a chunk of data sent over the wire. The packet has length and type. **Length** defines the boundaries of the
packet's data within the network stream of data. Several packets could be sent in a single network request. Likewise, a
single packet could span multiple network requests. The original request is restored on the receiver side and submitted for
processing. **Type** defines the handler for the request. Examples are partition handler, generic handler, Hazelcast Jet
handler.

### 1.2 Connection

`Connection` abstracts out a peer-to-peer connection between two members.

Connections are established lazily between communicating members. The connection could be closed on idle timeout or error and
re-established later. Connection to a particular member is provided by the `EndpointManager` interface through
`getConnection()` and `getOrConnect()` methods. Subsequent calls to these methods may return different connection objects.

The order of the delivery of messages sent over a single connection object is not defined in the general case. However, the
current implementations have this guarantee. Hazelcast Jet relies on the internal ordering guarantee of the current
implementations.

### 1.3 Operation

Most Hazelcast components don't operate on packets directly. Instead, the `Operation` abstraction is used. It encapsulates the
command to be executed as well as additional information such as the caller ID, timeouts, retries, etc.

Operations are submitted to the `OperationService` which obtains the connection, serializes operations to packets, and manage
completion futures, timeouts, and retries.

## 2. Design

The engine works similarly to Hazelcast Jet. It doesn't use operations infrastructure, works with packets and
connections directly, and relies on consistent packet delivery order. The following sections explain the motivation.

### 2.1 Data Exchange

During the prepare phase, the original SQL query is converted into a tree of relational operators, following the Volcano Model
approach as described in [[1]]. Data flows from the child operators (upstream) to parent operators (downstream). The only
exception to this rule is nested-loop join (NLJ), where the parent operator may ask the child operator to re-execute,
possibly sending some data to it (e.g. bloom filter build from the join attributes). Hazelcast Mustang doesn't support NLJ at
the moment, so we assume that data always flows from children to parents. Future implementation of NLJ is likely to use the
separate protocol, which is out of the scope of this paper.

When the data is sent from a child node to a parent node, the order of rows is important for correctness and performance.
Examples, where the order is required for correctness are sorting, streaming aggregation, merge joins. But if the child
operator imposes a certain order, parent operators try to preserve it when possible because it allows for more optimizations
to be applied to upper operators.

Now we summarize the key design requirements:
1. Data flows in one direction, form child nodes to parent nodes
1. Order of rows between operators must be preserved

We now discuss how these requirements are satisfied in the Hazelcast Mustang design.

### 2.2 Streams

The engine operates on **streams** of data. A stream is a sequence of rows sent from the child operator (upstream) to
the parent operator (downstream). A stream is uniquely identified by `[queryId. edgeId, senderMemberId, receiverMemberId]`,
where the `queryId` is a cluster-wide unique identifier of a query, and the `edgeId` is an identifier of the edge between
two remote operators, which is unique for the query.

### 2.3 Buffering and Flow Control
Rows are buffered on the sender into batches, which reduces the total number of messages. Batches are then serialized
into packets and sent over the wire.

The receiver doesn't send the ack back to the sender for every batch. Instead, it sends control flow messages back to
the sender, which defines the number of bytes the sender may send to the receiver.

### 2.4 Ordering
Every stream uses the **same** `Connection` object for the duration of the query. This guarantees that the order of
received rows is equal to the send order. If a connection is broken in the middle of query execution, the query is
canceled with an error.

It is possible to relax the requirement to use the same `Connection` for the given data stream if the order is somehow
preserved at the engine level. This has two advantages: we no longer need to rely on the undocumented ordering guarantee
of the current `Connection` implementations, and it will be possible to continue the query execution in the events of short
network interrupts. But to maintain the order at the engine level, we would have to introduce TCP-like ordering
and packet loss detection, which requires more RAM and additional network messages, reducing performance dramatically.
This makes such design inappropriate for us.

Future implementations of the `Connection` interface must provide the ability for ordered packet transfer between members.
Both Hazelcast Mustang and Hazelcast Jet require this.

### 2.5 Operations

The engine doesn't use the existing `Operation` infrastructure for two reasons:
1. We need consistent ordering for some messages
1. Most engine's messages sent between members **do not require responses** by the design. Streams are the most prominent
example
1. We employ the **fail-fast** approach for the most failover scenarios. That is, instead of trying to do a recovery, we
prefer to fail the query and ask the user to restart it. This simplifies the implementation a lot, making it faster and less
prone to bugs, which is a sensible trade-off for early versions of the engine

As a result, the engine doesn't need the additional functionality provided by `Operation` and `OperationService` interfaces.
Neither we can use `OperationService` invocation infrastructure, because it doesn't allow for sticky connections. For this
reason, the engine's messages do not extend the `Operation` interface. Instead, messages are serialized to packets manually.

[1]: 02-operator-interface.md "SQL Operator Interface"
