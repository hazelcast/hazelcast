# SQL Network Protocol

## Overview

Hazelcast Mustang is a distributed SQL engine. Network communication between nodes is required to produce the final result.
In this document, we describe the design of the communication protocol. This includes query initiation, data exchange,
query cancellation, and maintenance operations.

The remainder of this document is structured as follows. Section 1 describes the requirements of the engine which should be
satisfied. In Section 2 we discuss the existing networking infrastructure of the Hazelcast cluster and whether it satisfies
the requirements. Section 3 explains the protocol.

## 1 Requirements

In this section we summarize the key requirements to the engine which influences the protocol design decisions, which are:
1. Low latency
1. Fail-fast
1. Ordered data exchange

### 1.1 Low Latency

Hazelcast Mustang is a modern distributed SQL engine which targets the OLTP workloads operating on in-memory data. We expect
that the engine will be used mostly for queries, which should be completed in a matter of milliseconds or seconds. This
influences the protocol design significantly, because network calls still have relatively high latencies as of 2020. The network
protocol should be designed in a way, that members avoid waiting for each other when possible.

### 1.2 Fail-Fast

Distributed systems must account for network and hardware failures. Query execution is a stateful process. Whenever a participant
of a distributed query goes down, there are two options: try tolerating the failure and continue execution, of fail the query.

In order to tolerate the failure of a query participant, the system needs to track the progress of query execution. An example of
such a system is Google Spanner [[1]], which uses restart tokens to track the progress of the query, hiding network failures from
users. Another approach is to persist intermediate state to disk, which is used in OLAP engines where query restart may cause
inappropriate delays to user business processes.

While the idea of hiding intermittent failures from users is compelling from the usability point of view, it comes at a massive
engineering costs, as mentioned by Google Spanner engineers. The Hazelcast Mustang engine is designed with the **fail-fast**
behavior in mind: whenever a failure occurs, the query is terminated and a proper transient error is delivered to the user.
The user is expected to restart the query manually then. The query will fail in the following scenarios:
1. Member executing the query (participant) has left the cluster
1. Temporal network problem between two members which resulted in a broken TCP connection

We understand that fail-fast behavior might be not the best solution for some use cases, including machine shutdowns in cloud
environments. We therefore do not reject the idea to implement transparent handling of network errors in future releases.
Instead, we treat the fail-fast behavior as a starting point for future improvements, which provides a good trade-off between
the added value and implementation complexity.

### 1.3 Ordered Data Exchange

The Hazelcast Mustang engine employs the **Volcano Model** approach as described in [[2]]. Data flows from children relational
operators to parents operators. The order of rows transferred between operators is important for correctness. For example, a
parent operator may expect that the rows pulled from the child operator are sorted on a certain attribute.

Adjacent operators may be located on different members, exchanging data through network requests. The implementation must ensure
that the order of rows is preserved when they are being transferred over the wire.

## 2 Existing Infrastructure

The Hazelcast Mustange engine is a part of Hazelcast IMDG product, which is a distributed system with own networking
infrastructure, built on top of TCP/IP protocol. In this section we discuss the key networking abstractions used Hazelcast IMDG.
which are `Packet`, `Connection` and `Operation`. We then analyze how they are used in Hazelcast Mustang.

### 2.1 Packet

`Packet` is a chunk of data sent over the wire. The packet has length and type. **Length** defines the boundaries of the
packet's data within the network stream of data. Several packets could be sent in a single network request. Likewise, a
single packet could span multiple network requests. The original request is restored on the receiver side and submitted for
processing.A packet has a **type** that defines the handler for the packet. Examples are partition handler, generic handler,
Hazelcast Jet handler.

### 2.2 Connection

`Connection` abstracts out a peer-to-peer connection between two members.

Connections are established lazily between communicating members. The connection could be closed on idle timeout or error and
re-established later. Connection to a particular member is provided by the `EndpointManager` interface through
`getConnection()` and `getOrConnect()` methods. Subsequent calls to these methods may return different connection objects.

The order of the delivery of messages sent over a single `Connection` object is **not defined** in the general case. However,
the current implementations have relatively strong ordering and delivery guarantees, because internally they use a single socket
connection:
1. If a packet is delivered to a receiver, then all previous packets sent through the same connection are also delivered
1. If the sending of the packet A happens-before the sending of the packet B, then submission of the packet A for execution on
the receiver happens-before the submission of the packet B.

Hazelcast Jet relies on these guarantees.

### 2.3 Operation

Most Hazelcast components don't operate on packets directly. Instead, the `Operation` abstraction is used, which encapsulates the
command to be executed фтв additional information such as the caller ID, timeouts, retries, etc.

Operations are submitted to the `OperationService` which obtains the connection, serializes operations to packets, and manage
completion futures, timeouts, and retries.

### 2.4 Discussion

The Hazelcast Mustang engine doesn't use `Operation` and `OperationSerice` abstractions for the following reasons. First, to
satisfy the low-latency requirement, the engine doesn't use the request-response messaging pattern for query initiation and data
exchange. Second, due to fail-fast design choice, the engine doesn't need to keep data chunks on the sender side waiting for ack
from the receiver. The fire-and-forget approach is used instead. Last, since `Operation` and `OperationService` interfaces do
not guarantee that the same `Connection` object will be used between invocations, the ordering requirement cannot be satisfied.

Instead, `Packet` and `Connection` interfaces are used directly. For every stream of data we obtain the `Connection` object
from the connection manager. This object is used for the duration of query for the given stream, thus ensuring ordering of
data delivery.

Future implementations of the `Connection` interface must provide the ordering guarantees as described in Section 2.2.










## 2 Design

The engine works similarly to Hazelcast Jet. It doesn't use operations infrastructure, works with packets and
connections directly, and relies on consistent packet delivery order. The following sections explain the motivation.

### 2.1 Data Exchange

During the prepare phase, the original SQL query is converted into a tree of relational operators, following the Volcano Model
approach as described in [[2]]. Data flows from the child operators (upstream) to parent operators (downstream). The only
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
the parent operator (downstream). A stream is uniquely identified by `[queryId, edgeId, senderMemberId, receiverMemberId]`,
where the `queryId` is a cluster-wide unique identifier of a query, and the `edgeId` is an identifier of the edge between
two remote operators, which is unique for the query.

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

### 2.3 Operations

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

[1]: https://static.googleusercontent.com/media/research.google.com/en//pubs/archive/46103.pdf "Spanner: Becoming a SQL System"
[2]: 02-operator-interface.md "SQL Operator Interface"
