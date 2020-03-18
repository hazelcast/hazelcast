# SQL Protocol

## 1 Overview

This document describes the communication protocol of the Hazelcast Mustang engine, which is built on top of the networking
design [[1]].

## 2 Definitions

This section describes the messages sent throughout the query life cycle, except for data exchange messages.

Let us define the following member types:
1. **Participant (P)** - a member participating in query execution
1. **Initiator (I)** - a participant, which started query execution, and which is responsible for coordination
1. **Cancel initiator (C)** - a participant which initiated query cancellation

## 3 Query ID

Query execution is a distributed process which spans one or more nodes. Every query execution is assigned with a unique
ID which is propagated with messages to participating members. The ID must be unique cluster-wide.

We thus define the query ID as follows:
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

The following messages are used:
1. `execute` - starts query execution
1. `cancel` - cancels the query
1. `check` - verifies whether queries with the given IDs are still active on the initiator
1. `checkResponse` - a response to the `check` message with the subset of suspected query IDs, which are still active on the
initiator

### 2.1 Query start

Before the query is started, the engine determines the list of participants. This list is fixed during query execution.

Query execution starts with the `execute` message being sent from the initiator to all participants:
```
I               P1  P2
|--[execute]->--|   |
|------[execute]->--|

```

TODO

### 2.2 Query cancel

TODO

### 2.3 Cleanup

TODO

## 3 Data exchange

### 3.1 Batches

TODO

### 3.2 Flow Control

TODO

[1]: 03-networking.md "SQL Networking"
