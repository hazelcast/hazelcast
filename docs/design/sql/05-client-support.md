# SQL Client Support

## Overview

This document describes the design of the client protocol and the corresponding server-side implementation.    

The rest of this document is organized as follows. In section 1 we describe the requirements to the implementation. In 
section 2 we describe the protocol design and implementation concerns. In section 3 we summarize the limitations of the current
implementation.

## 1 Requirements

### 1.1 Pagination

The result set might be large enough so that it doesn't fit to client's memory. 

Therefore, data should be returned to the client in pages.

### 1.2 Message Routing

The Hazelcast Mustang engine is a distributed SQL query engine. When a query is submitted for execution, the engine splits
it into several fragments. The fragments are executed on cluster members. 

Members executing one or more fragments of the given query are called **participants**. The participant that started execution 
of the query is called **initiator**. Participants exchange data with each other. On the final execution stage participants send
data to the initiator to build the final result set. Only the initiator can return the final result set. 

Therefore, the implementation should be able to route all requests related to the given query to the query initiator. 

### 1.3 Lite Members

At the moment lite members cannot start queries. I.e. a lite member can never be a query initiator. Note that this limitation
is different from other Hazelcast subsystems. Normally, if the operation cannot be executed on the lite member, it is handed 
over to the normal member transparently. This is not the case for the SQL purely due to time constraints: we do no have enough
time to implement it in 4.1. This limitation will be removed in future versions.

Therefore, the client should never route query execute requests to the lite members in the 4.1 release. 

### 1.4 Resource Cleanup

When a query is started on the initiator, certain amount of system resources are allocated. The resources are released either
when all rows are consumed by the user, or when the explicit `close` command is invoked by the user.

Therefore, the implementation must support a distinct `close` command. Also, the implementation must release the resources when
the client disconnects.

## 2 Protocol

The protocol contains three commands: `execute`, `fetch`, and `close`.

### 2.1 Common Entities

The protocol contains the following common entities that are used across different commands:
- **Query ID** - an opaque query identifier that is used to locate the server-side cursor on the member
- **Data page** - a finite collection of rows fetched from the initiator, together with the "end of data" marker. If the 
"end of data" condition is observed, the cursor is closed on the server automatically and no separate `close` is needed.
- **SqlRow** - an object containing row data. At the moment we encode each separate value as `Data`. In the future we are likely
to replace it with specialized types for every SQL data type.
- **SqlError** - an object describing the SQL error that occurred on the server. We use a separate object because we pass an 
error code in addition to the error message.

### 2.1 Execute Command

The `execute` command starts execution of a query on the member. The response contains query ID, the data page, and metadata.

The command must never be sent to a lite member in 4.1.

```
request {
    sql : String 
    parameters : List<Data>
    timeoutMillis : long
    cursorBufferSize : int
}

response {
    queryId : SqlQueryId
    rowMetadata : SqlRowMetadata
    rowPage : List<SqlRow>
    rowPageLast : boolean
    error : SqlError
}
```

### 2.2 Fetch Command

The `fetch` command extracts the next page of rows from the server-side cursor via query ID. 

Data fetching is organized in a simple request-response fashion: one page is returned for every `fetch` request. This 
implementation is not optimal for large result sets due to increased latency. A better implementation will stream data from the 
member to the client without waiting for an explicit `fetch` request for every page. We decided not to do this in 4.1 due to 
time constraints. However, the design of the client protocol allows for such interaction, so we may implement the streaming 
protocol in the future. 

The command must be sent to the query initiator for two reasons:
1. This guarantees the optimal performance, because the data is extract with a minimal number of network requests 
1. Currently, there is no way to re-route `fetch` requests from the one member to another. This limitation might be relaxed in 
the future.

```
request {
    queryId : SqlQueryId
}

response {
    rowPage : List<SqlRow>
    rowPageLast : boolean
    error : SqlError
}
```

### 2.3 Close Command

The `close` command stops query execution on the member, and releases the associated resources. 

If the previous `execute` or `fetch` response returned "end of data" flag (`rowPageLast == true`), then it is not necessary 
to send this command to the member.

If the server-side cursor with the given ID is not found, the command is no-op.

The command must be sent to the query initiator for reasons similar to the `fetch` command.

```
request {
    queryId : SqlQueryId
}

response {}
```
  
## 3 Limitations

This section describes limitations of the current implementation

### 3.1 No Backward Compatibility

The protocol is not backward compatible at the moment, because the SQL feature is declared as beta. Messages could be changed 
in an incompatible way in minor and patch versions. 

In order to ensure that the user receives proper error messages in case of incompatibility, we will increment the ID of the 
message every time it is changed in an incompatible way. The user will receive an error message about the version mismatch.

When the SQL is no longer in beta, the SQL protocol will become backward/forward compatible, similarly to other Hazelcast 
components.  

### 3.2 Stickiness

There is no server-side logic to re-route client requests between members. Therefore, the protocol is sticky: `fetch` and 
`close` commands must be sent to the same member as the prior `execute` command.

We are likely to add missing routing logic in future versions. However, stickiness guarantees the optimal performance. Therefore,
we will still employ the sticky approach on the best-effort basis even after the limitation is resolved. 

### 3.3 No Support for Lite Members

Lite members cannot initiate the SQL queries, and we do not have routing of client requests between members. Therefore, the 
client cannot send `execute` requests to the lite members. 

This especially affects unisocket deployments: if a client is connected to a lite member, the query execution will fail
even though there could be data members in the topology. 

We will add missing routing logic in future versions, and remove this limitation. 
