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

Therefore, the client should never route query execute requests to the lite members in the 4.1 and 4.2 releases. 

### 1.4 Resource Cleanup

When a query is started on the initiator, certain amount of system resources are allocated. The resources are released either
when all rows are consumed by the user, or when the explicit `close` command is invoked by the user.

Therefore, the implementation must support a distinct `close` command. Also, the implementation must release the resources when
the client disconnects.

## 2 Protocol

The protocol contains three commands: `execute`, `fetch`, and `close`.

The protocol also has the following common objects that are used across different commands:
- **SqlQueryId** - an opaque query identifier that is used to locate the server-side cursor on the member
- **SqlPage** - a finite collection of rows fetched from the initiator, together with the "end of data" marker. If the 
  "end of data" condition is observed, the cursor is closed on the server automatically and no separate `close` operation is 
  needed.
- **SqlError** - an object describing the SQL error that occurred on the server. We use a separate object because we pass an 
  error code in addition to the error message.

### 2.1 Execute Command

The `execute` command starts execution of a query on the member. 

The client generates the query ID locally, which allows cancelling the query without waiting for the response to the `execute` 
commands. Other request parameters are converted to the relevant fields of the server-side `SqlStatement` object.

The response contains either a pair of `rowMetadata` and `rowPage` for queries that produce a result set, or an `updateCount`.
If an error occurred on the server, these fields will be initialized with the default values, and the `error` field will contain
the description of the error.

```
request {
    sql : String 
    parameters : List<Data>
    timeoutMillis : long
    cursorBufferSize : int
    schemaName : String
    expectedResultType: byte
    queryId : SqlQueryId
}

response {
    rowMetadata : SqlRowMetadata
    rowPage : SqlPage
    updateCount: long
    error : SqlError
}
```

### 2.2 Fetch Command

The `fetch` command extracts the next page of rows from the server-side cursor via query ID. 

Data fetching is organized in a simple request-response fashion: one page is returned for every `fetch` request. This 
implementation might be not optimal for large result sets due to increased latency. An alternative implementation could stream
data from the member to the client without waiting for an explicit `fetch` request for every page. We decided not to do this in
4.1 due to time constraints. However, the design of the client protocol allows for such interaction, so we may implement the 
streaming protocol in the future. 

```
request {
    queryId : SqlQueryId
    cursorBufferSize : int
}

response {
    rowPage : SqlPage
    error : SqlError
}
```

The `fetch` command must be sent to the query initiator only. There are two reasons for this:
1. This guarantees the optimal performance, because the data is extracted with a minimal number of network requests
1. Currently, there is no way to re-route `fetch` requests from the one member to another. This limitation might be relaxed in
   the future.

### 2.3 Close Command

The `close` command stops query execution on the member, and releases the associated resources. The request could be sent
without waiting for the initial response from the `execute` command. If the server-side cursor with the given ID is not found, 
the special cancellation marker is associated with the query ID. If the `execute` request is received afterward, it is 
ignored, and the marker is cleared. If the `execute` request is not received during a certain time, the marker is cleared
to avoid memory leaks.

```
request {
    queryId : SqlQueryId
}

response {}
```

The server-side resources are cleared automatically when the end of data is reached. Therefore, it is not necessary to send
the `close` command to the member, if the previous `execute` or `fetch` command returned the last page, or an update count.
  
## 3 Limitations

This section describes limitations of the current implementation

### 3.1 No Support for Lite Members

Lite members cannot initiate the SQL queries, and we do not have routing of client requests between members. Therefore, the 
client cannot send `execute` requests to the lite members. 

This especially affects unisocket deployments: if a client is connected to a lite member, the query execution will fail
even though there could be data members in the topology. 

We will add missing routing logic in future versions, and remove this limitation. 
