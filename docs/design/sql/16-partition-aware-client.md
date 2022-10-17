# Partition-aware client

If we want SQL to be the primary interface to Hazelcast, the basic map get/put
operations using SQL have to be equally fast as their native counterparts. We’ve
implemented shortcut evaluation for the queries where the WHERE clause contains
`__key=?` - we don’t start a job in this case, but directly execute
`IMap.get()`. It’s also implemented for INSERT/SINK, UPDATE and DELETE
statements.

However, the operation is still much slower than the `IMap` counterparts. The
main reason is that the client sending a query picks a random member to execute
the query, unaware of the partition owner.

The original idea to fix this was to use a kind of client-side plan cache. Along
with the query result, the cluster will send a plan to the client telling it
that the next time it sees that query, instead of sending it as a query it
should execute IMap.get directly. There are two issues with this approach:

1. cache invalidation: the member will have to keep track what plans were cached
   by which client, and will need to invalidate those 

2. client-side complexity: each type of operation will have to be implemented by
   each client. It’s not just simple `get`, but `get` with projections, additional
   filters, and also `put` for INSERT, and entry processor for UPDATE, which might
   not be possible to implement on non-java client.

Another option to implement this is much simpler on the client. Along with a
result, the cluster will send from which argument to derive the partition key,
and submit the query to the owner of that key. If the argument implements
`PartitionAware`, that should be taken into account. This approach doesn't have
any of the above disadvantages. Plan cache will remain only on members, and the
client will remain simple.

The following field can be added to `Sql.execute` response:

```
partitionArgument: int
```

If its value is -1, then the client will keep using a random coordinator for
this query. If its argument is >=0, the next time the client sees the exact same
sql query text, it will calculate the partition key from that argument and pick
that key owner as the coordinator.

Assuming the member cached the query too, after this change the performance of
key-based SQL queries should get much closer to native operations. The PR can
also include other micro-optimizations to more closely match the performance of
`IMap.get`.
