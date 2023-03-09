# Partition-aware client

If we want SQL to be the primary interface to Hazelcast, the basic map get/put
operations using SQL have to be equally fast as their native counterparts. We’ve
implemented shortcut evaluation for the queries where the WHERE clause contains
`__key=?` - we don’t start a job in this case, but directly execute
`IMap.get()`. It’s also implemented for INSERT/SINK, UPDATE and DELETE
statements.

However, the operation is still much slower than the `IMap` counterparts. The
main design issue is that the client sending a query picks a random member to
execute the query, unaware of the partition owner.

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
partitionArgumentIndex: int
```

If its value is -1, then the client will keep using a random coordinator for
this query. If its argument is >=0, the next time the client sees the exact same
sql query text, it will calculate the partition key from that argument and pick
that key owner as the coordinator.

Assuming the member cached the query too, after this change the performance of
key-based SQL queries should get much closer to the native operations. The PR
can also include other micro-optimizations to more closely match the performance
of `IMap.get`.

# Other proposed solutions

## Using a special call to get the partition argument index

As an alternative it was proposed to add a new client operation to the protocol:
`returnPartitionAwareKey`. The client will send this operation for each new
`PreparedStatement` to determine the partition argument index. The result will
then be cached within the `PreparedStatement` for subsequent executions. The
benefit will be avoiding the need for a shared cache, and improved execution of
the first query.

Reasons against it are:

- it will slow down queries that do not benefit from partition-aware client by
  an extra round-trip.

- if the query does benefit from the partition-aware client, the first execution
  is improved only if the result is large, which is less likely for a query not
  accessing all the partitions. For smaller results, the positive effect of going
  directly to the correct member is lost by the extra call to find out the
  argument for the first execution.

For the above reasons we decided to not use this approach.

## Caching the partition argument index in the query object

It was proposed to cache the argument index in `PreparedStatement`, or an
equivalent in other languages. The benefit is a much simpler implementation and
more predictable behavior (cache hits/misses are predictable). It was rejected
for two reasons:

- such an object doesn't always exist, or is used in a different style. For
  example, our custom Java API uses `SqlStatement`, but this is only a shorthand
  for setting per-query options, it's not really required to be used. Python
  doesn't have a statement object at all.

- for the optimization to work, one must reuse the `PreparedStatement`
  instances. But they aren't thread-safe, so to reuse, one would have to use some
  kind of pool, which isn't commonly done. JDBC drivers commonly cache more
  expensive immutable state of the prepared statement in background caches,
  assuming that many `PreparedStatement` instances will be created for the same
  query.

Especially for the 2nd reason we rejected this idea.

## Using custom API to specify the partition argument index

In addition for per-statement or per-client cache, we can provide a custom API
so that an advanced user can set the partition argument index so that even the
first call is optimized.

This idea was rejected for the following reasons:

- In JDBC, the user will have to downcast the statement object. This is
  generally frowned upon.

```java
((HazelcastPreparedStatement) pstmt).setPartitionArgumentIndex(0);
```

- The benefit is small. The speedup of a single query is in the range of 100s of
  microseconds or less, the benefit is important if the same query is executed
  many times. Saving 100µs on an execution that takes seconds overall is
  negligible.

- Even though the implementation is simple, there's more work needed to document
  and support it.

- It can be added later.

# Client-wide cache specification

We expect read-heavy usage pattern for the cache. We propose hard-coded capacity
of around 1024 elements. If average query is 128 bytes, the payload size of the
full cache will be 132 kBytes. Some kind of LRU available cache can be used. For Java,
we propose this read-optimized cache:

Stores the entries in a {@link ConcurrentHashMap}, along with the last access
time. It allows the size to grow beyond the capacity, up to `cleanupThreshold`,
at which point the inserting thread will remove a batch of the eldest items in
two passes: first to determine the access time below which to remove from the
cache, and a second pass to actually remove them. The cleanup process isn't
synchronized and the cache is available during the cleanup for both reads and
writes. The cleanup process is guarded for concurrent execution by CAS-ing a
boolean. If there's a large number of writes by many threads, the one thread
doing the cleanup might not be quick enough and there's no upper bound on the
actual size of the cache.

The Java code is available here:
https://github.com/hazelcast/hazelcast/pull/22659/files#diff-7b6e5ea0a8c86d03effa6ce417f47cc1a2438a37b493faf8bb640e2b51e7224f

## Cache size configuration

We'll expose the cache size as a client configuration option. In that case we
could use a smaller default and the user can increase or decrease it.
The clean-up threshold can be hardcoded to: `min(cacheSize / 10, 50)`.

```java
    /**
     * Parametrized SQL queries touching only a single partition benefit from
     * using the partition owner as the query coordinator, if the partition
     * owner can be determined from one of the query parameters. When such a
     * query is executed, the cluster sends the index of such argument to the
     * client. This parameter configures the size of the cache the client uses
     * for storing this information.
     */
    public static final HazelcastProperty PARTITION_ARGUMENT_CACHE_SIZE
            = new HazelcastProperty("hazelcast.client.sql.partition.argument.cache.size", 1024);
```