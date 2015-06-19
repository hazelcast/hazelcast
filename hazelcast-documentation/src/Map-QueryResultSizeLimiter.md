

### OOM Prevention

It is very easy to trigger an OOME with query based map methods, especially with large clusters or heap sizes. For example, on a 5 node cluster with 10 GB of data and 25 GB heap size per node, a single call of `IMap.entrySet()` fetches 50 GB of data and crashes the calling instance.

A call of `IMap.values()` may return too much data for a single node. This can also happen with a real query and an unlucky choice of predicates, especially when the parameters are chosen by a user of your application.

To prevent this, you can configure a maximum result size limit for query based operations. This is not a limit like `SELECT * FROM map LIMIT 100`, which can be achieved by a [Paging Predicate](#paging-predicate-order-limit). It is meant to be a last line of defense to prevent your nodes from retrieving more data than they can handle.

The Hazelcast component which calculates this limit is the `QueryResultSizeLimiter`.

#### QueryResultSizeLimiter

If the `QueryResultSizeLimiter` is activated, it calculates a result size limit per partition. Each `QueryOperation` runs on all partitions of a node, so it collects result entries as long as the node limit is not exceeded. If that happens, a `QueryResultSizeExceededException` is thrown and propagated to the calling instance.

This feature depends on an equal distribution of the data on the cluster nodes to calculate the result size limit per node. Therefore, there is a minimum value defined in `QueryResultSizeLimiter.MINIMUM_MAX_RESULT_LIMIT`. Configured values below the minimum will be increased to the minimum.

##### Local Pre-check

In addition to the distributed result size check in the `QueryOperations`, there is a local pre-check on the calling instance. If you call the method from a client, the pre-check is executed on the member which invokes the `QueryOperations`.

Since the local pre-check can increase the latency of a `QueryOperation` you can configure how many local partitions should be considered for the pre-check or you can deactivate the feature completely.

##### Scope of Feature

Besides the designated query operations, there are other operations which use predicates internally. Those method calls will throw the `QueryResultSizeExceededException` as well. Please see the following matrix to see the methods that are covered by the query result size limit.

![](images/Map-QueryResultSizeLimiterScope.png)

##### Configuration

The query result size limit is configured via the following system properties.

- `hazelcast.query.result.size.limit`
- `hazelcast.query.max.local.partition.limit.for.precheck`

Please refer to the [System Properties section](#system-properties) for explanations of these properties.
