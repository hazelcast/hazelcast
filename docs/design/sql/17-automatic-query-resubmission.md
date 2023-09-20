# Automatic query resubmission

|||
|---|---|
|---|------------------- https://hazelcast.atlassian.net/browse/HZ-1098 https://hazelcast.atlassian.net/browse/HZ-1098 |
|Related Github issues|https://github.com/hazelcast/hazelcast/pull/21635|
|Document Status / Completeness| Complete                                          |
|Requirement owner| Sandeep Akhouri                                              |
|TDD Author|Viliam Ďurina|
|Developer(s)| Krzysztof Ślusarski |
### Background
#### Description

A customer perceives that AP is NOT implemented correctly in Hazelcast. If the
member hosting the primary partition is NOT available, their expectation is that
data should be transparently served from the backup. When the cluster is
changing topology, e.g., cluster member that contains the primary data is
killed, the client throws errors.

Automatic retries, with an upper bound on the number of retries, are implemented
with many IMap operations on the client. However, with SQL, the client cannot do
this, because it doesn't understand the query, and it doesn't know if the query
is side-effect-free and it's safe to be retried. Secondly, the client might
already have received some result rows, and if it retries the query, those rows
will be received again.

#### Scope

The scope is to add support for automatic SQL retries in Java client. Other
clients might follow, but it's not in the scope of the initial PR, as the
requirement is currently only for Java client.

There will be no retry mechanism for queries submitted through member instances.

#### Terminology

| Term           | Definition                            |
|----------------|---------------------------------------|
| Topology error | Query failure due to a member failure |

### Technical Design

The client doesn't understand the SQL statements it sends, and we want it to
remain this way in order to simplify the implementation in multiple languages.
We also don't want to retry statements by default, as not all are read-only.

We will analyze three scenarios:
1. Some, but not all rows were already received when the query fails
2. No rows were received yet, the query text starts with `SELECT` (case-insensitive, ignoring 
       white space) 
3. No rows were received yet, the query _doesn't_ start with `SELECT`

#### Resubmission policies

We propose that the user will be able to pick one of these retry policies:

1. `NEVER`: the current state. If a query fails, the failure is immediately 
   forwarded to the user
2. `RETRY_SELECTS`: the query will be retried if:
   - no rows were received yet
   - the SQL text starts with `SELECT` (case-insensitive, ignoring
     white space)
3. `RETRY_SELECTS_ALLOW_DUPLICATES`: as before, but query can be retried
   after it returned some rows. The already-returned rows will be returned again.
4. `RETRY_ALL`: all queries will be retried after a failure. This option is 
   dangerous as it will retry all queries, including DML, DDL etc. E.g. a retried
   `CREATE MAPPING` query can fail with "object already exists", if there's no `OR
   REPLACE` option and the first attempt actually succeeded, so the user will   
   receive an error even though it was successful, which is very confusing.

The default mode will be `NEVER` (b/w compatible).

#### Ways to pick resubmission policy

1. Ideally, we would be able to configure the retry policy per query, but that's
not possible. We could add this option to custom Java API (to the `SqlStatement`
class), but this is a non-standard API. We expect many clients to use JDBC or
other standardized APIs in other languages, which can't be extended in this way.

2. Another idea is that we could specify this as a query hint, e.g. in the form of
`SELECT /*+ ALLOW_RETRY */`. But for this we would have to parse the query on
the client, which we want to avoid. Another reason against this is that queries
are often generated, such as through ORM or other SQL tools, and injecting the
hint might not be possible or easy.

3. Another option is to use a session parameter, e.g. executing `SET
RETRY_STATEGY=<mode>`. The client will have to parse these commands, but we will
not have to provide a full SQL parser.

4. Provide a new option in `ClientConfig`, and a new JDBC URL parameter for 
JDBC. Other standardized APIs also provide these. The disadvantage is that it
will apply to all statements executed using that connection, that is also to DDL
statements etc., and that it can't be changed later without creating a new
connection.

We propose to implement options (3) and (4). Since the option (4) is feasible
for all modes, except for not-that-useful `RETRY_ALL` mode, we consider the
option (3) a nice-to-have feature.

Adding of options (1) .. (3) in the future is possible as it won't break the
backwards compatibility.

#### Explanation of "no rows received yet"

This means that the `SqlResult.iterator()` didn't ever return from `next()`.
However, there can be an ongoing blocking call to `next()` in another thread.

#### Other configuration options

The Java client retries invocations in this way: first 5 invocations are retried
without a delay (hardcoded value). Afterwards the delay is `2^<failureCount>`
ms, up to 1000 ms (configurable by the
`hazelcast.client.invocation.retry.pause.millis` property). After 120 seconds
(configurable by the `hazelcast.client.invocation.timeout.seconds` property), a
failure is reported to the user.

We think this scheme can be also used for SQL retries and there's no need to
provide more configuration options.

#### Error classification

Each exception delivered to the client has an [error
code](https://github.com/hazelcast/hazelcast/blob/713cef1b54b725e6c7df971ff52da30d1133a0a2/hazelcast/src/main/java/com/hazelcast/sql/impl/SqlErrorCode.java#L22).
This code isn't sufficient to decide whether to retry or not. More error codes
need to be added. If we retried in case of a problem that has no chance of being
resolved when executing the query again, it's a waste of time and the error will
be delivered much later to the user.

Since the use case is to increase availability during member failures, we
propose to retry only in specific cases (as opposed to _not_ retrying in
specific cases and retry in all other). We'll rather risk not retrying in a case
where we could retry, rather than risk retrying in cases where the error could
be reported immediately.

From the existing codes we need to retry in these cases:
- `CONNECTION_PROBLEM`
- `PARTITION_DISTRIBUTION`

These do not cover most errors thrown at member failures. We propose introducing
a new error code `TOPOLOGY_CHANGE` and use this code for various
topology-related failures.

We should avoid checking the error message or exception instance. The
client-side logic must remain simple so that it can be easily ported to other
clients in the future.

#### SQL commands that are queries, but don't start with SELECT

It's possible that a read-only statement doesn't start with `SELECT`. For
example, it can start with a comment, or with the `WITH` keyword for a common
table expression. For the sake of simplicity, we don't resubmit them in the
`RETRY_SELECTS` mode for now.

### Testing Criteria

Testing must have a unit test for the all common member failure cases and check,
that the query still succeeds.