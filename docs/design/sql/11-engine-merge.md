# Engine Merge Analysis

## Current Situation

Historically, Hazelcast has two separate engines: one in the IMDG module
and another in Jet module. They provide very similar execution model
based on a DAG, however they differ in many details. The main reason was
that IMDG and Jet were two products so we needed another engine that
could run without Jet. Another reason was the different objectives of
the engines - IMDG engine focused on smaller queries while Jet engine
focused on large batches and streaming queries.

However, maintaining two engines often doubles the cost of adding and
maintaining new features. For example, adding a support for `LIMIT`
clause needs to be done completely separately in each engine, even
though the design of the feature is very similar in both cases.
Secondly, this approach creates feature gaps when some features are
supported in only one of the engines.

Currently, a query is parsed first by IMDG engine. If it cannot run it,
then Jet engine is tried.

## Summary of Features

All of the features can in theory be added to both engines. Here we list
extra features of each of the engines. They indicate the amount of work
needed to do on the other engine, should an engine be removed.

### Features Implemented Only in IMDG Engine

- Better reading from IMap (can use indexes)

- LIMIT/OFFSET

- ORDER BY (only using a sorted index currently)

- Fast query roundtrip

- Plan cache

- Dynamic parameters (`?` in the query)

### Features Implemented Only in Jet Engine

- Non-imap connectors (Kafka, CSV/JSON/Avro local or remote file
  (hadoop, s3, amazon, gcp))

- Migration-tolerant when reading IMaps

- Streaming support: queries with unbounded results

- Aggregate functions, `GROUP BY` - with no memory management. (This is
  WIP in imdg, but only allows grouping by indexed columns - due to memory
  concerns)

- Data generators (`generate_series`, `generate_stream`)

- Enrichment join (can join IMap to anything except to IMap)

- JSON serialization in IMap

- `CREATE MAPPING` (also enables working with empty maps in IMDG)

- Persistent jobs (`CREATE JOB`)

- Job management (suspend, resume, restart)

- Fault tolerance (exactly-once, at-least-once), automatic scale-up

- Operator parallelism

- Job metrics

- Visibility of jobs in Management Center

- `information_schema`, `SHOW MAPPINGS`, `SHOW JOBS`

- `INSERT INTO` and `SINK INTO` (connectors: IMap, Kafka, files)

- Engine is resilient to packet loss - it will be detected and a failure
reported

### Common Features

Changes in this areas don't require double implementation:

- APIs (including JDBC or the API in any HZ client)

- Supported data types

- Simple operators and functions, i.e. those that are just a projection
  in the plan like `+`, `ABS`, `SIN`, but not `SUM` or `TUMBLE` etc.

- Type coercion for the above functions (e.g. the resulting type of
  `TINYINT + REAL` or the error for `INT + VARCHAR`)

- Dialect (quoting style, case sensitivity, literal syntax...)

## Engine Architecture

### Basic Worker

The basic unit (`Tasklet` or `Processor` in Jet and `Exec` in IMDG) are
single-threaded. In Jet they are run on either cooperative or dedicated
threads. There's a fixed number of cooperative threads, each can run any
number of cooperative tasklets. Cooperative tasklets have very strict
requirements which substantially complicate the development: they must
not block, and they must not spend too much time in one call. However,
they function as light-weight threads and are very cheap to switch.
Dedicated threads are used for tasklets using blocking APIs, especially
in connectors.

In IMDG engine, `Exec` (wrapped in `QueryFragmentExecutable`) is
scheduled to a `ForkJoinPool`. When it's done or needs more data, it
terminates and is scheduled again when the upstream node notifies it.
Currently there's no limit to how much time can one scheduled execution
spend, but a limit can easily be added to allow for better CPU sharing
if there are many fragments to execute.

### Networking

In Jet, the processors add to queues, some of which are local and some
remote. Remote queues are connected to `SenderTasklet`, which serializes
the items and sends a batch of them as one packet, directly to the
connection object, which is handled by the member's network threads. On
the target member, the member's network threads add the packet to the
`ReceiverTasklet`'s queue (a `MPSCQueue` in this case). The
`ReceiverTasklet` deserializes them and adds them to input queues of
target processors. The sender and receiver tasklets are cooperative.

In Jet, to avoid possible packet loss or reordering of items sent over
the network, we use the lower-level `Connection` objects. A connection
glitch that is normally handled without a cluster topology change, will
cause the query to fail. However, in case of Jet, if the job is
fault-tolerant, it can restart from the latest snapshot and continue.

In IMDG, query plan is split into one or more query fragments. Fragments
are connected with each other via edges with unique IDs. Every fragment
has one or more inputs and strictly one output. Inputs could be either a
concrete data source (e.g., `IMap`), or a receiver operator. Output is
either a sender operator, or a user cursor. Fragment is a vertex in the
DAG, and a pair of sender/receiver operators is an edge.

IMDG also uses `Connection` objects to send messages, bypassing the
invocation subsystem. The engine is resilient to packet reordering, but
currently not resilient to packet loss - a lost packet can cause the
query to get stuck or consume large amounts of memory - an issue that
can be fixed.

### Queues

Jet uses single-consumer, single-producer concurrent queues that are
wait free. This is a requirement for cooperative processing - other
queue types can block waiting for locks.

### Backpressure

The mechanism in both engines is very similar: senders have a budget,
receivers periodically update it.

In Jet, backpressure between local processors is handled by using
fixed-capacity queues - if a processor can't add more, it will back off.
Items sent between processors on different members is handled using
approach similar to RWIN in TCP: the receiver regularly communicates how
many items it was able to process and the sender must not send more. The
rwin values for all jobs are sent in one packet, in a compact structure.

In IMDG, a pair of a sender and a receiver agrees on the initial number
of bytes (aka "credits") that the sender may send to the receiver. With
every outbound message, sender decreases the number of available
credits. When the number of credits reaches zero, the sender stops
sending messages to receiver. The Receiver periodically responds with a
special `flow_control` message that increases the number of credits on
the sender.

### Load balancing

Jet engine doesn't use any load-balancing. Partition skew can lead to
resource under-utilization withing a single query. Assignment to threads
is deterministic, tasklets are assigned in a round-robin way, so two
queries are deterministically assigned to different threads, but in a
specific situation it can happen that most important tasklets are
assigned to one thread, while other threads are under-utilized. Two
tasks of different size (a small vs. a large query) interleave well.
However the cases where this would be an issue are rare.

The cooperative threading model is also prone to issues of incorrect
implementations not following the rules for cooperative processors. This
is mitigated by the fact that most processors that will be needed for
SQL are already implemented in Core Jet.

IMDG engine exploits the load-balancing features of `ForkJoinPool` and
doesn't suffer from the above issues. If limits to the number of items
processed before returning is added, no issue from load imbalance is
expected.

### Memory Management

Neither of the engines currently implements memory management. The
design of memory management in both engines will be conceptually
similar.

### Idle CPU load issue

Jet engine cooperative threads run the assigned tasklets in an infinite
loop. If none of the assigned tasklets made progress, the thread sleeps
for a little while. However, if just one of many tasklets made progress,
all tasklets are called again immediately in a next iteration. This
causes some CPU usage even when no job is running (<1% per core
typically, but can be much higher if there are hundreds of idle
streaming jobs). Another aspect is that the CPU usage is high under
light streaming load. A ballpark figure is 80% cpu usage at 20% of the
maximum throughput. This issue doesn't affect batch jobs because they
run at maximum throughput.

The issue can be alleviated by increasing the backoff timeout, at the
price of higher latency in streaming jobs. A very high timeout will also
reduce the throughput of batch jobs. We can also implement less naive
tasklet calling, but no research was done in this area.

IMDG engine reschedules tasks to FJP when it has new data and therefore
doesn't have these issues.

### Low performance on Windows

The back-off in cooperative thread uses `LockSupport.parkNanos()` which
on Windows has a [minimum sleep time of 15
ms](https://hazelcast.com/blog/locksupport-parknanos-under-the-hood-and-the-curious-case-of-parking-part-ii-windows/)
(or 1 ms under some conditions), even though we request 50µs. This
substantially reduces both throughput and latency. Due to this we do not
support Windows for production use for Jet engine at all.

## Performance Comparison of Current Versions

Jet engine currently substantially lags in short job performance.
Comparing queries returning 1 or 100 rows would give figures many times
lower. We think these issue are fixable and can be brought on par with
IMDG engine.

To compare the engines we designed a benchmark with substantial number
of items, 1M in our case. To make the comparison fair, we also used
local parallelism of 1 for Jet because operator parallelism isn't
implemented in IMDG engine. We've also set the partition count to 2
because Jet off-loads the map reading to partition threads so it
benefits from more parallelism than what's currently implemented in
IMDG.

We ran the benchmark on two AWS `c5.xlarge` members.

| Cluster type | Time |
| --- | ---: |
| IMDG | 847ms |
| Jet | 290ms |

We understand that this benchmark has quite low value because it doesn't
directly measure the processing performance of each of the engines
because the engines use a very different way to read IMaps, but we think
it indicates that the engines are in the same arena.

Benchmark code:
```java
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.core.processor.SourceProcessors;
import com.hazelcast.jet.function.RunnableEx;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.map.IMap;

import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.jet.core.Edge.between;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class JetImdgBenchmark {

    private static int numItems;
    private static int jetParallelism;
    private static int warmUpIterations;
    private static int measuredIterations;

    private static HazelcastInstance hzInst;

    public static void main(String[] args) {
        if (args.length != 5) {
            System.err.println("Usage:");
            System.err.println("  JetImdgBenchmark <jet|imdg> <numItems> <jetParallelism> <warmUpIterations> <measuredIterations>");
            System.exit(1);
        }

        boolean isJet = "jet".equalsIgnoreCase(args[0]);
        numItems = Integer.parseInt(args[1]);
        jetParallelism = Integer.parseInt(args[2]);
        warmUpIterations = Integer.parseInt(args[3]);
        measuredIterations = Integer.parseInt(args[4]);
        
        if (isJet) {
            testJet();
        } else {
            testImdg();
        }
    }

    public static void testJet() {
        JetInstance inst = Jet.newJetClient();
        try {
            hzInst = inst.getHazelcastInstance();
            prepareData();

            DAG dag = new DAG();
            Vertex src = dag.newVertex("src", SourceProcessors.readMapP("m"))
                            .localParallelism(jetParallelism);
            Vertex sink = dag.newVertex("sink", Processors.noopP());
            dag.edge(between(src, sink).distributeTo(hzInst.getCluster().getMembers().iterator().next().getAddress()));

            Pipeline p = Pipeline.create();
            p.readFrom(Sources.map("m"))
             .writeTo(Sinks.noop());

            runTest(() -> inst.newJob(p).join());
        } finally {
            inst.shutdown();
        }
    }

    public static void testImdg() {
        ClientConfig config = new ClientConfig()
                .setClusterName("jet");
        hzInst = HazelcastClient.newHazelcastClient(config);
        try {
            prepareData();

            runTest(() ->
                    hzInst.getSql().execute("select * from m").iterator().forEachRemaining(r -> { }));
        } finally {
            hzInst.shutdown();
        }
    }

    private static void runTest(RunnableEx test) {
        for (int i = 0; i < warmUpIterations; i++) {
            System.out.println("warmup " + i);
            long start = System.nanoTime();
            test.run();
            long elapsed = System.nanoTime() - start;
            System.out.println("### warmup iteration " + i + " took " + NANOSECONDS.toMillis(elapsed));
        }

        long totalTime = 0;
        for (int i = 0; i < measuredIterations; i++) {
            System.out.println("real " + i);
            long start = System.nanoTime();
            test.run();
            long elapsed = System.nanoTime() - start;
            totalTime += elapsed;
            System.out.println("### real iteration " + i + " took " + NANOSECONDS.toMillis(elapsed));
        }
        System.out.println("### avg time = " + NANOSECONDS.toMillis(totalTime / measuredIterations));
    }

    private static void prepareData() {
        IMap<Long, Long> imap = hzInst.getMap("m");
        Map<Long, Long> tmpMap = new HashMap<>();
        int batchSize = 100_000;
        for (long i = 0; i < numItems; ) {
            for (int j = 0; j < batchSize; j++, i++) {
                tmpMap.put(i, i);
            }
            imap.putAll(tmpMap);
            System.out.println(i);
            tmpMap.clear();
        }
        if (imap.size() != numItems) {
            throw new AssertionError();
        }
    }
}
```

### Performance of short-running queries

Historically, Jet focused on large batch and stream processing.
Therefore the job initialization time wasn't a concern. To bring the
performance of very short jobs on par with IMDG, we need to implement
the following:

- Finish the [light job
  prototype](https://github.com/viliam-durina/hazelcast-jet/tree/light-job).
  It merges the `init`, `execute` and `complete` operations into one.
  This won't be trivial because various new races will appear, but it's
  possible. We might do it only for non-fault-tolerant jobs.

- Add a _light job_ mode that will not persist the job metadata in any
  IMap, that will not use the `JetClassLoader`. The operation of the
  coordinator will not require any remote calls except for forwarding
  the work to members.

- Enable any member to coordinate the job, not just the master member.
  With this, queries submitted from a non-master member will not have
  additional hop to the master.

- Add plan caching, dynamic parameter support.

After these changes we expect the performance to be comparable also for
short-running queries.

## Conclusion

The engines are quite similar and functionally both can support all
required features, though with different characteristics at specific
scenarios. We didn't identify any inherent characteristic in that would
prevent implementing all of the features.

Because the Jet engine currently has more features (especially the fault
tolerance) and is more mature, we propose to add missing features to
this engine and remove the IMDG engine.
