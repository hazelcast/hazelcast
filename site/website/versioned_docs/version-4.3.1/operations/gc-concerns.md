---
title: Concerns Related to GC
description: Choosing the right JDK-GC combination is important.
id: version-4.3.1-gc-concerns
original_id: gc-concerns
---

Based on an [extensive testing
campaign](/blog/2020/06/09/jdk-gc-benchmarks-part1) we performed in
June-August 2020, we extracted some points of advice on how to choose
the right JDK/GC combination and how to tune your setup to the workload
of your Hazelcast Jet data pipeline.

## Upgrade Your JDK

If you are still on JDK 8, seriously consider upgrading. We found that
none of its garbage collectors are a match for the offerings of JDK 11,
which is the current version with Oracle's Long-Term Support (LTS). The
JVM has been undergoing a phase of rapid development lately, which means
you can expect numerous improvements with each JDK version.

## The G1 Collector is Great for Most Workloads

For batch workloads, as well as streaming workloads that can tolerate
occasional latency spikes of 2-3 seconds, the G1 collector is the best
choice because it has very good throughput and its failure modes are
graceful. It performs very well in a variety of workloads without any
tuning parameters. Its default target for the maximum stop-the-world GC
pause is 200 ms and you can configure it lower, down to 5 ms (using
`-XX:MaxGCPauseMillis`). Lower targets allow less throughput, though.
The mentioned 2-3 seconds latency (as opposed to the usual 0.2 seconds)
occurs only in exceptional conditions with very high GC pressure. The
advantage of G1 over many other collectors is a graceful increase in GC
pause length under such conditions.

## For Latency Goals Below 10 ms, Consider a Low-Latency GC

If you aim for very low latencies (anything below 10 ms), you can
achieve it with G1 as well, but you will probably have to use the
`-XX:MaxNewSize` flag in order to constrain the Minor GC pause duration.
In our [test](/blog/2020/08/05/gc-tuning-for-jet) we found the values
`100m`-`200m` to work best over our range of throughputs, lower values
being better for lower throughputs.

If your data pipeline doesn't have too large state (i.e., less than a
million keys within a sliding window), you can consider the Z garbage
collector. We found it to work well without any tuning parameters. Its
current downside is that it handles less throughput compared to G1 and,
being non-generational, doesn't work well if you have a lot of static
data on the heap (for example, if your data pipeline has a `hashJoin`
stage). ZGC is an experimental collector under intense development, so
you can expect further improvements, including generational GC behavior,
in the future.

In our tests we found that as of JDK version 14.0.2, the other
low-latency collector, Shenandoah, still did not perform as well as ZGC
and latencies with it exceeded 10 ms in many cases.

### Reduce the Jet Cooperative Thread Pool

A concurrent garbage collector uses a number of threads to do its work
in the background. It uses some static heuristics to determine how many
to use, mostly based on the number of `availableProcessors` that the JVM
reports. For example, on a 16-vCPU EC2 c5.4xlarge instance:

- ZGC uses 2 threads
- G1 uses 3 threads
- Shenandoah uses 4 threads

The number of GC threads is configurable through `-XX:ConcGCThreads`,
but we found it best to leave the default setting. However, it is
important to find out the number of GC threads and set Hazelcast Jet's
`config/hazelcast-jet.yaml/instance/cooperative-thread-count` to
(`availableProcessors` - `ConGCThreads`). This will allow the GC threads
to be assigned to their own CPU cores, alongside Jet's threads, and thus
the OS can avoid having to interleave Jet and GC threads on the same
core.

A Hazelcast Jet data pipeline may use additional threads for
non-cooperative tasklets, in this case you may consider adjusting the
cooperative thread pool size even lower.

### Egregious Amounts of Free Heap Help Latency

The data pipeline in our tests used less than 1 GB of heap, but we
needed at least `-Xmx=4g` to get a good 99.99% latency. We also tested
with `-Xmx=8g` (less than 15% heap usage), and it made the latencies
even lower.

## For Batch Processing, Garbage-Free Aggregation is a Big Deal

In batch aggregation, once a given grouping key is observed, the state
associated with it is retained until the end of the computation. If
updating that state doesn't create garbage, the whole aggregation
process is garbage-free. The computation still produces young garbage,
but since most garbage collectors are generational, this has
significantly less cost. In our tests, garbage-free aggregation boosted
the throughput of the batch pipeline by 30-35%.

For this reason we always strive to make the aggregate operations we
provide with Jet garbage-free. Examples are summing, averaging and
finding extremes. Our current implementation of linear trend, however,
does generate garbage because it uses immutable `BigDecimal`s in the
state.

If your requirements call for a complex aggregate operation not provided
by Jet, and if you use Jet for batch processing, putting extra effort
into implementing a custom garbage-free aggregate operation can be
worth it.
