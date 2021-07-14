---
title: Concerns Related to GC
description: Choosing the right JDK-GC combination is important.
id: version-4.2-gc-concerns
original_id: gc-concerns
---

Based on an [extensive test](/blog/2020/06/09/jdk-gc-benchmarks-part1)
we performed in June 2020 regarding the choice of JDK and GC, here are
some tips to get the most out of Hazelcast Jet.

## Upgrade Your JDK

If you are still on JDK 8, seriously consider upgrading. We found that
none of its garbage collectors are a match for the offerings of JDK 11,
which is the current version with Oracle's Long-Term Support (LTS).

## The G1 Collector is Great for Most Workloads

For batch workloads, as well as streaming workloads that can tolerate
occasional latency spikes of 2-3 seconds, the G1 collector is the best
choice because it has very good throughput and its failure modes are
graceful. It performs very well in a variety of workloads without any
tuning parameters. Its default target for the maximum stop-the-world GC
pause is 200 ms and you can configure it lower, down to 10 ms (using
`-XX:MaxGCPauseMillis`). Lower targets allow less throughput, though.
The mentioned 2-3 seconds latency (as opposed to the usual 200 ms)
occurs only in exceptional conditions with very high GC pressure. The
advantage of G1 over many other collectors is a graceful increase in
GC pause length under such conditions.

## For Latency Goals Below 10 ms, Use the Low-Latency GCs

If you aim for very low latencies (anything below 10 ms), your best bet
are the Shenandoah and ZGC collectors. They are still in their
experimental phases and under active development, so using the latest
JDK is highly recommended. Their maximum throughput is lower than G1,
which means you must provision more hardware for them.

In our tests we found that, in JDK version 14.0.2, ZGC performed
significantly better than Shenandoah, which still had some issues with
its pacer heuristics. ZGC was the only collector that maintained the
99.99th percentile latency below 10 millisecond across a significant
range of pipeline throughputs.

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
