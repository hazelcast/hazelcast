---
title: Event Time-Based Processing
description: How Jet deals with event-time disorder with techniques such as watermarks.
id: version-4.0-event-time-processing
original_id: event-time-processing
---

In [Concepts](/docs/concepts/event-time) we introduced event time and,
especially, event disorder. Here we'll present some of the tricks Jet
uses to deal with it.

## The Watermark

The key event-time concept is the *watermark* which corresponds to the
notion of *now* in the processing-time domain. It marks a position in
the data stream and tells what time it is at that position. Check out
this diagram:

![The Watermark](assets/arch-eventtime-wm.svg)

The user has configured the maximum tolerable lag as `0:02`. We can see
that the watermark always reflects that and lags behind the event
timestamps by at most `0:02`. When an event arrives out of order, the
watermark lags behind it by less than `0:02` and, if the lag would be
negative (watermark is already past the event's timestamp), Jet
considers that event as a "fluke in the input" and ignores it. By this
time Jet already emitted the aggregation result for the window where
this event belongs so there's no point in dealing with it.

## Distributed Watermark

The picture above is the simplest and a very straightforward aspect of
dealing with event disorder. More serious problems start when we take
into account that a Jet processing task doesn't receive a single data
stream, but many streams from many parallel upstream tasks. We
illustrated this fact in the [DAG](/docs/concepts/dag) page.

## Coalescing the Distributed Watermark

Every such substream has its own watermark and we must find a way to
combine (*coalesce*) all of them into a unified watermark value observed
by the task. The invariant we want to protect is that an event that
wasn't late within its substream, can't become late in the merged flow.
This means that the coalesced watermark should be the lowest watermark
of any substream:

![Distributed Watermark](assets/arch-eventtime-wm-dist.svg)

The overall result is that there is no upper bound anymore on how much
the watermark can lag behind an event in the stream. The substream with
the highest latency holds back all others, but not just that: since the
watermark is purely data-driven, even a substream with no latency can
cause unbounded watermark lag simply by having low activity &mdash; a
lot of time passing between consecutive events. This can occur when
substreams come from a partitioned source. Since partitioning is based
on some key like user ID, it may happen that there are no users active
on a partition. While on others there are millions of events, on one
there is nothing for minutes.

Hazelcast Jet mitigates this problem by tracking the watermark on each
partition of a partitioned source, detecting when a partition has gone
idle, and starting to advance the watermark even in the absence of data.
