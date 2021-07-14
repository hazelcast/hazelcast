---
title: Introduction
description: Welcome to Hazelcast Jet!
---

Welcome to Hazelcast Jet!

Hazelcast Jet allows you to write modern Java code that focuses purely
on data transformation while it does all the heavy lifting of getting
the data flowing and computation running across a cluster of nodes. It
supports working with both bounded (batch) and unbounded (streaming) data.

These are some of the concerns Jet handles well:

* **Scale Up and Out**: Parallelize a computation across all CPU cores
  and cluster nodes
* **Auto-Rescale**: Scale out to newly added nodes and recover from
  nodes that left or failed
* **Correctness Guarantee**: *at-least-once* and *exactly-once*
  processing in the face of node failures

Jet integrates out of the box with many popular data storage systems
such as Apache Kafka, Hadoop, relational databases, message
queues and many more.

Jet supports a rich set of data transformations, such as windowed
aggregations. For example, if your data is GPS location reports from
millions of users, Jet can compute every user's velocity vector by using
a *sliding window* and just a few lines of code.

Jet also comes with a fully-featured, in-memory key-value store. Use it
to cache results, store reference data or as a data source itself.
