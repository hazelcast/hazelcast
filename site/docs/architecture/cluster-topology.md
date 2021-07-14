---
title: Cluster Topology
description: Different ways to form a Jet cluster
---

Hazelcast Jet is a distributed system, consisting of several machines
talking over the network. In a topological sense, it is a *complete
graph*, having an edge between all vertices. Hazelcast Jet uses the term
"cluster member" or just "member" to denote a Jet instance participating
in a cluster. A common term is also "node", but we tend to avoid it due
to the similar term "vertex" used for the Jet DAG. Since Jet deploys the
entire DAG to all the members, data travels on all the network links.

Jet tries hard to relieve you from the task of manually forming and
maintaining the cluster. There are several algorithms a Jet instance can
use to automatically discover other instances that should form a
cluster. The default one is to use the IP Multicast protocol, which
allows it to work out-of-the-box in a non-production setting like your
office WiFi. In a cloud setting Jet uses the cloud providers'
proprietary APIs to find a list of machines in its local network
environment. You can find out more about that in the [Operations
Guide](../operations/discovery).

## The Coordinator

One of the members is is the *coordinator:*

![Jet Cluster is a Complete Graph](/docs/assets/arch-topo-1.svg)

The coordinator expands the Core DAG into the tasklet execution plan,
distributes it to all the other members, and takes the initiative in
moving the pipeline execution job through its lifecycle (initialize,
run, clean up) while the other members follow its commands and report
state changes.

![Coordinator creates and distributes the execution plan](/docs/assets/arch-topo-2.svg)

If a cluster member fails (leaves the cluster), the coordinator suspends
all the jobs, rescales them to the new cluster topology, and resumes
them.

The coordinator itself may fail, in that case the other members enter a
consensus protocol to elect a new one, which then proceeds as above to
restore all the running jobs.

## Embedded Jet Cluster

Since Hazelcast Jet is just a library, you can build it into your
own distributed application. If each instance of your application
starts a Jet instance, they will automatically form their own cluster,
and you can use the local Jet instance to interact with the Jet cluster,
just like you would normally use a Jet client.

![Embedded Jet Cluster](/docs/assets/arch-topo-3.svg)

If you are a Java developer working in an IDE, this is a very convenient
way to run a Jet cluster on your development machine. There is nothing
stopping you from starting your program several times, getting a Jet
cluster on the same physical machine.

Jet also allows you to run several instances inside a single application:

![Jet Embedded in a Single Application](/docs/assets/arch-topo-4.svg)

Even though they share a JVM, the instances will still use the usual
network-based discovery mechanisms and will work just as if they were
on separate machines.

One major convenience of the embedded architecture is that all the
classes the Jet pipeline uses are visible to Jet. When you submit a
pipeline to an outside cluster, you must be careful to include all the
classes within the job itself, but in the embedded mode you can just
submit it with no configuration.

While this is convenient and a great way to prepare self-contained
demos, it can also be deceiving and hide problems that you will
encounter in production. You are encouraged to start a small but
independent cluster on your development machine and submit the pipeline
as a client of that cluster (using the command-line tool `jet submit`).
