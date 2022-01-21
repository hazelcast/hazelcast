---
title: Change Data Capture meets Stream Processing
description: Debezium based Change Data Capture sources for Hazelcast Jet
author: Bartók József
authorURL: https://www.linkedin.com/in/bjozsef/
authorImageURL: https://www.itdays.ro/public/images/speakers-big/Jozsef_Bartok.jpg
---

## Introduction

*Change Data Capture* (CDC) refers to the process of *observing
changes made to a database* and extracting them in a form usable by
other systems, for the purposes of replication, analysis and many more.

*Hazelcast Jet* is a distributed, lightweight stream processing
framework. It allows you to write modern Java code that focuses purely
on data transformation while it does all the heavy lifting of getting
the data flowing and computation running across a cluster of nodes. Jet
stores computational state in [fault-tolerant, distributed in-memory
storage](https://jet-start.sh/docs/api/data-structures), allowing
thousands of concurrent users granular and fast access to your data
without breaking a sweat.

While stream processing is a natural solution for providing insight into
many big-data workloads, it’s a relatively new evolution over its
predecessor - offline batch processing. Utilizing stream processing
effectively requires re-architecting existing systems to event-driven
architectures and introducing several new components. This process is
not always straightforward and also requires a shift in mindset.

In this context, the functionality provided by change data capture
technologies, for which Debezium is one of the, if not THE best
open-source alternative, is a godsend. To be able to ingest data from
relational databases, without affecting the applications that use them,
changes the game for streaming systems. It becomes possible to safely
extend old systems with all kinds of new functionality: real-time
analytics, complex event processing, anomaly & fraud detection and so
on.

## Integration

When we first considered integrating Debezium into Jet, the most
important decisions were centered around the fact that Debezium is
designed to be deployed via Apache [Kafka
Connect](https://kafka.apache.org/documentation/#connect), which then
takes care of *fault tolerance* and *scalability*. Fortunately, Jet is
fully capable of providing these crucial services. Also, Kafka Connect
is a good enough abstraction that we were able to mimic it for Debezium.

We are aware that Debezium also offers an *embedded mode* for
applications not interested in fault-tolerance guarantees such as
exactly-once processing and resilience, but since Jet does not have a
“dumbed down” version (even as full-blown is light enough to be
embedded), we quickly discarded this approach.

So, first, we added generic support for Kafka Connect sources to Jet,
which should be a valuable feature even outside the scope of CDC. Then
we used Debezium to build a Kafka Connect source for Jet. Well… “build”
might be overstating it. Debezium already is a Kafka Connect source. We
just had to make sure that Jet’s specific fault-tolerance mechanisms
will interact with it properly, through the Kafka Connect API.

## Synergy

One immediate benefit that Jet offers to Debezium users is eliminating
the need for *external services*. No Zookeeper, no Kafka needed. When
using Debezium through Jet, the latter takes care of the whole lifecycle
and fault tolerance of all the components involved. The setup is greatly
simplified.

Then, obviously, there is the *stream processing capability*, because
that’s what Jet does. Not only do you get access to the data, but you
also have the toolbox to process it, extract whatever insights you need
from it.

In addition, Jet also aims to offer *further convenience* wrappers when
the Debezium source is being used. For example:

* builders for the most common configuration properties to make setting
  up Debezium for some specific DB as simple as possible
* standard Java interfaces to give structure to the complex Debezium
  events
* JSON parsing, including mapping to Objects, based on [Jackson
  jr](https://github.com/FasterXML/jackson-jr), to simplify how parts
  of - or even entire Debezium events can be interpreted

For an example look at this sample from our [CDC
tutorial](https://jet-start.sh/docs/tutorials/cdc#6-define-jet-job). All
the code you would need to build an in-memory replica of your MySQL
database table would be something like:

```java
StreamSource<ChangeRecord> source = MySqlCdcSources.mysql("source")
        .setDatabaseAddress("127.0.0.1")
        .setDatabasePort(3306)
        .setDatabaseUser("debezium")
        .setDatabasePassword("dbz")
        .setClusterName("dbserver1")
        .setDatabaseWhitelist("inventory")
        .setTableWhitelist("inventory.customers")
        .build();

Pipeline pipeline = Pipeline.create();
pipeline.readFrom(source)
        .withoutTimestamps()
        .writeTo(CdcSinks.map("customers",
                r -> r.key().toMap().get("id"),
                r -> r.value().toObject(Customer.class).toString()));

JobConfig cfg = new JobConfig().setName("mysql-monitor");
Jet.bootstrappedInstance().newJob(pipeline, cfg);
```

## Architecture

I have stated above that when Debezium is integrated into Jet, the
latter takes on the role of service-provider as far as fault tolerance
and scalability are concerned.

Jet doesn't delegate its cluster management and fault tolerance concerns
to an outside system like ZooKeeper. It reuses the groundwork
implemented for Hazelcast IMDG: cluster management and the IMap, and
adds its own implementation of Chandy-Lamport distributed snapshots. If
a cluster member fails, Jet will restart the job on the remaining
members, restore the state of processing from the last snapshot, and
then seamlessly continue from that point. For further details, consult
our [documentation on the
topic](https://jet-start.sh/docs/next/architecture/fault-tolerance).

Extending this functionality umbrella to cover Debezium has been
surprisingly simple. All we had to do was to add Debezium’s
source offset to Jet’s snapshots. This way, whenever Jet needs to
execute a recovery, it passes the recovered offset to Debezium,
which in turn resumes the data flow from that offset.

One other thing we did and might be worth mentioning is that the Jet
integration also makes use of Debezium’s [new record state
extraction](https://debezium.io/documentation/reference/1.2/configuration/event-flattening.html)
SMT (Simple Message Transformation), for the purpose of message
structure simplification. With this transformation in effect, only the
"after" structure of the Debezium event envelope is processed by Jet.
However, whether this is a good idea or not, only time will tell. I
personally think that if and when we will start covering schema changes
more, we might end up re-enabling the full Debezium event content.

## Examples

The simplest example of using the Jet-Debezium integration would be our
[CDC tutorial](https://jet-start.sh/docs/next/tutorials/cdc) that I’ve
already mentioned above. A more involved one can be seen in my
colleague’s, Nicolas Fränkel’s [blog
post](https://jet-start.sh/blog/2020/07/16/designing-evergreen-cache-cdc).

## License

The Jet - Debezium integration is currently provided under the [Apache
License, Version 2](https://www.apache.org/licenses/LICENSE-2.0.txt),
just like Debezium and most of Jet (full details [here](https://jet-start.sh/license)),
so making full usage of the combination of the two should have no
impediments in your own projects.

## Looking ahead

At the moment of writing the Jet-Debezium integration is fully finished
only for MySQL and Postgres databases and has been [released in version
4.2](https://jet-start.sh/blog/2020/07/14/jet-42-is-released) of Jet.
Further work on covering more connectors and extending current
ones (for example by adding handling for database schema changes),
has not yet been scheduled.

The functionality provided by Debezium, the ability to allow modern
processing of legacy data is a great fit to Jet’s ability to carry out
that processing efficiently. The combination of the two has the
potential to become much more than the sum of their parts. I very much
look forward to finding out what this integration can lead to. Stay
tuned!
