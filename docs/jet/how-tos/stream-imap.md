---
title: Receive IMap Change Stream
description: How to monitor the stream of change events happening to an `IMap.`
---

[IMap](/javadoc/{jet-version}/com/hazelcast/map/IMap.html)
is the main data structure in Hazelcast IMDG. Jet can use it as both a
data source and a data sink. It can serve as both a batch source, giving
you all the current data, and as a streaming source, giving you a stream
of change events.

Here we focus on the `IMap` as a streaming source.

## 1. Enable Event Journal in Configuration

To capture the change stream of an `IMap` you must enable its _event
journal_. Add this to your YAML config:

```yaml
  map:
    streamed-map:
      event-journal:
        enabled: true
```

## 2. Write the Producer

First let's write some code that will be updating our `IMap`. Put this
into your project:

```java
package org.example;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;

import java.util.concurrent.ThreadLocalRandom;

public class Producer {
    public static void main(String[] args) {
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.itemStream(100,
                (ts, seq) -> ThreadLocalRandom.current().nextLong(0, 1000)))
                .withoutTimestamps()
                .map(l -> Util.entry(l % 5, l))
                .writeTo(Sinks.map("streamed-map"));

        JobConfig cfg = new JobConfig().setName("producer");
        Jet.bootstrappedInstance().newJob(p, cfg);
    }
}
```

## 3. Write the Consumer

Now we are ready to write the pipeline that consumes and processes the
change events. It will tell us for each map key how many times per
second it's being updated.

```java
package org.example;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.JournalInitialPosition;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.WindowDefinition;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Consumer {

    public static void main(String[] args) {
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.<Long, Long>mapJournal("streamed-map",
                JournalInitialPosition.START_FROM_CURRENT))
                .withIngestionTimestamps()
                .window(WindowDefinition.tumbling(TimeUnit.SECONDS.toMillis(1)))
                .groupingKey(Map.Entry::getKey)
                .aggregate(AggregateOperations.counting())
                .map(r -> String.format("Key %d had %d updates", r.getKey(), r.getValue()))
                .writeTo(Sinks.logger());

        JobConfig cfg = new JobConfig().setName("consumer");
        Jet.bootstrappedInstance().newJob(p, cfg);
    }
}
```

## 4. Submit for Execution

Assuming you already started a Jet instance, use `jet submit` to first
start the producing job:

```bash
<path_to_jet>/bin/jet submit --class org.example.Producer <path_to_your_jar>
```

Then start the consuming job:

```bash
<path_to_jet>/bin/jet submit --class org.example.Consumer <path_to_your_jar>
```

You should start seeing output like this in the Jet member's log:

```text
...
2020-03-06 10:17:21,025 ... Key 2 had 24 updates
2020-03-06 10:17:21,025 ... Key 1 had 19 updates
2020-03-06 10:17:21,025 ... Key 0 had 17 updates
2020-03-06 10:17:21,025 ... Key 3 had 14 updates
2020-03-06 10:17:21,025 ... Key 4 had 26 updates
...
```

## 5. Clean Up

Cancel your jobs when you're done:

```bash
<path_to_jet>/bin/jet cancel consumer
<path_to_jet>/bin/jet cancel producer
```
