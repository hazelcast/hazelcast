---
title: Connect to Amazon Kinesis
description: How to use Jet to write data into and read data from Amazon Kinesis Data Streams.
id: version-4.4-kinesis
original_id: kinesis
---

*Since*: 4.4

[Amazon Kinesis Data
Streams](https://aws.amazon.com/kinesis/data-streams/) (KDS) is a
massively scalable and durable real-time data streaming service. It can
be easily combined with Jet for building any number of useful data
pipelines. KDS can be used by Jet either as a data source or as a data
sink.

Let's build sample pipelines to illustrate. One will generate a
continuous flow of simulated tweets and push them into KDS, and the
other will consume the flow and compute its traffic intensity (events
per second).

## 1. Setup Amazon Kinesis

Using Jet is simple. Once set up, Amazon Kinesis also works nicely.
Getting to that point, however, is not always painless. For doing the
setup required, we will link to the "Stock Trade Stream" tutorial Amazon
offers as part of their developer guide.

### Create Data Stream

Follow the [steps from the KDS Developer
Guide](https://docs.aws.amazon.com/streams/latest/dev/tutorial-stock-data-kplkcl2-create-stream.html).
The only difference is that we should create a data stream named
"Tweets" (instead of "StockTradeStream").

### Setup Permissions

Follow the [steps from the KDS Developer
Guide](https://docs.aws.amazon.com/streams/latest/dev/tutorial-stock-data-kplkcl2-iam.html).
For the sake of this tutorial, depending on your security constraints,
it might be acceptable to enable all permissions for the needed
services. Again keep in mind that we are setting up the "Tweets" stream,
not "StockTradeStream."

### Check Everything Works

The simplest way to check that previous steps were successful and that
we are ready to continue with the tutorial is to [install the AWS
Command Line
Interface](https://docs.aws.amazon.com/streams/latest/dev/kinesis-tutorial-cli-installation.html)
and [perform some basic operations with
it](https://docs.aws.amazon.com/streams/latest/dev/fundamental-stream.html).

Please keep in mind that if your situation differs from the default,
both the Jet Kinesis sources and sinks have methods for overriding
backend parameters like region, endpoint, and security keys.

## 2. Start Hazelcast Jet

1. Download Hazelcast Jet

```bash
wget https://github.com/hazelcast/hazelcast-jet/releases/download/v4.4/hazelcast-jet-4.4.tar.gz
tar zxvf hazelcast-jet-4.4.tar.gz && cd hazelcast-jet-4.4
```

If you already have Jet, and you skipped the above steps, make sure to
follow from here on (just check that `hazelcast-jet-kinesis-4.4.jar` is
in the `lib/` folder of your distribution, because you might have
the slim distribution).

2. Start Jet:

```bash
bin/jet-start
```

3. When you see output like this, Hazelcast Jet is up:

```text
Members {size:1, ver:1} [
    Member [192.168.1.5]:5701 - e7c26f7c-df9e-4994-a41d-203a1c63480e this
]
```

From now on we assume Hazelcast Jet is running on your machine.

## 3. Create a New Java Project

We'll assume you're using an IDE. Create a blank Java project named
`kinesis-tutorial` and copy the Gradle or Maven file into it:

<!--DOCUSAURUS_CODE_TABS-->

<!--Gradle-->

```groovy
plugins {
    id 'java'
}

group 'org.example'
version '1.0-SNAPSHOT'

repositories.mavenCentral()

dependencies {
    implementation 'com.hazelcast.jet:hazelcast-jet:4.4'
    implementation 'com.hazelcast.jet:hazelcast-jet-kinesis:4.4'
}
```

<!--Maven-->

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>org.example</groupId>
    <artifactId>kinesis-tutorial</artifactId>
    <version>1.0-SNAPSHOT</version>

    <properties>
        <maven.compiler.target>1.8</maven.compiler.target>
        <maven.compiler.source>1.8</maven.compiler.source>
    </properties>

    <dependencies>
        <dependency>
            <groupId>com.hazelcast.jet</groupId>
            <artifactId>hazelcast-jet</artifactId>
            <version>4.4</version>
        </dependency>
        <dependency>
            <groupId>com.hazelcast.jet</groupId>
            <artifactId>hazelcast-jet-kinesis</artifactId>
            <version>4.4</version>
        </dependency>
    </dependencies>
</project>
```

<!--END_DOCUSAURUS_CODE_TABS-->

## 4. Publish a Stream to Kinesis

This code publishes "tweets" (just some simple strings) to the Kinesis
 data stream `Tweets`, with varying intensity:

```java
package org.example;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.kinesis.KinesisSinks;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.test.TestSources;

import java.util.Map.Entry;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.hazelcast.jet.Util.entry;

public class TweetPublisher {

  public static void main(String[] args) {
    Pipeline p = Pipeline.create();
    p.readFrom(TestSources.itemStream(3))
     .withoutTimestamps()
     .flatMap(event -> {
       ThreadLocalRandom random = ThreadLocalRandom.current();
       long count = random.nextLong(1, 10);

       Stream<Entry<String, byte[]>> tweets = LongStream.range(0, count)
           .map(l -> event.sequence() * 10 + l)
           .boxed()
           .map(l -> entry(
               Long.toString(l % 10),
               String.format("tweet-%0,4d", l).getBytes())
           );

       return Traversers.traverseStream(tweets);
     })
     .writeTo(KinesisSinks.kinesis("Tweets").build());

    JobConfig cfg = new JobConfig().setName("tweet-publisher");
    Jet.bootstrappedInstance().newJob(p, cfg);
  }
}
```

You may run this code from your IDE, and it will work, but it will
create its own Jet instance. To run it on the Jet instance you already
started, use the command line like this:

<!--DOCUSAURUS_CODE_TABS-->

<!--Gradle-->

```bash
gradle build
<path_to_jet>/bin/jet submit -c org.example.TweetPublisher build/libs/kinesis-tutorial-1.0-SNAPSHOT.jar
```

<!--Maven-->

```bash
mvn package
<path_to_jet>/bin/jet submit -c org.example.TweetPublisher target/kinesis-tutorial-1.0-SNAPSHOT.jar
```

<!--END_DOCUSAURUS_CODE_TABS-->

Let it run in the background while we go on to creating the next class.

## 5. Use Jet to Analyze the Stream

This code lets Jet connect to Kinesis and show how many events per
 second were published to the Kinesis stream at a given time:

```java
package org.example;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.kinesis.KinesisSources;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.pipeline.WindowDefinition.sliding;

public class JetJob {
  static final DateTimeFormatter TIME_FORMATTER =
      DateTimeFormatter.ofPattern("HH:mm:ss:SSS");

  public static void main(String[] args) {
    StreamSource<Map.Entry<String, byte[]>> source = KinesisSources.kinesis("Tweets")
     .withInitialShardIteratorRule(".*", "LATEST", null)
     .build();

    Pipeline p = Pipeline.create();
    p.readFrom(source)
     .withNativeTimestamps(3_000) //allow for some lateness in KDS timestamps
     .window(sliding(1_000, 500))
     .aggregate(counting())
     .writeTo(Sinks.logger(wr -> String.format(
         "At %s Kinesis got %,d tweets per second",
         TIME_FORMATTER.format(LocalDateTime.ofInstant(
             Instant.ofEpochMilli(wr.end()), ZoneId.systemDefault())),
         wr.result())));

    JobConfig cfg = new JobConfig().setName("kinesis-traffic-monitor");
    Jet.bootstrappedInstance().newJob(p, cfg);
  }
}
```

You may run this code from your IDE and it will work, but it will create
its own Jet instance. To run it on the Jet instance you already started,
use the command line like this:

<!--DOCUSAURUS_CODE_TABS-->

<!--Gradle-->

```bash
gradle build
<path_to_jet>/bin/jet submit -c org.example.JetJob build/libs/kinesis-tutorial-1.0-SNAPSHOT.jar
```

<!--Maven-->

```bash
mvn package
<path_to_jet>/bin/jet submit -c org.example.JetJob target/kinesis-tutorial-1.0-SNAPSHOT.jar
```

<!--END_DOCUSAURUS_CODE_TABS-->

Now go to the window where you started Jet. Its log output will contain
the output from the pipeline.

If `TweetPublisher` was running while you were following these steps,
you'll now get a report on the whole history and then a steady stream of
real-time updates. If you restart this program, you'll get all the
history again. That's how Hazelcast Jet behaves when working with a
replayable source.

Sample output:

```text
... At 16:11:27:500 Kinesis got 13 tweets per second
... At 16:11:28:000 Kinesis got 17 tweets per second
... At 16:11:28:500 Kinesis got 8 tweets per second
```

## 6. Clean-up

Once you're done, cancel the Jet jobs:

```bash
<path_to_jet>/bin/jet cancel tweet-publisher
<path_to_jet>/bin/jet cancel kinesis-traffic-monitor
```

Then we shut down our Jet member/cluster:

```bash
<path_to_jet>/bin/jet-stop
```

Also, clean up the "Tweets" stream from Kinesis (use the [AWS
 Console](https://console.aws.amazon.com/kinesis) or the
 [CLI](https://docs.aws.amazon.com/streams/latest/dev/fundamental-stream.html#clean-up)).
