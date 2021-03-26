---
title: Connect to Apache Kafka
description: How to use Jet for processing data streaming from Apache Kafka.
id: version-4.4-kafka
original_id: kafka
---

Apache Kafka is a distributed, replayable messaging system. It is a
great fit for building a fault-tolerant data pipeline with Jet.

Let's build a Jet data pipeline that receives an event stream from
Kafka and computes its traffic intensity (events per second).

## 1. Start Apache Kafka

If you don't have it already, install and run Kafka. You can use [these
instructions](https://kafka.apache.org/quickstart) (you need just steps
1 and 2).

From now on we assume Kafka is running on your machine.

## 2. Start Hazelcast Jet

1. Download Hazelcast Jet

```bash
wget https://github.com/hazelcast/hazelcast-jet/releases/download/v4.4/hazelcast-jet-4.4.tar.gz
tar zxvf hazelcast-jet-4.4.tar.gz && cd hazelcast-jet-4.4
```

If you already have Jet, and you skipped the above steps, make sure to
follow from here on (just check that `hazelcast-jet-kafka-4.4.jar` is
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
`kafka-tutorial` and copy the Gradle or Maven file into it:

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
    implementation 'com.hazelcast.jet:hazelcast-jet-kafka:4.4'
}

jar.manifest.attributes 'Main-Class': 'org.example.JetJob'
```

<!--Maven-->

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>org.example</groupId>
    <artifactId>kafka-tutorial</artifactId>
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
            <artifactId>hazelcast-jet-kafka</artifactId>
            <version>4.4</version>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-jar-plugin</artifactId>
                <configuration>
                    <archive>
                        <manifest>
                            <mainClass>org.example.JetJob</mainClass>
                        </manifest>
                    </archive>
                </configuration>
            </plugin>
        </plugins>
    </build>
</project>
```

<!--END_DOCUSAURUS_CODE_TABS-->

## 4. Publish an Event Stream to Kafka

This code publishes "tweets" (just some simple strings) to a Kafka topic
`tweets`, with varying intensity:

```java
package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;

import java.util.Properties;

public class TweetPublisher {
    public static void main(String[] args) throws Exception {
        String topicName = "tweets";
        try (KafkaProducer<Long, String> producer = new KafkaProducer<>(kafkaProps())) {
            for (long eventCount = 0; ; eventCount++) {
                String tweet = String.format("tweet-%0,4d", eventCount);
                producer.send(new ProducerRecord<>(topicName, eventCount, tweet));
                System.out.format("Published '%s' to Kafka topic '%s'%n", tweet, topicName);
                Thread.sleep(20 * (eventCount % 20));
            }
        }
    }

    private static Properties kafkaProps() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.serializer", LongSerializer.class.getCanonicalName());
        props.setProperty("value.serializer", StringSerializer.class.getCanonicalName());
        return props;
    }
}
```

Run it from your IDE. You should see this in the output:

```text
Published 'tweet-0001' to Kafka topic 'tweets'
Published 'tweet-0002' to Kafka topic 'tweets'
Published 'tweet-0003' to Kafka topic 'tweets'
...
```

Let it run in the background while we go on to creating the next class.

## 5. Use Hazelcast Jet to Analyze the Event Stream

This code lets Jet connect to Kafka and show how many events per second
were published to the Kafka topic at a given time:

```java
package org.example;

import com.hazelcast.jet.*;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.kafka.KafkaSources;
import com.hazelcast.jet.pipeline.*;
import org.apache.kafka.common.serialization.*;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.pipeline.WindowDefinition.sliding;

public class JetJob {
    static final DateTimeFormatter TIME_FORMATTER =
            DateTimeFormatter.ofPattern("HH:mm:ss:SSS");

    public static void main(String[] args) {
        Pipeline p = Pipeline.create();
        p.readFrom(KafkaSources.kafka(kafkaProps(), "tweets"))
         .withNativeTimestamps(0)
         .window(sliding(1_000, 500))
         .aggregate(counting())
         .writeTo(Sinks.logger(wr -> String.format(
                 "At %s Kafka got %,d tweets per second",
                 TIME_FORMATTER.format(LocalDateTime.ofInstant(
                         Instant.ofEpochMilli(wr.end()), ZoneId.systemDefault())),
                 wr.result())));

        JobConfig cfg = new JobConfig().setName("kafka-traffic-monitor");
        Jet.bootstrappedInstance().newJob(p, cfg);
    }

    private static Properties kafkaProps() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.deserializer", LongDeserializer.class.getCanonicalName());
        props.setProperty("value.deserializer", StringDeserializer.class.getCanonicalName());
        props.setProperty("auto.offset.reset", "earliest");
        return props;
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
<path_to_jet>/bin/jet submit build/libs/kafka-tutorial-1.0-SNAPSHOT.jar
```

<!--Maven-->

```bash
mvn package
<path_to_jet>/bin/jet submit target/kafka-tutorial-1.0-SNAPSHOT.jar
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
16:11:35.033 ... At 16:11:27:500 Kafka got 3 tweets per second
16:11:35.034 ... At 16:11:28:000 Kafka got 2 tweets per second
16:11:35.034 ... At 16:11:28:500 Kafka got 8 tweets per second
```

Once you're done with it, cancel the job:

```bash
<path_to_jet>/bin/jet cancel kafka-traffic-monitor
```
