---
title: Apply Windowed Aggregation
description: How to use windowed aggregation in Jet for monitoring most actively traded stocks on an exchange.
id: version-4.3.1-windowing
original_id: windowing
---

As we've seen in the guide for
[Stateful Transformations](../api/stateful-transforms.md#aggregate)
aggregation is the cornerstone of distributed stream processing. Most of
the useful things we can achieve with stream processing need one or the
other form of aggregation.

By definition aggregation takes a **finite** set of data and produces
a result from it. In order to make it work with infinite streams, we
need some way to break up the stream into finite chunks. This is what
**windowing** does.

Let's see how windowed aggregation can be used in Jet for monitoring a
financial exchange's most actively traded stocks.

## 1. Start Hazelcast Jet

1. Download Hazelcast Jet

```bash
wget https://github.com/hazelcast/hazelcast-jet/releases/download/v4.3.1/hazelcast-jet-4.3.1.tar.gz
tar zxvf hazelcast-jet-4.3.1.tar.gz && cd hazelcast-jet-4.3.1
```

If you already have Jet and you skipped the above steps, make sure to
follow from here on.

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

## 2. Create a New Java Project

We'll assume you're using an IDE. Create a blank Java project named
`trade-monitor` and copy the Gradle or Maven file
into it:

<!--DOCUSAURUS_CODE_TABS-->

<!--Gradle-->

```groovy
plugins {
    id 'com.github.johnrengelman.shadow' version '5.2.0'
    id 'java'
}

group 'org.example'
version '1.0-SNAPSHOT'

repositories.mavenCentral()

dependencies {
    compile 'com.hazelcast.jet:hazelcast-jet:4.3.1'
    compile 'com.hazelcast.jet.examples:hazelcast-jet-examples-trade-source:4.3.1'
}

jar {
    enabled = false
    dependsOn(shadowJar{archiveClassifier.set("")})
    manifest.attributes 'Main-Class': 'org.example.TradeMonitor'
}

shadowJar {
    dependencies {
        exclude(dependency('com.hazelcast.jet:hazelcast-jet:4.3.1'))
    }
}
```

<!--Maven-->

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>org.example</groupId>
    <artifactId>trade-monitor</artifactId>
    <version>1.0-SNAPSHOT</version>

    <properties>
        <maven.compiler.target>1.8</maven.compiler.target>
        <maven.compiler.source>1.8</maven.compiler.source>
    </properties>

    <dependencies>
        <dependency>
            <groupId>com.hazelcast.jet</groupId>
            <artifactId>hazelcast-jet</artifactId>
            <version>4.3.1</version>
        </dependency>
        <dependency>
            <groupId>com.hazelcast.jet.examples</groupId>
            <artifactId>hazelcast-jet-examples-trade-source</artifactId>
            <version>4.3.1</version>
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
                            <mainClass>org.example.TradeMonitor</mainClass>
                        </manifest>
                    </archive>
                </configuration>
            </plugin>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-shade-plugin</artifactId>
                <version>3.2.2</version>
                <executions>
                    <execution>
                        <phase>package</phase>
                        <goals>
                            <goal>shade</goal>
                        </goals>
                        <configuration>
                            <artifactSet>
                                <excludes>
                                    <exclude>com.hazelcast.jet:hazelcast-jet</exclude>
                                </excludes>
                            </artifactSet>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>
```

<!--END_DOCUSAURUS_CODE_TABS-->

## 3. Define Jet Job

The next thing we need to do is to write the Jet code which computes
the data we need.

We will define a pipeline doing the following:

* read an unbounded stream of trades
* compute the number of trades in the past minute, for each stock
  monitored (every 5 seconds)
* compute the top 10 stocks with most trades from the previous
  results (every 5 seconds)
* format and log the final results (every 5 seconds)

Add following class to your project.

```java
package org.example;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import com.hazelcast.jet.datamodel.WindowResult;
import com.hazelcast.jet.examples.tradesource.Trade;
import com.hazelcast.jet.examples.tradesource.TradeSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamStage;

import java.util.List;

import static com.hazelcast.function.ComparatorEx.comparing;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.aggregate.AggregateOperations.topN;
import static com.hazelcast.jet.pipeline.WindowDefinition.sliding;
import static com.hazelcast.jet.pipeline.WindowDefinition.tumbling;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TradeMonitor {

    private static final int TRADES_PER_SEC = 5000;
    private static final long MONITORING_INTERVAL = SECONDS.toMillis(60);
    private static final long REPORTING_INTERVAL = SECONDS.toMillis(5);

    public static void main(String[] args) {
        Pipeline pipeline = definePipeline();
        submitForExecution(pipeline);
    }

    private static Pipeline definePipeline() {
        Pipeline pipeline = Pipeline.create();

        StreamStage<Trade> source = pipeline.readFrom(TradeSource.tradeStream(TRADES_PER_SEC))
                .withNativeTimestamps(0);

        StreamStage<KeyedWindowResult<String, Long>> tradeCounts = source
                .groupingKey(Trade::getTicker)
                .window(sliding(MONITORING_INTERVAL, REPORTING_INTERVAL))
                .aggregate(counting());

        StreamStage<WindowResult<List<KeyedWindowResult<String, Long>>>> topN = tradeCounts
                .window(tumbling(REPORTING_INTERVAL))
                .aggregate(topN(10, comparing(KeyedWindowResult::result)));

        topN.map(wrList -> format(wrList.result()))
            .writeTo(Sinks.logger());

        return pipeline;
    }

    private static String format(List<KeyedWindowResult<String, Long>> results) {
        StringBuilder sb = new StringBuilder("Most active stocks in past minute:");
        for (int i = 0; i < results.size(); i++) {
            KeyedWindowResult<String, Long> result = results.get(i);
            sb.append(String.format("\n\t%2d. %5s - %d trades", i + 1, result.getKey(), result.getValue()));
        }
        return sb.toString();
    }

    private static void submitForExecution(Pipeline pipeline) {
        JetInstance instance = Jet.bootstrappedInstance();
        instance.newJob(pipeline, new JobConfig().setName("trade-monitor"));
    }

}
```

## 4. Package

Now we need to submit this code to Jet for execution. Since Jet runs on
our machine as a standalone cluster in a standalone process we need to
give it all the code that we have written.

For this reason we create a jar containing everything we need. All we
need to do is to run the build command:

<!--DOCUSAURUS_CODE_TABS-->

<!--Gradle-->

```bash
gradle build
```

This will produce a jar file called `trade-monitor-1.0-SNAPSHOT.jar`
in the `build/libs` folder of our project.

<!--Maven-->

```bash
mvn package
```

This will produce a jar file called `trade-monitor-1.0-SNAPSHOT.jar`
in the `target` folder or our project.

<!--END_DOCUSAURUS_CODE_TABS-->

## 6. Submit for Execution

Assuming our cluster is [still running](#1-start-hazelcast-jet) all we
need to issue is following command:

<!--DOCUSAURUS_CODE_TABS-->

<!--Gradle-->

```bash
<path_to_jet>/bin/jet submit build/libs/trade-monitor-1.0-SNAPSHOT.jar
```

<!--Maven-->

```bash
<path_to_jet>/bin/jet submit target/trade-monitor-1.0-SNAPSHOT.jar
```

<!--END_DOCUSAURUS_CODE_TABS-->

The output you should be seeing in the Jet member's log is one such
message every 5 seconds:

```text
... Most active stocks in past minute:
     1.  AXDX - 55 trades
     2.  MTBC - 53 trades
     3.  ARIS - 52 trades
     4.  ASUR - 51 trades
     5.  CSBR - 50 trades
     6.  ARII - 50 trades
     7.  FTXD - 50 trades
     8. MSDIW - 49 trades
     9.  SGEN - 49 trades
    10. LILAK - 49 trades
```

## 7. Clean up

Let's clean-up after ourselves. First we cancel our Jet Job:

```bash
<path_to_jet>/bin/jet cancel trade-monitor
```

Then we shut down our Jet member/cluster:

```bash
<path_to_jet>/bin/jet-stop
```
