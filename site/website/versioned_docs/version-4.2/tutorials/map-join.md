---
title: Enrich Your Stream
description: How to enrich your Jet stream with data from static sources.
id: version-4.2-map-join
original_id: map-join
---

Streams usually contain information changing with time, e.g. prices,
quantities, sales. Sometimes the stream needs to be enriched with
static, or infrequently changing data, such as labels, organizational
structure or some characteristics.

Hazelcast Jet allows you to enrich your stream directly from a
Hazelcast IMap or ReplicatedMap.
Since it must look up the data again for each item, performance is
lower than with a hash-join, but the data is kept fresh this way.
This matters especially for unbounded streaming jobs, where a
hash-join would use data frozen in time at the beginning of the job.

Let's build a pipeline that takes a stream of trades from a stock
exchange and enriches the stream with a full company name for
nicer presentation.
Each trade contains a stock symbol, also known as ticker, which
uniquely identifies the stock.
We will use the ticker to lookup the full company name in a replicated map.

## 1. Start Hazelcast Jet

1. Download Hazelcast Jet

```bash
wget https://github.com/hazelcast/hazelcast-jet/releases/download/v4.2/hazelcast-jet-4.2.tar.gz
tar zxvf hazelcast-jet-4.2.tar.gz && cd hazelcast-jet-4.2
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
`map-join-tutorial` and copy the Gradle or Maven file into it:

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
    compile 'com.hazelcast.jet:hazelcast-jet:4.2'
    compile 'com.hazelcast.jet.examples:hazelcast-jet-examples-trade-source:4.2'
}

jar {
    enabled = false
    dependsOn(shadowJar{archiveClassifier.set("")})
    manifest.attributes 'Main-Class': 'org.example.JoinUsingMapJob'
}

shadowJar {
    dependencies {
        exclude(dependency('com.hazelcast.jet:hazelcast-jet:4.2'))
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
    <artifactId>map-join-tutorial</artifactId>
    <version>1.0-SNAPSHOT</version>

    <properties>
        <maven.compiler.target>1.8</maven.compiler.target>
        <maven.compiler.source>1.8</maven.compiler.source>
    </properties>

    <dependencies>
        <dependency>
            <groupId>com.hazelcast.jet</groupId>
            <artifactId>hazelcast-jet</artifactId>
            <version>4.2</version>
        </dependency>
        <dependency>
            <groupId>com.hazelcast.jet.examples</groupId>
            <artifactId>hazelcast-jet-examples-trade-source</artifactId>
            <version>4.2</version>
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
                            <mainClass>org.example.JoinUsingMapJob</mainClass>
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

We have created a so-called fat jar.
If you want to know more about packaging Jet jobs please read the
[Submitting jobs](../get-started/submit-job.md) section.

## 3. Load Company Names into a Map

Copy the file [nasdaqlisted.txt](assets/nasdaqlisted.txt) containing a
list of company names to `src/main/resources`.

The following code creates a map containing company names from the list.
How we create the map is not important for this tutorial,
e.g. it could be loaded from a local file, S3 or be a result of another
job.

```java
package org.example;

import com.hazelcast.jet.*;

import java.io.*;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toMap;

public class LoadNames {

    public static void main(String[] args) {
        JetInstance instance = Jet.newJetClient();

        Map<String, String> namesMap = loadNames();
        instance.getMap("companyNames").putAll(namesMap);

        System.out.println(namesMap.size() + " names put to a map called 'companyNames'");

        instance.shutdown();
    }

    private static Map<String, String> loadNames() {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                LoadNames.class.getResourceAsStream("/nasdaqlisted.txt"), UTF_8))) {
            return reader.lines()
                    .skip(1)
                    .map(line -> line.split("\\|"))
                    .collect(toMap(parts -> parts[0], parts -> parts[1]));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

```

Finally, run it from your IDE. You should see this in the among other logs:

```text
3170 names put to a map called 'companyNames'
```

## 4. Use the Map to Enrich Jet Stream

This code takes a dummy source of trade data, enriches the trades with
the company name and finally writes to log.

```java
package org.example;

import com.hazelcast.jet.*;
import com.hazelcast.jet.config.*;
import com.hazelcast.jet.examples.tradesource.*;
import com.hazelcast.jet.pipeline.*;

import static com.hazelcast.jet.datamodel.Tuple4.tuple4;

public class JoinUsingMapJob {

    public static final int TRADES_PER_SEC = 1;

    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        pipeline.readFrom(TradeSource.tradeStream(TRADES_PER_SEC))
         .withoutTimestamps()
         .mapUsingIMap("companyNames", Trade::getTicker, (trade, name) ->
             tuple4(trade.getTicker(), trade.getQuantity(), trade.getPrice(), name))
         .writeTo(Sinks.logger(tuple -> String.format("%5s quantity=%4d, price=%d (%s)",
             tuple.f0(), tuple.f1(), tuple.f2(), tuple.f3()
         )));

        JetInstance instance = Jet.bootstrappedInstance();
        instance.newJob(pipeline, new JobConfig().setName("map-join-tutorial"));
        instance.shutdown();
    }

}
```

Submit the job to the Jet cluster

<!--DOCUSAURUS_CODE_TABS-->

<!--Gradle-->

```bash
gradle build
<path_to_jet>/bin/jet submit build/libs/map-join-tutorial-1.0-SNAPSHOT.jar
```

<!--Maven-->

```bash
mvn package
<path_to_jet>/bin/jet submit target/map-join-tutorial-1.0-SNAPSHOT.jar
```

<!--END_DOCUSAURUS_CODE_TABS-->

Now go to the window where you started Jet. Its log output will contain
the output from the pipeline.

If you submit the job before loading the company names you will see
null values.
Once you run the LoadNames class you will immediately see company
names.
This is how you can react to changing data.
You can restart the Jet instance to start with empty map to try this out.

## 5. Clean up

Let's clean-up after ourselves. First we cancel our Jet Job:

```bash
<path_to_jet>/bin/jet cancel map-join-tutorial
```

Then we shut down our Jet member/cluster:

```bash
<path_to_jet>/bin/jet-stop
```
