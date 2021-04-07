---
title: Group Messages from Apache Pulsar
description: How to use a Pulsar topic as a data source in Jet pipelines.
id: version-4.2-pulsar
original_id: pulsar
---

Apache Pulsar is an open-source distributed pub-sub messaging system. By
using Pulsar connector module, Hazelcast Jet can consume messages from
Pulsar topics.

Hazelcast Jet allows us to compute different aggregation functions(sum,
avg etc.) on specified window of stream items. It also provide a
function to group them by some key and then aggregate over each group
separately.

Below, we will create a Jet job that receives messages from Pulsar and
then counts messages by grouping them by their `user` keys. We will use
the concept of sliding window to process these continuous stream.

## 1. Start Apache Pulsar

If you already have Pulsar and you skip below steps:

If you don't have it already, you can download, and run Pulsar in
standalone mode on your machine for testing purposes.

1. [Download](https://pulsar.apache.org/download/) Apache Pulsar
2. Extract it into folder and go into `./bin` folder.
3. Run the local Pulsar broker by calling`./pulsar standalone`. It will
   start on localhost:6650 by default. For more details about standalone
   mode, [view](https://pulsar.apache.org/docs/en/standalone/)

For the code sample, we will assume that the Pulsar is running on
localhost:6650.

## 2. Start Hazelcast Jet

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

## 3. Create a New Java Project

Create a blank Java project named
`pulsar-example` and copy the Gradle or Maven file into it:

<!--DOCUSAURUS_CODE_TABS-->

<!--Gradle-->

```groovy
plugins {
    id 'java'
    id 'com.github.johnrengelman.shadow' version '5.2.0'
}

group 'org.example'
version '1.0-SNAPSHOT'

sourceCompatibility = 1.8

repositories {
    mavenCentral()
}

dependencies {
    compile 'com.hazelcast.jet:hazelcast-jet:4.2'
    compile 'com.hazelcast.jet-contrib:pulsar:0.1'
    compile 'org.apache.pulsar:pulsar-client:2.5.0'
}

jar {
    enabled = false
    dependsOn(shadowJar{archiveClassifier.set("")})
    manifest.attributes 'Main-Class': 'org.example.JetJob'
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
    <artifactId>pulsar-example</artifactId>
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
            <groupId>com.hazelcast.jet.contrib</groupId>
            <artifactId>pulsar</artifactId>
            <version>0.1</version>
        </dependency>
        <dependency>
            <groupId>org.apache.pulsar</groupId>
            <artifactId>pulsar-client</artifactId>
            <version>2.5.0</version>
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
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-shade-plugin</artifactId>
                <version>3.2.2</version>
                <configuration>
                    <filters>
                        <filter>
                            <artifact>*:*</artifact>
                            <excludes>
                                <exclude>META-INF/*.SF</exclude>
                                <exclude>META-INF/*.DSA</exclude>
                                <exclude>META-INF/*.RSA</exclude>
                            </excludes>
                        </filter>
                    </filters>
                </configuration>
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

## 4. Publish messages to a Pulsar topic

The program below connects the previously started pulsar cluster located
at `pulsar://localhost:6650`. And then, it iteratively picks a user
uniformly at random, then creates event on behalf of this user, and
sends this event as a message to the Pulsar topic named `hz-jet-topic`.

```java
package org.example;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class MessagePublisher {

    public static class Event {
        public String user;
        public Long eventCount;
        public String message;

        public Event() {
        }

        public Event(String user, Long eventCount, String message) {
            this.user = user;
            this.eventCount = eventCount;
            this.message = message;
        }
        public String getUser() {
            return user;
        }

        public Long getEventCount() {
            return eventCount;
        }

        public String getMessage() {
            return message;
        }

    }

    public static void main(String[] args) throws Exception {
        String topicName = "hz-jet-topic";
        PulsarClient client = PulsarClient.builder()
                                          .serviceUrl("pulsar://localhost:6650")
                                          .build();
        String[] userArray = {"user1", "user2", "user3", "user4", "user5"};
        Producer<Event> producer = client
                .newProducer(Schema.JSON(Event.class))
                .topic(topicName)
                .batchingMaxPublishDelay(10, TimeUnit.MILLISECONDS)
                .sendTimeout(10, TimeUnit.SECONDS)
                .blockIfQueueFull(true)
                .create();

        for (long eventCount = 0; ; eventCount++) {
            String message = String.format("message-%0,4d", eventCount);
            String user = getRandomUser(userArray);
            producer.send(new Event(user, eventCount, message));
            System.out.format("Published '%s' from '%s' to Pulsar topic '%s'%n", message, user, topicName);
            Thread.sleep(20);
        }
    }

    private static String getRandomUser(String[] userArray) {
        Random r = new Random();
        return userArray[r.nextInt(userArray.length)];
    }
}

```

Run it from your IDE. You should see this in the output:

```text
Published 'message-0000' from 'user1' to Pulsar topic 'hz-jet-topic'
Published 'message-0001' from 'user3' to Pulsar topic 'hz-jet-topic'
Published 'message-0002' from 'user1' to Pulsar topic 'hz-jet-topic'
Published 'message-0003' from 'user2' to Pulsar topic 'hz-jet-topic'
Published 'message-0004' from 'user4' to Pulsar topic 'hz-jet-topic'
...
```

## 5. Use Hazelcast Jet to Count the Messages of Users

The code below is used to connect to the Pulsar topic and gets messages
from it and then logs the count of messages by grouping them with their
users.

```java
package org.example;

import com.hazelcast.jet.*;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.contrib.pulsar.PulsarSources;
import com.hazelcast.jet.pipeline.*;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.example.MessagePublisher.Event;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.pipeline.WindowDefinition.sliding;

public class JetJob {
    static final DateTimeFormatter TIME_FORMATTER =
            DateTimeFormatter.ofPattern("HH:mm:ss:SSS");

    public static void main(String[] args) {
        String topicName = "hz-jet-topic";

        StreamSource<Event> source = PulsarSources.pulsarReaderBuilder(
                topicName,
                () -> PulsarClient.builder().serviceUrl("pulsar://localhost:6650").build(),
                () -> Schema.JSON(Event.class),
                Message::getValue).build();

        Pipeline p = Pipeline.create();
        p.readFrom(source)
         .withNativeTimestamps(0)
         .groupingKey(Event::getUser)
         .window(sliding(TimeUnit.SECONDS.toMillis(60), TimeUnit.SECONDS.toMillis(30)))
         .aggregate(counting())
         .writeTo(Sinks.logger(wr -> String.format(
                 "At %s Pulsar got %,d messages in the previous minute from %s.",
                 TIME_FORMATTER.format(LocalDateTime.ofInstant(
                         Instant.ofEpochMilli(wr.end()), ZoneId.systemDefault())),
                 wr.result(), wr.key())));

        JobConfig cfg = new JobConfig()
                .setName("pulsar-message-counter");
        Jet.bootstrappedInstance().newJob(p, cfg);
    }
}
```

If you run this code from your IDE, it will create its own Jet instance
and run the job on it. To run this on the previously started Jet member,
you need to create a runnable JAR including all dependencies required to
run it. Then, submit it to the Jet cluster. Since build.gradle/pom.xml
files are configured to create a fat jar, we can do these steps easily
as shown as below:
<!--DOCUSAURUS_CODE_TABS-->

<!--Gradle-->

```bash
gradle build
<path_to_jet>/bin/jet submit build/libs/pulsar-example-1.0-SNAPSHOT.jar
```

<!--Maven-->

```bash
mvn package
<path_to_jet>/bin/jet submit target/pulsar-example-1.0-SNAPSHOT.jar
```

<!--END_DOCUSAURUS_CODE_TABS-->
For more information on submitting jobs, you can look
[this](../get-started/submit-job.md) section.

Now go to the window where you started Jet. Its log output will contain
the output from the pipeline.

If `MessagePublisher` was running while you were following these steps,
you'll now get a report on the whole history of the events and then a
steady stream of real-time updates.

Sample output: 10:38:44.504 Between 10:32:00:00-10:32:30:000 Pulsar got
508 messages from user4.

```text
10:38:44.504  Between 10:32:00:00-10:32:30:000 Pulsar got 538 messages from user1.
10:38:44.504  Between 10:32:00:00-10:32:30:000 Pulsar got 508 messages from user4.
10:38:44.597  Between 10:32:30:00-10:33:00:000 Pulsar got 584 messages from user2.
10:38:44.597  Between 10:32:30:00-10:33:00:000 Pulsar got 551 messages from user5.
10:38:44.597  Between 10:32:30:00-10:33:00:000 Pulsar got 540 messages from user3.
```

It is possible to restart a job without suspending and resuming in one
atomic action. You can restart the job by calling:

```bash
<path_to_jet>/bin/jet restart pulsar-message-counter
```

After that, you'll get all the history again. If you want to change this
behaviour to continue where it left off, you can change the fault
tolerance behaviour of the job to exactly-once by replacing the
JobConfig section of the program with the following.

```java
JobConfig cfg = new JobConfig()
                .setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE)
                .setSnapshotIntervalMillis(SECONDS.toMillis(1))
                .setName("pulsar-message-counter");
```

When we are done with this tutorial. To clean up things, cancel the job:

```bash
<path_to_jet>/bin/jet cancel pulsar-message-counter
```

Then, shut down the Jet member:

```bash
<path_to_jet>/bin/jet-stop
```
