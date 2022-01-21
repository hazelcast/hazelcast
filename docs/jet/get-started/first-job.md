---
title: Your First Jet Program
description: How to write simple data processing code with Jet.
---

Let's write some data processing code and have Jet run it for us.

## Start a Java Project

By now you should have some version of Java (at least 8, recommended is
11 or later) installed. You can get it from the
[AdoptOpenJDK](https://adoptopenjdk.net/) website. Create a new project
targeting your build tool of preference, Maven or Gradle, and add the
Jet JAR to your build:

<!--DOCUSAURUS_CODE_TABS-->

<!--Gradle-->

```groovy
plugins {
    id 'application'
}
group 'org.example'
version '1.0-SNAPSHOT'

repositories.mavenCentral()

dependencies {
    compile 'com.hazelcast.jet:hazelcast-jet:{jet-version}'
}

application {
    mainClassName = 'org.example.JetJob'
}
```

<!--Maven-->

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>org.example</groupId>
    <artifactId>first-jet-program</artifactId>
    <version>1.0-SNAPSHOT</version>

    <name>first-jet-program</name>
    <properties>
      <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
      <maven.compiler.target>1.8</maven.compiler.target>
      <maven.compiler.source>1.8</maven.compiler.source>
    </properties>

    <dependencies>
      <dependency>
        <groupId>com.hazelcast.jet</groupId>
        <artifactId>hazelcast-jet</artifactId>
        <version>{jet-version}</version>
      </dependency>
    </dependencies>

    <build>
      <plugins>
        <plugin>
          <groupId>org.apache.maven.plugins</groupId>
          <artifactId>maven-jar-plugin</artifactId>
          <version>3.2.0</version>
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

## Write Your Data Pipeline

Unlike some other paradigms you may have met, in the world of
distributed stream processing we specify not just the processing steps
but also where to pull the data from and where to deliver the results.
This means that as soon as you deploy your code, it takes action and
starts moving the data through the pipeline.

With this in mind let's start writing code. Instead of connecting to
actual systems we'll start simple, using generated data as the source
and your screen as the sink:

```java
public static void main(String[] args) {
  Pipeline p = Pipeline.create();
  p.readFrom(TestSources.itemStream(10))
   .withoutTimestamps()
   .filter(event -> event.sequence() % 2 == 0)
   .setName("filter out odd numbers")
   .writeTo(Sinks.logger());
}
```

`itemStream()` emits `SimpleEvent`s that have an increasing *sequence*
number. The pipeline we wrote will discard every other event and keep
those with an even sequence number.

## Start Embedded Jet and Run the Pipeline

To create a single Jet node and submit the job to it, add this code to
**the bottom** of the `main` method:

```java
JetInstance jet = Jet.newJetInstance();
jet.newJob(p).join();
```

And run it either from your IDE or from command line:

<!--DOCUSAURUS_CODE_TABS-->

<!--Gradle-->

```bash
gradle run
```

<!--Maven-->

```bash
mvn -Dexec.mainClass=org.example.JetJob exec:java
```

<!--END_DOCUSAURUS_CODE_TABS-->

It will start a full-featured Jet node right there in the JVM where you
call it and submit your pipeline to it. If you were submitting the code
to an external Jet cluster, the syntax would be the same because
`JetInstance` can represent both an embedded instance or a remote one
via a local proxy object. You'd just call a different method to create
the client instance.

Once you submit a job, it has a life of its own. It is not coupled to
the client that submitted it, so the client can disconnect without
affecting the job. In our simple code we call `job.join()` so we keep
the JVM alive while the job lasts.

The output should look like this:

```log
11:28:24.039 [INFO] [loggerSink#0] (timestamp=11:28:24.000, sequence=0)
11:28:24.246 [INFO] [loggerSink#0] (timestamp=11:28:24.200, sequence=2)
11:28:24.443 [INFO] [loggerSink#0] (timestamp=11:28:24.400, sequence=4)
11:28:24.647 [INFO] [loggerSink#0] (timestamp=11:28:24.600, sequence=6)
11:28:24.846 [INFO] [loggerSink#0] (timestamp=11:28:24.800, sequence=8)
11:28:25.038 [INFO] [loggerSink#0] (timestamp=11:28:25.000, sequence=10)
11:28:25.241 [INFO] [loggerSink#0] (timestamp=11:28:25.200, sequence=12)
11:28:25.443 [INFO] [loggerSink#0] (timestamp=11:28:25.400, sequence=14)
11:28:25.643 [INFO] [loggerSink#0] (timestamp=11:28:25.600, sequence=16)
```

Continue to the next step to submit this job to a running cluster instead
of running embedded instance.
