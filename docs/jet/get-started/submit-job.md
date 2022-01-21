---
title: Submit a Job to the Cluster
description: How to submit a Jet job to a standalone cluster.
---

At this point you saw how to create a simple Jet program (a *job*) that
starts its own Jet instance to run on, and how to create a standalone
Jet cluster. Now it's time to combine the two and run the program on the
cluster.

## Use the Bootstrapped Instance

Originally we used `Jet.newJetInstance()` to create an embedded Jet node
and noted that this line will change once you have an outside cluster.
Jet uses the concept of a *bootstrapped instance* which acts differently
depending on context. Change the line

```java
JetInstance jet = Jet.newJetInstance();
```

to

```java
JetInstance jet = Jet.bootstrappedInstance();
```

If you run the application again, it will have the same behavior as
before and create an embedded Jet instance. However, if you package your
code in a JAR and pass it to `jet submit`, it will instead return a
client proxy that talks to the cluster.

## Package the Job as a JAR

If you're using Maven, `mvn package` will generate a JAR file ready to
be submitted to the cluster. `gradle build` does the same. You should
also set the `Main-Class` attribute in `MANIFEST.MF` to avoid the need
to specify the main class in `jet submit`. Both Maven and Gradle can be
configured to do this, refer to their docs.

<!--DOCUSAURUS_CODE_TABS-->

<!--Gradle-->

```groovy
application {
    mainClassName = 'org.example.JetJob'
}
```

<!--Maven-->

```xml
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
```

<!--END_DOCUSAURUS_CODE_TABS-->

## Submit to the Cluster

From the Jet home folder execute the command:

<!--DOCUSAURUS_CODE_TABS-->
<!--Standalone-->

```bash
bin/jet submit <path_to_JAR_file>
```

<!--Docker-->

```bash
docker run -it -v <path_to_JAR_file>:/jars hazelcast/hazelcast-jet jet -t 172.17.0.2 submit /jars/<name_of_the_JAR_file>
```

<!--END_DOCUSAURUS_CODE_TABS-->

If you didn't specify the main class in the JAR, you must use `-c`:

<!--DOCUSAURUS_CODE_TABS-->
<!--Standalone-->

```bash
bin/jet submit -c <main_class_name> <path_to_JAR_file>
```

<!--Docker-->

```bash
docker run -it -v <path_to_JAR_file>:/jars hazelcast/hazelcast-jet jet -t 172.17.0.2 submit -c <main_class_name> /jars/<name_of_the_JAR_file>
```

<!--END_DOCUSAURUS_CODE_TABS-->

You can notice in the server logs that a new job has been submitted and
it's running on the cluster. Since we're using the logger as the sink
(`Sinks.logger()`), the output of the job appears in the server logs.

You can also see a list of running jobs as follows:

<!--DOCUSAURUS_CODE_TABS-->
<!--Standalone-->

```bash
$ bin/jet list-jobs
ID                  STATUS             SUBMISSION TIME         NAME
03de-e38d-3480-0001 RUNNING            2020-02-09T16:30:26.843 N/A
```

<!--Docker-->

```bash
$ docker run -it hazelcast/hazelcast-jet jet -t 172.17.0.2 list-jobs
ID                  STATUS             SUBMISSION TIME         NAME
03e3-b8f6-5340-0001 RUNNING            2020-02-13T09:36:46.898 N/A
```

<!--END_DOCUSAURUS_CODE_TABS-->

As we noted earlier, whether or not you kill the client application, the
job keeps running on the server. A job with a streaming source will run
indefinitely until explicitly cancelled (`jet cancel <job-id>`) or the
cluster is shut down. Keep the job running for now, in the next steps
we'll be scaling it up and down.
