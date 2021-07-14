---
title: Running With Docker
description: How to use Jet in a Docker environment
id: version-4.4-docker
original_id: docker
---

This section shows you how to use Hazelcast Jet with Docker. We explain
some options and parameters used in the examples, but otherwise assume
you are familiar with Docker in general. Visit the [official
documentation](https://docs.docker.com/get-started/) for a basic
introduction.

## Start a Hazelcast Jet Member

Paste the following into your command line:

```bash
docker pull hazelcast/hazelcast-jet
docker run -p 5701:5701 hazelcast/hazelcast-jet
```

On Linux you can omit the `-p 5701:5701` part, but on other platforms
you may need it if the container's network isn't directly exposed to the
host machine.

Wait for a line like this in the log:

```text
2020-05-18 19:34:43,872 [ INFO] [main] [c.h.system]:
    Hazelcast Jet <version> (20200429 - e6c60a1) starting at [172.17.0.2]:5701
```

The prompt doesn't return, which allows you to easily stop or restart
Jet. Note Jet's IP address, you'll need it later.

You can also see Jet's version and build info without starting it:

```text
$ docker run --rm hazelcast/hazelcast-jet jet --version
Hazelcast Jet <version>
Revision e6c60a1
Build 20200429
```

## Start the Management Center

The Management Center provides an easy way to monitor Hazelcast Jet
cluster and running jobs. Start it like this:

```bash
docker run -p 8081:8081 -e JET_MEMBER_ADDRESS=172.17.0.2 hazelcast/hazelcast-jet-management-center
```

After a few seconds you can access the management center on
[http://localhost:8081](http://localhost:8081), the default
username/password are admin/admin.

## Submit a Job

Let's submit a Hello World job to the Jet server you started. It is
packaged with the Hazelcast Jet distribution, so start by downloading
it. Paste this into your command line:

```text
wget https://github.com/hazelcast/hazelcast-jet/releases/download/v4.4/hazelcast-jet-4.4.tar.gz
tar xvf hazelcast-jet-4.4.tar.gz
cd hazelcast-jet-4.4
```

Now submit the Hello World job:

```bash
docker run -it -v "$(pwd)"/examples:/examples --rm hazelcast/hazelcast-jet jet -t 172.17.0.2 submit /examples/hello-world.jar
```

The Docker parameters are as follows:

* `-it` tells Docker to start an interactive session, allowing you to
  cancel the `submit` command with `Ctrl+C`
* `-v "$(pwd)"/examples:/examples` mounts the folder `examples` from
  your current directory as `/examples` inside the container
* `--rm` tells Docker to remove the container from its local cache once
  it exits

These are the parameters for `jet submit`:

* `-t 172.17.0.2`, short for `--targets`, is the address of the Jet
  server to connect to
* `/examples/hello-world.jar` is the JAR containing the code which will
  create a Jet pipeline and submit it

List the jobs running inside the cluster:

```bash
$ docker run --rm hazelcast/hazelcast-jet jet -t 172.17.0.2 list-jobs
ID                  STATUS             SUBMISSION TIME         NAME
045e-987c-1940-0001 RUNNING            2020-05-18T20:08:03.020 hello-world
```

To cancel the running job:

```bash
docker run --rm hazelcast/hazelcast-jet jet -t 172.17.0.2 cancel hello-world
```

## Submit the Job as a Docker Image

Let's create a Docker image with the code of the Hello World job and
submit it to Jet.

### Create a Dockerfile

```Dockerfile
FROM hazelcast/hazelcast-jet
ADD examples/hello-world.jar /examples/
ENV JET_ADDRESS 172.17.0.2
CMD ["sh", "-c", "jet -t $JET_ADDRESS submit /examples/hello-world.jar"]
```

We exposed the Jet address through the `JTE_ADDRESS` environment
variable, with the default value of `172.17.0.2`. This makes it easy to
pass a different address with `docker run -e JET_ADDRESS=<another.one>`.

### Create the Image

```bash
$ docker build . -t hazelcast-jet-hello-world
Sending build context to Docker daemon  77.35MB
...
Successfully built 6bc0f527b69c
Successfully tagged hazelcast-jet-hello-world:latest
```

### Submit the Job

```bash
docker run -it hazelcast-jet-hello-world
```

## Configure Hazelcast Jet

### Memory configuration

By default, Jet configures the JVM with `-XX:MaxRAMPercentage=80.0`.
This limits the JVM heap to 80% of the RAM available to the container.
We recommend you leave this as-is and control Jet's memory size with the
Docker parameter `--memory`. For example, this will start Jet with 1.6
GB assigned to the JVM:

```bash
docker run --memory 2g --rm hazelcast/hazelcast-jet
```

### JAVA_OPTS

To change the JVM parameters directly, use the `JAVA_OPTS` environment
variable. Jet passes it to the JVM when starting. For example:

```bash
docker run --memory 2g -e JAVA_OPTS="-XX:MaxRAMPercentage=85.0" --rm hazelcast/hazelcast-jet
```

Make sure to leave enough free RAM for Metaspace and other overheads.

### Custom Hazelcast Jet Configuration File

You can configure Hazelcast Jet with your own `hazelcast-jet.yaml` or
`hazelcast.yaml` by replacing the default ones in the container at
`/opt/hazelcast-jet/config`. We recommend that you use the default
configuration file as a starting point:

```bash
docker run --rm hazelcast/hazelcast-jet cat /opt/hazelcast-jet/config/hazelcast.yaml > hazelcast.yaml
```

Now edit the file and apply it when starting Jet:

```bash
docker run -v "$(pwd)"/hazelcast.yaml:/opt/hazelcast-jet/config/hazelcast.yaml hazelcast/hazelcast-jet
```

### Extend Jet's CLASSPATH with Custom Jars and Files

If you have to add more classes or files to Jet's classpath, one way to
do it is to put them in a folder, e.g., `ext`, mount it to the
container, and set the `CLASSPATH` environment variable:

```bash
docker run -e CLASSPATH="/opt/hazelcast-jet/ext/" -v /path/to/ext:/opt/hazelcast-jet/ext hazelcast/hazelcast-jet
```

If you have just one file to add, it's simpler to mount it directly into
Jet's `lib` folder:

```bash
docker run -v /path/to/my.jar:/opt/hazelcast-jet/lib/my.jar hazelcast/hazelcast-jet
```

### Change Logging Level

You can set the logging level using the `LOGGING_LEVEL` environment
variable:

```bash
docker run -e LOGGING_LEVEL=DEBUG hazelcast/hazelcast-jet
```

Available logging levels are (from highest to lowest): `FATAL`, `ERROR`,
`WARN`, `INFO`, `DEBUG`, `TRACE`. The default logging level is `INFO`.

If you need more control over logging, you can supply your own
`log4j2.properties` file. Use the default one as the starting point:

```bash
docker run --rm hazelcast/hazelcast-jet cat /opt/hazelcast-jet/config/log4j2.properties > log4j2.properties
```

Edit the file and mount it when starting Jet:

```bash
docker run -v /path/to/log4j2.properties:/opt/hazelcast-jet/config/log4j2.properties hazelcast/hazelcast-jet
```

## Create a Jet Cluster

To ensure your pipeline code will work in the cluster, you should test
it locally in a cluster as well. Testing on a single node hides some
common serialization issues your code may have.

You can form a Jet cluster on your local machine by starting multiple
Jet containers. Run `docker run` from multiple terminal windows, using a
distinct port mapping for each:

```bash
docker run -p 5701:5701 hazelcast/hazelcast-jet
```

```bash
docker run -p 5702:5701 hazelcast/hazelcast-jet
```

The two Jet instances will discover each other automatically. Wait for
output like the following in the logs:

```text
Members {size:2, ver:2} [
    Member [172.17.0.2]:5701 - df428435-8cbd-464d-8671-afe396f5eef6
    Member [172.17.0.3]:5701 - 011a9dd4-936f-4170-a7c6-622b7430b789 this
]
```

You can use the IP of any Jet node to submit a job. Jet takes care of
distributing it across the whole cluster.

## Docker Compose

You can start a Hazelcast Jet cluster managed by Docker Compose. This
also makes it easier to customize Jet with configuration files, mounted
directories etc.

Here's a simple `docker-compose.yml`:

```yaml
version: '3'

services:
  jet:
    image: hazelcast/hazelcast-jet
    ports:
      - "5701-5703:5701"
```

Now you can start a 3-node Jet cluster:

```bash
docker-compose up --scale jet=3
```

You should eventually see a 3-node cluster has formed:

```text
jet_3  | Members {size:3, ver:3} [
jet_3  |    Member [172.21.0.3]:5701 - 99d3de67-8c5d-452b-8165 -085a4cd1fcda
jet_3  |    Member [172.21.0.2]:5701 - 64f5b01b-847e-49e0-87f2 -db2a6f7750b7
jet_3  |    Member [172.21.0.4]:5701 - ffa362f9-617d-42bc-a74c -05ce857e8e48 this
jet_3  | ]
```

The `ports` section says that port 5701 from each container should be
mapped to a port from the range 5701-5703. Increase the range if you
want to start more than three instances.

### Configuration

You can provide custom `hazelcast.yaml` or `hazelcast-jet.yaml`
configuration files by using a volume:

```yaml
version: '3'

services:

  jet:
    image: hazelcast/hazelcast-jet
    ports:
      - "5701-5703:5701"
    volumes:
      - ./hazelcast.yaml:/opt/hazelcast-jet/config/hazelcast.yaml
```

## Build a Custom Image from the Slim Image

Since version 4.4, Hazelcast Jet offers a slim Docker image that
contains just the core Jet engine. When image size is a concern, use it
as the starting point to build your custom image with just the
extensions you need.

Here's a quick example. Write a Dockerfile in an empty directory:

```Dockerfile
FROM hazelcast-jet:<jet-version>-slim
ARG JET_HOME=/opt/hazelcast-jet
ARG REPO_URL=https://repo1.maven.org/maven2/com/hazelcast/jet
ADD $REPO_URL/hazelcast-jet-kafka/4.3/hazelcast-jet-kafka-4.3-jar-with-dependencies.jar $JET_HOME/lib/
# ... more ADD statements ...
```

To find the available extensions and their URLs, open the [Maven
URL](https://repo1.maven.org/maven2/com/hazelcast/jet) in your browser.

Build the image from the `Dockerfile`:

```bash
docker build . -t jet-with-kafka
```

Start the image:

```bash
docker run -p 5701:5701 jet-with-kafka
```

For more information on Dockerfile, go to its [reference
manual](https://docs.docker.com/engine/reference/builder/).
