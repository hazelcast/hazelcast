---
title: Running With Docker
description: How to use Jet in a Docker environment
---

This section provides comprehensive coverage of usage of Hazelcast Jet
inside a Docker environment. While it gives an explanation of all
options and parameters used in the examples, it is recommended to be
familiar with Docker; you can visit the [official
documentation](https://docs.docker.com/get-started/) for a beginner
focused tutorial.

## Start Hazelcast Jet Member

Let's begin with starting an instance of Hazelcast Jet using the
official Docker image. To start the instance simply execute the
following command in your terminal:

```bash
docker run -p 5701:5701 hazelcast/hazelcast-jet
```

If you previously haven't run Hazelcast Jet Docker image, it will
download the latest version of the image from [Docker
hub](https://hub.docker.com/r/hazelcast/hazelcast-jet/). You can check
the version of Jet in the logs, look for a line similar to this one:

```text
2020-05-18 19:34:43,872 [ INFO] [main] [c.h.system]: Hazelcast Jet {jet-version} (20200429 - e6c60a1) starting at [172.17.0.2]:5701
```

You can also verify the version of Hazelcast Jet by running the
following command without starting a full instance:

```bash
docker run hazelcast/hazelcast-jet jet --version
```

You should get an output similar to the following:

```text
$ docker run hazelcast/hazelcast-jet jet --version
Hazelcast Jet {jet-version}
Revision e6c60a1
Build 20200429
```

If your `hazelcast/hazelcast-jet` latest image points to an older
version you can update it by running the following command:

```bash
docker pull hazelcast/hazelcast-jet
```

You can also specify a particular version of the image to run when
starting the container, e.g. when you need to run an older version of
Jet:

```bash
docker run hazelcast/hazelcast-jet:4.0
```

## Submit Job to Hazelcast Jet

We will submit a Hello World job, which is available in the Hazelcast
Jet distribution. Download the Jet distribution from
[here](https://github.com/hazelcast/hazelcast-jet/releases/download/v{jet-version}/hazelcast-jet-{jet-version}.tar.gz)
. From now on we will refer to the extracted location as `<jet home>`.

Also, note the address of your Hazelcast Jet instance from the log:

```text
Members {size:1, ver:1} [
    Member [172.17.0.2]:5701 - c54413e4-2cd2-4c07-91ec-d5992b18f9b0 this
]
```

Run the following command to submit the Hello World job to your Hazelcast
Jet instance:

```bash
docker run -it -v "$(pwd)"/examples:/examples hazelcast/hazelcast-jet jet -a 172.17.0.2 submit /examples/hello-world.jar
```

The parameters are as follows:

* `-it` allocates interactive terminal, allowing you to cancel the
  submit command by pressing Ctrl+C
* `-v "$(pwd)"/examples:/examples` mounts folder `examples` from your
  current directory to `/examples` directory inside the container
* `-a 172.17.0.2` - shortcut for `--addresses`, specifies an address of
  running Hazelcast Jet instance, needs to be specified because the
  instance is not running on `localhost:5701`
* `submit` command to submit a job
* `/examples/hello-world.jar` path to a jar with a job inside the
  container

You can list the running jobs inside the cluster by running the
following command:

```bash
docker run hazelcast/hazelcast-jet jet -a 172.17.0.2 list-jobs
```

which gives an output similar to:

```text
$ docker run hazelcast/hazelcast-jet jet -a 172.17.0.2 list-jobs
ID                  STATUS             SUBMISSION TIME         NAME
045e-987c-1940-0001 RUNNING            2020-05-18T20:08:03.020 hello-world
```

You can cancel a running job using the following command:

```bash
docker run hazelcast/hazelcast-jet jet -a 172.17.0.2 cancel hello-world
```

### Using Local Distribution

Alternatively you can use the downloaded distribution to submit a job to
a cluster directly. While in `<jet home>`, run the following command:

```bash
bin/jet submit examples/hello-world.jar
```

In some environments, e.g. on Windows or macOS, the container is not
directly accessible via network from your host machine. When starting
the container we have used the `-p 5701:5701` option to expose the
container port 5701 on localhost. The command above uses that by
default.

On Linux it is enough to start the container with the following:

```bash
docker run hazelcast/hazelcast-jet
```

And submit the job, specifying the cluster address:

```bash
bin/jet -a 172.17.0.2 submit examples/hello-world.jar
```

## Configure Hazelcast Jet

### JAVA_OPTS

As shown below, you can use `JAVA_OPTS` environment variable if you need
to pass multiple VM arguments to your Hazelcast Jet member.

```bash
docker run -e JAVA_OPTS="-Xms512M -Xmx1024M" -p 5701:5701 hazelcast/hazelcast-jet
```

### Custom Hazelcast Jet Configuration File

If you need to configure Hazelcast Jet with your own
`hazelcast-jet.yaml`, or `hazelcast.yaml`, you just need to mount the
file to configuration file location in the container. The configuration
files are stored in the `/opt/hazelcast-jet/config` directory.

For example to change the cluster name, create a file `hazelcast.yaml`
with the following content:

```yaml
hazelcast:
  # The name of the cluster. All members of a single cluster must have the
  # same cluster name configured and a client connecting to this cluster
  # must use it as well.
  cluster-name: jet-1
```

Then use it when starting the Jet instance by running the following
command (it expects the file in the current directory):

```bash
docker run -v "$(pwd)"/hazelcast.yaml:/opt/hazelcast-jet/config/hazelcast.yaml hazelcast/hazelcast-jet
```

Submit a job to the cluster, specifying cluster name in the jet command
using `-n jet-1` option (shortcut for `--cluster-name`):

```bash
docker run -it -v "$(pwd)"/examples:/examples hazelcast/hazelcast-jet jet -a 172.17.0.2 -n jet-1 submit /examples/hello-world.jar
```

## Start Jet Cluster

While running a cluster locally does not give you any computational
advantage, it allows you to test your pipelines in a distributed
environment.

The simplest way to start a cluster is to start multiple docker
instances. Just run the `docker run` command from multiple terminals,
remember to use a different port each time:

```bash
docker run -p 5701:5701 hazelcast/hazelcast-jet
...
docker run -p 5702:5701 hazelcast/hazelcast-jet
```

Eventually, the instances should discover each other and you should
see an output similar to the following:

```text
Members {size:2, ver:2} [
    Member [172.17.0.2]:5701 - df428435-8cbd-464d-8671-afe396f5eef6
    Member [172.17.0.3]:5701 - 011a9dd4-936f-4170-a7c6-622b7430b789 this
]
```  

You can use any instance to submit jobs to the cluster. Jet will
take care of distributing it across all instances.

## Docker Compose Example

When the configuration is more involved such as  including networks and
volumes etc. it is usually easier to create a `docker-compose.yml` file.
It also provides an advantage that you can easily scale the Jet cluster:

Create a file called `docker-compose.yml`:

```yaml
version: '3'

services:
  jet:
    image: hazelcast/hazelcast-jet
    ports:
      - "5701-5703:5701"
```

And then start 3-node cluster by running the following command:

```bash
docker-compose up --scale jet=3
```

You should eventually see a 3-node cluster formed. Note that you will
see the output multiple times, prefixed by the container name:

```text
jet_3  | Members {size:3, ver:3} [
jet_3  |    Member [172.21.0.3]:5701 - 99d3de67-8c5d-452b-8165 -085a4cd1fcda
jet_3  |    Member [172.21.0.2]:5701 - 64f5b01b-847e-49e0-87f2 -db2a6f7750b7
jet_3  |    Member [172.21.0.4]:5701 - ffa362f9-617d-42bc-a74c -05ce857e8e48 this
jet_3  | ]
```

The `ports` section says that port 5701 from each container should be
mapped to a port from the range 5701-5703. Increase the range if you
would like to start more than 3 instances.

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

To see the default content of the files run the following command:

```bash
docker run hazelcast/hazelcast-jet cat /opt/hazelcast-jet/config/hazelcast-jet.yaml
```

## Use Built-in Modules

The image comes with several officially supported connectors. You can
find out the module names on the
[Sources and Sinks](../api/sources-sinks.md#summary) page.

To use one or more modules provide a list of comma separated names in
`JET_MODULES` environment variable when running the `docker run`
command:

```bash
docker run -e JET_MODULES="avro,kafka" -p 5701:5701 hazelcast/hazelcast-jet
```

## Package Job as a Docker Image

When your architecture is Docker focused it might be easier to have
also your job packaged as a Docker image. We will use Hazelcast Jet
image as the base image and create an image with Hello World job. You
can use the same approach to package any job.

Create a file called `Dockerfile` in the `<jet home>` directory.

```text
FROM hazelcast/hazelcast-jet

ADD examples/hello-world.jar /examples/

ENV ADDRESSES 172.17.0.2

CMD ["sh", "-c", "jet -a $ADDRESSES submit /examples/hello-world.jar"]
```

The first line specifies the `hazelcast/hazelcast-jet` image as base
image. The `ADD` command copies the `hello-world.jar` to the `/examples`
directory inside the container image. `ENV` command defines an
environment variable `ADDRESSES` with default value of `172.17.0.2`.
This can be overriden via `-e` parameter of the `docker run`
command. The `CMD` command specifies what command to run.

Then create the image by running the following command:

```bash
docker build . -t hazelcast-jet-hello-world
```

You should see output similar to the following:

```text
Sending build context to Docker daemon  77.35MB
Step 1/4 : FROM hazelcast/hazelcast-jet:{jet-version}
 ---> c126c24c3d0b
Step 2/4 : ADD examples/hello-world.jar /examples/
 ---> Using cache
 ---> 2e3fa98d0c7e
Step 3/4 : ENV ADDRESSES 172.17.0.2
 ---> Using cache
 ---> f96d01158d6e
Step 4/4 : CMD ["sh", "-c", "jet -a $ADDRESSES submit /examples/hello-world.jar"]
 ---> Using cache
 ---> 6bc0f527b69c
Successfully built 6bc0f527b69c
Successfully tagged hazelcast-jet-hello-world:latest
```

Now you can submit the job by creating a container from the newly built
image:

```bash
docker run -it hazelcast-jet-hello-world
```

> You don't need the `-i` and `-t` parameters if your job class exits
> after the job is submitted.
