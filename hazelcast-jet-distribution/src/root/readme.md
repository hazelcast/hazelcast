# Hazelcast Jet 

Hazelcast Jet is an open-source, cloud-native, distributed stream
processing engine. It handles both unbounded (stream) and bounded 
(batch) data sources.

## What's included

* `bin/jet-start`: starts a new Jet instance
* `bin/jet-stop`: stops all Jet instances running on this machine
* `bin/jet`: submits and manages jobs
* `bin/jet-cluster-admin`: manages the Jet cluster (for example, shutting
  down the cluster)
* `config/hazelcast-jet.yaml`: The jet configuration file
* `config/hazelcast.yaml`: The IMDG configuration used by the Jet instance
* `config/hazelcast-client.yaml`: The client configuration file used by the 
  Jet Command Line client
* `config/log4j.properties`: Logging configuration used by the Jet Instance
* `opt`: Optional sources and sinks for Jet. The following are included:
    * Kafka
    * Hadoop
    * S3
    * Avro
   
## Quickstart

### 1. Starting a Jet Instance

On a terminal prompt, enter the command below:

```
$ bin/jet-start
```

### 2. (optional) Start a second node to form a cluster

Repeat the first step on the terminal and you should see output like below,
after the nodes have  discovered each other. 

```
Members {size:2, ver:2} [
	Member [192.168.0.2]:5701 - 399aa674-cc73-4bae-8451-67943efc4a66 this
	Member [192.168.0.2]:5702 - 79b6399b-433f-4d61-b0d9-38ec121af07b
]
```

Note: By default Jet uses multicast, which may be disabled in some
environments. In this case you can enable the TCP-IP join inside 
`config/hazelcast.yaml`. For more details, please see the the section on
setting up clusters in [Hazelcast IMDG's reference
manual](https://docs.hazelcast.org/docs/3.12.3/manual/html-single/index.html#setting-up-clusters)

### 3. Submitting a Job

You can submit the hello world example to the cluster using the command below:

```
$ bin/jet submit examples/hello-world.jar
```

This job creates a stream of random numbers and then calculates the
top 10 largest values observed so far, writing the results to an IMap.
The contents of the IMap are then periodically printed out. 
You can play around with the various fault tolerance features of Jet by
adding a second node, and then killing it again and observing 
how the job is affected. You can also monitor the job using the Jet 
command line like below:

```
$ bin/jet list-jobs
```

## Additional Libraries

To use the additional libraries provided in the opt folder, you can move them
into the `lib` folder and they will be automatically added to the classpath next time
Jet is started.

## Additional Information

* [Hazelcast Jet on GitHub](https://github.com/hazelcast-jet)
* [Hazelcast Jet Homepage](https://jet.hazelcast.org)
* [Hazelcast Jet Reference Manual](https://docs.hazelcast.org/docs/jet/latest/manual/)