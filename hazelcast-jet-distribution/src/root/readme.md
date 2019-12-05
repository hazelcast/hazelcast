Hazelcast Jet Readme
======================

About Hazelcast Jet
-------------------
 
Hazelcast Jet is an open-source, cloud-native, distributed stream
processing engine. It handles both unbounded (stream) and bounded 
(batch) data sources.

What's Included
---------------

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
   
Quickstart
----------

### 1. Start a Jet Instance

On a terminal prompt, with the unpacked distribution's root as your current
directory, type this:

```
$ bin/jet-start
```

### 2. (Optional) Start a Second Node to Form a Cluster

Repeat the first step on the terminal and within the log output you
should see the text like below, after the nodes have discovered each other: 

```
Members {size:2, ver:2} [
	Member [192.168.0.2]:5701 - 399aa674-cc73-4bae-8451-67943efc4a66 this
	Member [192.168.0.2]:5702 - 79b6399b-433f-4d61-b0d9-38ec121af07b
]
```

Note: By default Jet nodes use IP multicast to discover each other. This
network feature is often disabled in managed cloud environments. In this 
case you can enable the TCP-IP join inside 
`config/hazelcast.yaml`. For more details, please see the the section on
setting up clusters in [Hazelcast IMDG's reference
manual](https://docs.hazelcast.org/docs/3.12.3/manual/html-single/index.html#setting-up-clusters)

### 4. Submit a Job

In order to validate your cluster setup, submit the provided Hello World
example:

```
$ bin/jet submit examples/hello-world.jar
```

This job creates a stream of random numbers and then calculates the
top 10 largest values observed so far, writing the results to an IMap.
It periodically prints the contents of the IMap.
 
You can play around with the various fault tolerance features of Jet by
adding and removing Jet instances to the cluster and observe how the job
is affected. You can also monitor the job from the command line :

```
$ bin/jet list-jobs
```

Additional Libraries
--------------------

To use an additional library provided in the opt folder, move it to the
`lib` folder. Jet will automatically use it the next time you start it.


Additional Information
----------------------

* [Hazelcast Jet on GitHub](https://github.com/hazelcast-jet)
* [Hazelcast Jet Homepage](https://jet.hazelcast.org)
* [Hazelcast Jet Reference Manual](https://docs.hazelcast.org/docs/jet/latest/manual/)
