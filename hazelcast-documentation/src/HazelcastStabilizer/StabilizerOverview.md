
# Hazelcast Stabilizer

## Stabilizer Overview

Hazelcast Stabilizer is a production simulator API used to test Hazelcast and Hazelcast based applications in clustered environments. It also allows you to create your own tests and perform them on your Hazelcast clusters and applications deployed to the cloud computing environments. In your tests, you can provide any property (hardware, operating system, Java version, etc.) that can be specified on these environments like Amazon Web Services (AWS) and Google Compute Engine (GCE).

Hazelcast Stabilizer allows you to add potential production problems like real-life failures, network problems, overloaded CPU and failing nodes to your tests. It also provides a benchmarking and performance testing platform by supporting performance tracking and various out of the box profilers.

Hazelcast Stabilizer makes use of Apache JClouds, an open source multi-cloud toolkit, and is primarily designed for testing on the clouds like Amazon EC2 and GCE.

You can use Hazelcast Stabilizer for the following example use cases:

- When upgrading your Hazelcast version
- ???
- ???
- ???

Hazelcast Stabilizer is available as a downloadable package on the Hazelcast [web site](http://www.hazelcast.org/downloads). Please refer to the [Installing Stabilizer section](#installing-stabilizer) for more information.


## Key Concepts

The following are the key concepts mentioned with Hazelcast Stabilizer.

- **Test** -  The functionality you want to test, e.g. a map. It may seem like a JUnit test, but it does not use annotations and has a bunch of methods that one can be overriden.

- **TestSuite** -  A property file that contains the name of the test class and the properties you want to set on that test class instance. In most cases, a `testsuite` contains a single test class, but you can configure multiple tests within a single `testsuite`.

- **Failure** -  An indication that something has gone wrong. Failures are picked up by the Agent and sent back to the Coordinator. Please see below for the descriptions of the Agent and Coordinator.

- **Worker** - A Java Virtual Machine (JVM) responsible for running a `testsuite`.

- **Agent** - A JVM installed on a piece of hardware. Its main responsibility is spawning, monitoring and terminating the workers.

- **Coordinator** -  A JVM that can run anywhere, e.g. on your local machine. It is actually responsible for running the test using the agents. You configure it with a list of Agent IP addresses and send a command like "run this testsuite with 10 worker JVMs for 2 hours".

- **Provisioner** -  It is responsible for spawning and terminating EC2 instances and installing Agents on the remote machines. It can be used in combination with EC2 (or any other cloud), but it can also be used in a static setup like a local machine or a cluster of machines in your data center.

- **Communicator** -  A JVM that enables the communication between the agents and workers.