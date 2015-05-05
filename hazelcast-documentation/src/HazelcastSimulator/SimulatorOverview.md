
# Hazelcast Simulator

## Simulator Overview

Hazelcast Simulator is a production simulator used to test Hazelcast and Hazelcast-based applications in clustered environments. It also allows you to create your own tests and perform them on your Hazelcast clusters and applications that are deployed to cloud computing environments. In your tests, you can provide any property that can be specified on these environments (Amazon EC2, Google Compute Engine(GCE), or your own environment): properties such as hardware specifications, operating system, Java version, etc.

Hazelcast Simulator allows you to add potential production problems, such as real-life failures, network problems, overloaded CPU, and failing nodes to your tests. It also provides a benchmarking and performance testing platform by supporting performance tracking and also supporting various out-of-the-box profilers.

Hazelcast Simulator makes use of Apache jclouds&reg;, an open source multi-cloud toolkit that is primarily designed for testing on the clouds like Amazon EC2 and GCE.

You can use Hazelcast Simulator for the following use cases:

- In your pre-production phase to simulate the expected throughput/latency of Hazelcast with your specific requirements.
- To test if Hazelcast behaves as expected when you implement a new functionality in your project.
- As part of your test suite in your deployment process.
- When you upgrade your Hazelcast version.

Hazelcast Simulator is available as a downloadable package on the Hazelcast [web site](http://www.hazelcast.org/download). Please refer to the [Installing Simulator section](#installing-simulator) for more information.

## Key Concepts

The following are the key concepts mentioned with Hazelcast Simulator.

- **Test** -  A test class for the functionality you want to test, such as a Hazelcast map. This test class may seem like a JUnit test, but it uses custom annotations to define methods for different test phases (e.g. setup, warmup, run, verify).

- **TestSuite** -  A property file that contains the name of the test class and the properties you want to set on that test class instance. In most cases, a `TestSuite` contains a single test class, but you can configure multiple tests within a single `TestSuite`.

- **Failure** -  An indication that something has gone wrong. Failures are picked up by the `Agent` and sent back to the `Coordinator`. Please see the descriptions below for the `Agent` and `Coordinator`.

- **Worker** - A Java Virtual Machine (JVM) responsible for running a `TestSuite`. It can be configured to spawn a Hazelcast client or member instance.

- **Agent** - A JVM installed on a piece of hardware. Its main responsibility is spawning, monitoring and terminating `Workers`.

- **Coordinator** -  A JVM that can run anywhere, such as on your local machine. **Coordinator** is actually responsible for running the test using the `Agents`. You configure it with a list of `Agent` IP addresses, and you run it by sending a command like "run this testsuite with 10 worker JVMs for 2 hours".

- **Provisioner** -  Spawns and terminates cloud instances, and installs `Agents` on the remote machines. It can be used in combination with EC2 (or any other cloud), but it can also be used in a static setup, such as a local machine or a cluster of machines in your data center.

- **Communicator** -  A JVM that enables the communication between the `Agents` and `Workers`.

- `simulator.properties` - The configuration file you use to adapt the Hazelcast Simulator to your business needs (e.g. cloud selection and configuration).
