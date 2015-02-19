
# Hazelcast Stabilizer

## Stabilizer Overview

Hazelcast Stabilizer is a production simulator to perform tests on Hazelcast and Hazelcast based applications in clustered environments. It allows you to create your own tests and perform tests against anything that can be specified on the virtual computing environments like Amazon Web Services (AWS) and Google Compute Engine (GCE) specific hardware, operating system, Java version etc - anything that can be specified on AWS or GCE. It can also be used to reproduce a customer issue.
To simulate production, it also allows us to add real-life failures. Network problems, nodes dying, high CPU etc. Much like Netflix Chaos Monkey.
Provides a platform for benchmarking/performance-testing: it has basic support for performance tracking (more will be added in the 0.4 release) and has out of the box support for various profilers etc.



This can be in a local machine, but can also be in a cloud like EC2 or Google Compute Engine. The Stabilizer makes use of JClouds, so in theory we can roll out in any cloud.

Stabilizer includes a test suite for our own stress simulation, but you can fork this repo, and add your own.

Commercially we offer support agreements where we will integrate your tests into our runs for new releases so that your tests act as an Application TCK.

Additional content: The goal of Stabilizer is to verify Hazelcast and Hazelcast Enterprise by deploying it in simulated customer situations and adding stresses and loads to it.
Secondly to allow us to reproduce customer issues.

Hazelcast Stabilizer consists of the following components:
- User code
- Stabilizer Framework
- JClouds
- Linux OS tools and drivers
- EC2 cluster or physical hardware
- Chef


The goal of the Stabilizer is to put load on a Hazelcast environment so that we can verify its resilience to all kinds of problems:
total failure of networking e.g. through ip table changes
partial failure of networking by network congestion, dropping packets etc etc
total failure of a JVM, e.g. by calling kill -9 or with different termination levels. 
partial failure of JVM, e.g. send a message to it that it should start claiming e.g. 90% of the available heap and see what happens. Or send a message that a trainee should spawn x threads that consume all cpu. Or send random packets with huge size, or open many connections to a HazelcastInstance. 
total failure of the OS (e.g calling shutdown or an EC2 api to do the hard kill)
internal buffers overflow by running for a long time.
The stability test should give a lot more confidence that Hazelcast will operate fine in completely failing and partly failing environments.

## Key Concepts

Test: the functionality you want to test, e.g. a map. It looks a bit like a JUnit test, but it doesn't use annotations and has a bunch of methods that one can override.

TestSuite: this is a property file that contains the name of the Test class and the properties you want to set on that test class instance. In most cases a testsuite contains a single test class, but you can configure multiple tests within a single testsuite.

Failure: an indication that something has gone wrong. E.g. the Worker crashed with an OOME, an exception occurred while doing a map.get or the result of some test didn't give the expected answer. Failures are picked up by the Agent and send back to the coordinator.

Worker: a JVM responsible for running a TestSuite.

Agent: a JVM installed on a piece of hardware. Its main responsibility is spawning, monitoring and terminating workers.

Coordinator: a JVM that can run anywhere, e.g. on your local machine. You configure it with a list of Agent ip addresses and you send a command like "run this testsuite with 10 worker JVMs for 2 hours".

Provisioner: responsible for spawning/terminating EC2 instances and to install Agents on remote machines. It can be used in combination with EC2 (or any other cloud), but it can also be used in a static setup like a local machine or the test cluster we have in Istanbul office.

Communicator: ???