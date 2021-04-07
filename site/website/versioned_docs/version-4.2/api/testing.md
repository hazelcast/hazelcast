---
title: Testing
description: Test support offered by Jet for testing the correctness of your processing pipelines.
id: version-4.2-testing
original_id: testing
---

Hazelcast Jet project uses JUnit testing framework to test
itself in various scenarios. Over the years there has been some
repetition within the test implementations and it led us to come up
with our own set of convenience methods for testing.

Hazelcast Jet provides test support classes to verify correctness of
your pipelines. Those support classes and the test sources are published
 to the Maven central repository for each version with the `tests`
classifier.

To start using them, please add following dependencies to your
project:

<!--DOCUSAURUS_CODE_TABS-->

<!--Gradle-->

```bash
compile 'com.hazelcast.jet:hazelcast-jet-core:4.2:tests'
compile 'com.hazelcast:hazelcast:4.0.1:tests'
```

<!--Maven-->

```xml
<dependency>
  <groupId>com.hazelcast.jet</groupId>
  <artifactId>hazelcast-jet-core</artifactId>
  <version>4.2</version>
  <classifier>tests</classifier>
</dependency>
<dependency>
  <groupId>com.hazelcast</groupId>
  <artifactId>hazelcast</artifactId>
  <version>4.0.1</version>
  <classifier>tests</classifier>
</dependency>
```

<!--END_DOCUSAURUS_CODE_TABS-->

After adding the dependencies to your project, the test classes should
be available to your project.

## Creating Hazelcast Jet instances with Mock Network

Test utilities contains factory methods to create Hazelcast Jet
instances with the mock network stack. Those Hazelcast Jet nodes
communicate with each other using intra-process communication methods.
This means we can create multiple lightweight Hazelcast Jet instances
in our tests without using any networking resources.

To create Hazelcast Jet instances with mock network(along with a lot
of convenience methods), you need to extend `com.hazelcast.jet.core.JetTestSupport`
class.

Here is a simple test harness which creates 2 node Hazelcast Jet cluster
with mock network:

```java
public class ClusteringTest extends JetTestSupport {

    @Test
    public void given_2nodeCluster_when...._then...() {
        // given
        JetInstance instance1 = createJetMember();
        JetInstance instance2 = createJetMember();
        // Alternatively
        // JetInstance[] instances = createJetMembers(2);

        // when
        ...
        ...
        // then
        ...
        ...
    }
...
}
```

If needed, a `JetConfig` object can be passed to the factory methods
like below:

```java
public class ClusteringTest extends JetTestSupport {

    @Test
    public void given_2nodeClusterWith16CooperativeThreads_when...._then...() {
        // given
        JetConfig config = new JetConfig();
        config.getInstanceConfig().setCooperativeThreadCount(16);

        JetInstance[] instances = createJetMembers(config, 2);
        // when
        ...
        ...
        // then
        ...
        ...
    }
...
}
```

Similar to the Hazelcast Jet nodes, you can create Hazelcast Jet clients
with the same factories. There is no need to provide any network
configuration for clients to discover nodes since they are using the
same factory. The discovery will work out of the box.

```java
public class ClusteringTest extends JetTestSupport {

    @Test
    public void given_2nodeClusterAndClient_when...._then...() {
        // given
        JetInstance[] instances = createJetMembers(2);
        JetInstance client = createJetClient();
        // when
        ...
        ...
        // then
        ...
        ...
    }
...
}
```

When the above test run it should create 2 Hazelcast Jet nodes and a
Hazelcast Jet client connected to them. When run, it can be verified
that they form the cluster from the logs and client connected to them.

```log
10:45:03.927 [ INFO] [c.h.i.c.ClusterService]
....
Members {size:2, ver:2} [
 Member [127.0.0.1]:5701 - 93328d97-0975-4dfa-bf56-4d46e8a469a1 this
 Member [127.0.0.1]:5702 - 920d1b0c-0898-4b6e-9009-8f29889d6a77
]

10:45:03.933 [ INFO] [c.h.i.c.ClusterService]

Members {size:2, ver:2} [
 Member [127.0.0.1]:5701 - 93328d97-0975-4dfa-bf56-4d46e8a469a1
 Member [127.0.0.1]:5702 - 920d1b0c-0898-4b6e-9009-8f29889d6a77 this
]
....
10:45:04.890 [ INFO] [c.h.c.i.s.ClientClusterService]

Members [2] {
 Member [127.0.0.1]:5701 - 93328d97-0975-4dfa-bf56-4d46e8a469a1
 Member [127.0.0.1]:5702 - 920d1b0c-0898-4b6e-9009-8f29889d6a77
}
```

First two blocks are the member list printed from each member's point
of view and the last one is the cluster from the client's point of view.

So far, we've seen how to create any number of Hazelcast Jet clusters
and clients using factory methods provided within the `com.hazelcast.jet.core.JetTestSupport`
class to create the test environments. Let's explore other utilities to
write meaningful test.

## Integration Tests

For integration testing, there might be a need to create real instances
without the mock network. For those cases you can create real instances
with `Jet.newJetInstance()` method.

## Using random cluster names

If multiple tests are running in parallel there is a chance that the
clusters in each test can discover others, interfere the test
execution and most of the time causing both of them to fail.

To avoid such scenarios, you need to isolate the clusters in each test
execution by giving them unique cluster names. This way, they won't
try to connect each other since the nodes will only try to connect to
other members with the same cluster name property.

Random cluster name for each test execution can be generated like
below:

```java

public class ClusteringTest extends JetTestSupport {

    @Test
    public void given_2nodeClusterAndClient_when..._then...() {
        // given
        String clusterName = randomName();
        JetConfig jetConfig = new JetConfig();
        jetConfig.getHazelcastConfig().setClusterName(clusterName);
        JetInstance[] instances = createJetMembers(jetConfig, 2);

        JetClientConfig clientConfig = new JetClientConfig();
        clientConfig.setClusterName(clusterName);
        JetInstance client = createJetClient(clientConfig);
        // when
        ...
        ...
        // then
        ...
        ...
    }
...
}
```

In the example above `randomName()` utility method has been used to
generate a random string from `com.hazelcast.jet.core.JetTestSupport`
class.

## Cleaning up the resources

Mock instances created from the factory of `com.hazelcast.jet.core.JetTestSupport`
are cleaned-up automatically after the test execution has been finished.

If the test contains real instances, then they either needs to be
tracked individually and shut down when the test finished or you can
write a teardown method like below to shut down all instances created.

```java
    @After
    public void after() {
        Jet.shutdownAll();
    }
```

Either way you have to shut down Hazelcast Jet instances after the test
has been finished to reclaim resources and not to leave a room for
interference with the next test execution due to distributed
nature of the product.

## Test Sources and Sinks

Hazelcast Jet comes with batch and streaming test sources along with a
assertion sinks where you can write tests to assert the output of a
pipeline without having to write boilerplate code.

Test sources allow you to generate events for your pipeline.

### Batch Source

These sources create a fixed amount of data. These sources are
non-distributed.

```java
Pipeline p = Pipeline.create();
p.readFrom(TestSources.items(1, 2, 3, 4))
 .writeTo(Sinks.logger());
```

This will yield an output like below:

```text
12:33:01.780 [ INFO] [c.h.j.i.c.W.loggerSink#0] 1
12:33:01.780 [ INFO] [c.h.j.i.c.W.loggerSink#0] 2
12:33:01.780 [ INFO] [c.h.j.i.c.W.loggerSink#0] 3
12:33:01.780 [ INFO] [c.h.j.i.c.W.loggerSink#0] 4
```

### Streaming Source

Streaming sources create an infinite stream of data. The generated
events have timestamps and like the batch source, this source is also
non-distributed.

```java
int itemsPerSecond = 10;
pipeline.readFrom(TestSources.itemStream(itemsPerSecond))
        .withNativeTimestamps(0)
        .writeTo(Sinks.logger());
```

The source above will emit data as follows:

```text
12:33:36.774 [ INFO] [c.h.j.i.c.W.loggerSink#0] SimpleEvent(timestamp=12:33:36.700, sequence=0)
12:33:36.877 [ INFO] [c.h.j.i.c.W.loggerSink#0] SimpleEvent(timestamp=12:33:36.800, sequence=1)
12:33:36.976 [ INFO] [c.h.j.i.c.W.loggerSink#0] SimpleEvent(timestamp=12:33:36.900, sequence=2)
12:33:37.074 [ INFO] [c.h.j.i.c.W.loggerSink#0] SimpleEvent(timestamp=12:33:37.000, sequence=3)
12:33:37.175 [ INFO] [c.h.j.i.c.W.loggerSink#0] SimpleEvent(timestamp=12:33:37.100, sequence=4)
12:33:37.274 [ INFO] [c.h.j.i.c.W.loggerSink#0] SimpleEvent(timestamp=12:33:37.200, sequence=5)
```

## Assertions

Hazelcast Jet contains several sinks to support asserting directly in
the pipeline. Furthermore, there's additional convenience to have the
assertions done inline with the sink without having to terminate the
pipeline, using the `apply()` operator.

### Batch Assertions

Batch assertions collect all incoming items, and perform assertions on
the collected list after all the items are received. If the assertion
passes, then no exception is thrown. If the assertion fails, then the
job will fail with an AssertionError.

#### Ordered Assertion

This asserts that items have been received in a certain order and no
other items have been received. Only applicable to batch jobs.

```java
pipeline.readFrom(TestSources.items(1, 2, 3, 4))
        .apply(Assertions.assertOrdered("unexpected values", Arrays.asList(1, 2, 3, 4)))
        .writeTo(Sinks.logger())
```

#### Unordered Assertion

Asserts that items have been received in any order and no other items
have been received. Only applicable to batch stages.

```java
pipeline.readFrom(TestSources.items(4, 3, 2, 1)
        .apply(Assertions.assertAnyOrder("unexpected values", Arrays.asList(1, 2, 3, 4)))
        .writeTo(Sinks.logger())
```

#### Contains Assertions

Assert that the given items have been received in any order; receiving
other, unrelated items does not affect this assertion. Only applicable
to batch stages.

```java
pipeline.readFrom(TestSources.items(4, 3, 2, 1))
        .apply(Assertions.assertContains(Arrays.asList(1, 3)))
        .writeTo(Sinks.logger())
```

#### Collected Assertion

This is a more flexible assertion which is only responsible for
collecting the received items, and passes the asserting responsibility
to the user. It is a building block for the other assertions. Only
applicable to batch stages.

```java
pipeline.readFrom(TestSources.items(1, 2, 3, 4))
        .apply(Assertions.assertCollected(items -> assertTrue("expected minimum of 4 items", items.size() >= 4)))
        .writeTo(Sinks.logger())
```

### Streaming Assertions

For streaming assertions, it's not possible to assert after all items
have been received, as the stream never terminates. Instead, we
periodically assert and throw if the assertion is not valid after a
given period of time. However even if the assertion passes, we don't
want to the job to continue running forever. Instead a special
exception `AssertionCompletedException` is thrown to signal the
assertion has passed successfully.

#### Collected Eventually Assertion

This assertion collects incoming items and runs the given assertion
function repeatedly on the received item set. If the assertion passes at
any point, the job will be completed with an
`AssertionCompletedException`. If the assertion fails after the given
timeout period, the job will fail with an `AssertionError`.

```java
pipeline.readFrom(TestSources.itemStream(10))
        .withoutTimestamps()
        .apply(assertCollectedEventually(5, c -> assertTrue("did not receive at least 20 items", c.size() > 20)))
```

The pipeline above with fail with an `AssertionError` if 20 items are
not received after 5 seconds. The job will complete with an `AssertionCompletedException`
 as soon as 20 items or more are received.

### Assertion Sink Builder

Both the batch and streaming assertions use an assertion sink builder
for building the assertions. Although a lower-level API, this is also
public and can be used to build other, more complex assertions if
desired:

```java
/**
 * Returns a builder object that offers a step-by-step fluent API to build
 * an assertion {@link Sink} for the Pipeline API. An assertion sink is
 * typically used for testing of pipelines where you can want to run
 * an assertion either on each item as they arrive, or when all items have been
 * received.
 * <p>
 * These are the callback functions you can provide to implement the sink's
 * behavior:
 * <ol><li>
 *     {@code createFn} creates the state which can be used to hold incoming
 *     items.
 * </li><li>
 *     {@code receiveFn} gets notified of each item the sink receives
 *     and can either assert the item directly or add it to the state
 *     object.
 * </li><li>
 *     {@code timerFn} is run periodically even when there are no items
 *     received. This can be used to assert that certain assertions have
 *     been reached within a specific period in streaming pipelines.
 * </li><li>
 *     {@code completeFn} is run after all the items have been received.
 *     This typically only applies only for batch jobs, in a streaming
 *     job this method may never be called.
 * </li></ol>
 * The returned sink will have a global parallelism of 1: all items will be
 * sent to the same instance of the sink.
 *
 * It doesn't participate in the fault-tolerance protocol,
 * which means you can't remember across a job restart which items you
 * already received. The sink will still receive each item at least once,
 * thus complying with the <em>at-least-once</em> processing guarantee. If
 * the sink is idempotent (suppresses duplicate items), it will also be
 * compatible with the <em>exactly-once</em> guarantee.
 *
 * @param <A> type of the state object
 *
 * @since 3.2
 */
@Nonnull
public static <A> AssertionSinkBuilder<A, Void> assertionSink(
        @Nonnull String name,
        @Nonnull SupplierEx<? extends A> createFn
) {
  ..
}
```

In addition to the assertions mentioned above, `com.hazelcast.jet.core.JetTestSupport`
contains a lot of assertion methods which can be used to verify whether
the job/member/cluster is in desired state.

## Class Runners

There are multiple JUnit test class runners shipped with the tests
package which gives various abilities.

The common features are:

- Ability to print a thread dump in case of a test failure, configured
 via `hazelcast.test.threadDumpOnFailure` property
- Supports repetitive test execution
- Uses mock networking, unless configured to use real networking via `hazelcast.test.use.network`
 property
- Disabled phone-home feature, configured via `hazelcast.phone.home.enabled`
 property
- Have shorter(1 sec) wait time before joining than default(5 secs).
 This leads to faster cluster formation and test execution, configured
 via `hazelcast.wait.seconds.before.join` property.
- Uses loopback address, configured via `hazelcast.local.localAddress`
 property
- Uses IPv4 stack, configured via `java.net.preferIPv4Stack`
 property
- Prints out test execution duration after they finish execution

Let's have a look at them in detail:

### Serial Class Runner

`com.hazelcast.test.HazelcastSerialClassRunner` is a JUnit test class
runner which runs the tests in series. Nothing fancy, it just executes
the tests with the features listed above.

### Parallel Class Runner

`com.hazelcast.test.HazelcastParallelClassRunner` is a JUnit test class
runner which runs the tests in parallel with multiple threads. If the
test methods within the test class does not share any resources this
yields a faster execution compared to it's serial counterpart.

### Repetitive Test Execution

While dealing with intermittently failing tests, it is helpful to run
the test multiple times in series to increase chances to make it
fail. In those cases `com.hazelcast.test.annotation.Repeat` annotation
can be used to run the test repeatedly. `@Repeat` annotation can be
used on both the class and method level. On the class level it repeats the
whole class execution specified items. On the method level it only
repeats particular test method.

Following is an example test which repeats the test method execution
5 times:

```java
@RunWith(HazelcastSerialClassRunner.class)
public class RepetitiveTest extends JetTestSupport {

    @Repeat(5)
    @Test
    public void test() {
        System.out.println("Test method to be implemented!");
    }
}
```

When run, it logs like the following:

```log
Started Running Test: test
---> Repeating test [RepetitiveTest:test], run count [1]
Test method to be implemented!
---> Repeating test [RepetitiveTest:test], run count [2]
Test method to be implemented!
---> Repeating test [RepetitiveTest:test], run count [3]
Test method to be implemented!
---> Repeating test [RepetitiveTest:test], run count [4]
Test method to be implemented!
---> Repeating test [RepetitiveTest:test], run count [5]
Test method to be implemented!
Finished Running Test: test in 0.009 seconds.
```

> Note: `@Repeat` annotation only works with Hazelcast Class runners.

## Waiting for job to be in desired state

On some use cases, you need to make the job is submitted and running
on the cluster before generating any events on the controlled source
to observe results. To achieve that following assertion could be used
to validate job is in the desired state.

```java

public class DesiredStateTest extends JetTestSupport {

    @Test
    public void given_singeNodeJet_when_jobIsRunning__then...() {
        // given
        JetInstance jet = createJetMember();

        // when
        Pipeline p = buildPipeline();
        Job job = jet.newJob(p);
        assertJobStatusEventually(job, JobStatus.RUNNING);

        // then
        ...
        ...
    }
...
}
```

In the example above `assertJobStatusEventually(Job, JobStatus)`
utility method has been used to validate the job is in the desired
state from the `com.hazelcast.jet.core.JetTestSupport` class.
