/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.test.bounce;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.bounce.BounceTestConfiguration.DriverType;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.nio.ClassLoaderUtil.isClassAvailable;
import static com.hazelcast.test.HazelcastTestSupport.sleepSeconds;
import static com.hazelcast.test.HazelcastTestSupport.spawn;
import static com.hazelcast.test.bounce.BounceTestConfiguration.DriverType.ALWAYS_UP_MEMBER;
import static com.hazelcast.test.bounce.BounceTestConfiguration.DriverType.CLIENT;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static java.lang.System.currentTimeMillis;

/**
 * JUnit rule for testing behavior of cluster while members bounce (shutdown & startup). Key concepts:
 * <ul>
 * <li>BounceMemberRule sets up a cluster before the test's {@code @Before} methods are executed and tears
 * it down after its {@code @After} methods. One member of the cluster is kept running for the duration of
 * the test, while other members are shutdown and replaced by new instances. For example, if the
 * BounceMemberRule is configured to setup a cluster of 5 members, one will stay up while the rest 4 will
 * be shutting down and replaced by new ones while the test tasks are executed. Use
 * {@link #getSteadyMember()} to obtain a reference to the member that is always up.<br/>
 * <b>Defaults: </b>cluster size 6 members<br/>
 * <b>Configuration: </b>
 * <ul>
 * <li>Cluster members configuration must be provided in {@link BounceMemberRule#with(Config)} which
 * returns a {@link Builder} to obtain a configured {@code BounceMemberRule}</li>
 * <li>Set cluster size with {@link Builder#clusterSize(int)}</li>
 * </ul>
 * </li>
 * <li>BounceMemberRule also initializes {@code HazelcastInstance}s to be used as test drivers. References
 * to test drivers can be obtained by invoking {@link #getNextTestDriver()}. Choices for test drivers
 * include:
 * <ul>
 * <li>{@code ALWAYS_UP_MEMBER}: use as test driver the cluster member that is never shutdown during
 * the test.</li>
 * <li>{@code MEMBER}: prepare a set of members, apart from the configured cluster members, to be used
 * as test drivers.</li>
 * <li>{@code CLIENT}: use clients as test drivers</li>
 * </ul>
 * <b>Defaults: </b> when {@code com.hazelcast.client.test.TestHazelcastFactory} is available on the
 * classpath, then defaults to preparing 5 {@code CLIENT} test drivers, otherwise uses
 * {@code ALWAYS_UP_MEMBER} as test driver.<br/>
 * <b>Configuration: </b>test driver type and count can be configured with the
 * {@link Builder#driverType(DriverType)} and {@link Builder#driverCount(int)} methods. For more control
 * over the configuration of test drivers, you may also specify a {@link DriverFactory} with
 * {@link Builder#driverFactory(DriverFactory)}.</li>
 * <li>The test case can be directly coded in your {@code @Test} methods, as with regular tests.
 * Alternatively, {@code Runnable}s can be submitted for concurrent execution in a fixed thread pool
 * initialized with as many threads as the number of {@code Runnable}s submitted:
 * <ul>
 * <li>{@link #testRepeatedly(int, Runnable, long)} submits the given {@code Runnable} to be executed
 * with the given number of concurrent threads. The submitted {@code Runnable} must be thread safe.
 * The {@code Runnable} is executed repeatedly until either the configured duration in seconds has
 * passed or its execution throws a {@code Throwable}, in which case the test is failed and the
 * exception is rethrown.</li>
 * <li>{@link #testRepeatedly(Runnable[], long)} same as above, however instead of submitting the same
 * {@code Runnable} to several threads, submits an array of {@code Runnable}s, allowing for concurrent
 * execution of heterogeneous {@code Runnable}s (e.g. several producers & consumers in an
 * {@code ITopic} test). {@code Runnable}s do not have to be thread-safe in this case.</li>
 * <li>{@link #test(Runnable[])} submits an array of {@code Runnable}s to be executed concurrently and
 * blocks until all {@code Runnable}s have been completed. If an exception was thrown during a
 * {@code Runnable}'s execution, the test will fail.</li>
 * </ul>
 * </li>
 * </ul>
 *
 * For examples on how to use this rule, see {@code BounceMemberRuleTest} and {@code QueryBounceTest}.
 *
 * {@code BounceMemberRule} integrates with test lifecycle as described below:
 * <ol>
 * <li>{@code BounceMemberRule} creates the cluster and test drivers</li>
 * <li>the test's {@code @Before} method(s) are executed to prepare the test. Use
 * {@link #getSteadyMember()} to obtain the {@code HazelcastInstace} that is kept running during the test
 * and prepare the test's environment.</li>
 * <li>{@code BounceMemberRule} spawns a separate thread that starts bouncing cluster members</li>
 * <li>the {@code @Test} method is executed</li>
 * <li>{@code BounceMemberRule} stops bouncing cluster members</li>
 * <li>the test's {@code @After} methods are executed</li>
 * <li>{@code BounceMemberRule} tears down test drivers and cluster members</li>
 * </ol>
 */
public class BounceMemberRule implements TestRule {

    private static final int DEFAULT_CLUSTER_SIZE = 6;
    private static final int DEFAULT_DRIVERS_COUNT = 5;

    private final BounceTestConfiguration bounceTestConfig;

    private TestHazelcastInstanceFactory factory;
    private HazelcastInstance[] members;
    private AtomicBoolean testRunning = new AtomicBoolean();
    private AtomicBoolean testFailed = new AtomicBoolean();
    private Future bouncingMembersTask;

    private AtomicInteger driverCounter = new AtomicInteger();
    private HazelcastInstance[] testDrivers;
    private ExecutorService taskExecutor;

    private BounceMemberRule(BounceTestConfiguration bounceTestConfig) {
        this.bounceTestConfig = bounceTestConfig;
    }

    /**
     * @return cluster member that stays up for the duration of the test
     */
    public final HazelcastInstance getSteadyMember() {
        return members[0];
    }

    /**
     * @return a {@code HazelcastInstance} to drive a test; if several test drivers are configured, this method will
     * rotate and return the next one.
     */
    public HazelcastInstance getNextTestDriver() {
        return testDrivers[driverCounter.getAndIncrement() % testDrivers.length];
    }

    /**
     * @return the {@code TestHazelcastInstanceFactory} from which cluster members and test drivers are created
     */
    public final TestHazelcastInstanceFactory getFactory() {
        return factory;
    }

    /**
     * @return the configuration for this BounceMemberRule
     */
    public BounceTestConfiguration getBounceTestConfig() {
        return bounceTestConfig;
    }

    /**
     * Submit the given {@code task} to be executed concurrently by {@code concurrency} number of threads.
     * The given {@code task} must be thread-safe, as the same instance will be reused by all threads that execute it.
     * The {@code task} is executed in a loop until either the configured test's duration is reached or an exception
     * is thrown from the {@code task}, in which case the test is marked as failed and the exception will be propagated
     * as test failure cause.
     *
     * This method blocks until all test tasks have been completed or one of them throws an exception.
     *
     * @param concurrency
     * @param task
     */
    public void testRepeatedly(int concurrency, Runnable task, long durationSeconds) {
        assert concurrency > 0 : "Concurrency level should be greater than 0";

        TestTaskRunable[] tasks = new TestTaskRunable[concurrency];
        Arrays.fill(tasks, new TestTaskRunable(task));
        testRepeatedly(tasks, durationSeconds);
    }

    /**
     * Submit Runnables to be executed repeatedly until test is done.
     * The {@code task} is executed in a loop until either the configured test's duration is reached or an exception
     * is thrown from the {@code task}, in which case the test is marked as failed and the exception will be propagated
     * as test failure cause.
     *
     * This method blocks until all test tasks have been completed or one of them throws an exception.
     *
     * @param tasks
     */
    public void testRepeatedly(Runnable[] tasks, long durationSeconds) {
        assert tasks != null && tasks.length > 0 : "Some tasks must be submitted for execution";

        TestTaskRunable[] runnables = new TestTaskRunable[tasks.length];
        for (int i = 0; i < tasks.length; i++) {
            runnables[i] = new TestTaskRunable(tasks[i]);
        }
        testWithDuration(runnables, durationSeconds);
    }

    /**
     * Submit Runnables to be executed concurrently. Each {@code task} is executed once. Exceptions thrown from a {@code task}
     * will mark the test as failed and the exception will be propagated as test failure cause.
     *
     * This method blocks until all test tasks have been completed or one of them throws an exception.
     *
     * @param tasks
     */
    public void test(Runnable[] tasks) {
        testWithDuration(tasks, 0);
    }

    private void testWithDuration(Runnable[] tasks, long durationSeconds) {
        // cannot submit tasks to an already executing test
        assert taskExecutor == null : "Cannot start test tasks on a bouncing member test that is already executing tasks";
        assert tasks != null && tasks.length > 0 : "Some tasks must be submitted for execution";

        Future[] futures = new Future[tasks.length];
        taskExecutor = Executors.newFixedThreadPool(tasks.length);
        for (int i = 0; i < tasks.length; i++) {
            futures[i] = taskExecutor.submit(tasks[i]);
        }

        // let the test run for test duration or until failed or finished, whichever comes first
        if (durationSeconds > 0) {
            long deadline = currentTimeMillis() + durationSeconds * 1000;
            while (currentTimeMillis() < deadline) {
                if (testFailed.get()) {
                    break;
                }
                sleepSeconds(1);
            }
            testRunning.set(false);
            waitForFutures(futures);
        } else {
            waitForFutures(futures);
            testRunning.set(false);
        }
    }

    public static Builder with(Config memberConfig) {
        return new Builder(memberConfig);
    }

    @Override
    public Statement apply(Statement base, Description description) {
        return new BouncingClusterStatement(base);
    }

    /**
     * A wrapper {@link Statement} that initializes a cluster with given cluster size and configuration, evaluates the
     * wrapped statement, then tears down the cluster.
     */
    private class BouncingClusterStatement extends Statement {
        private final Statement statement;

        public BouncingClusterStatement(Statement statement) {
            this.statement = statement;
        }

        @Override
        public void evaluate() throws Throwable {
            try {
                setup();
                statement.evaluate();
            } finally {
                tearDown();
            }
        }
    }

    // Start cluster with configured number of members and member configuration
    private void setup() {
        assert (bounceTestConfig.getClusterSize() > 1) : "Cluster size must be at least 2.";

        if (bounceTestConfig.getDriverType() == CLIENT) {
            factory = newTestHazelcastFactory();
        } else {
            factory = new TestHazelcastInstanceFactory();
        }
        Config memberConfig = bounceTestConfig.getMemberConfig();
        members = new HazelcastInstance[bounceTestConfig.getClusterSize()];
        for (int i = 0; i < bounceTestConfig.getClusterSize(); i++) {
            members[i] = factory.newHazelcastInstance(memberConfig);
        }

        // setup drivers
        testDrivers = bounceTestConfig.getDriverFactory().createTestDrivers(this);

        testRunning.set(true);
    }

    private void tearDown() {
        try {
            if (taskExecutor != null) {
                taskExecutor.shutdown();
            }
            // shutdown test drivers first
            if (testDrivers != null) {
                for (HazelcastInstance hz : testDrivers) {
                    hz.shutdown();
                }
            }
            if (factory != null) {
                factory.shutdownAll();
            }
        } catch (Throwable t) {
            // any exceptions thrown in tearDown are not interesting and may hide the actual failure, so are ignored
        }
    }

    // reflectively instantiate client-side TestHazelcastFactory
    private TestHazelcastInstanceFactory newTestHazelcastFactory() {
        try {
            Class klass = ClassLoaderUtil.loadClass(null, "com.hazelcast.client.test.TestHazelcastFactory");
            return (TestHazelcastInstanceFactory) klass.newInstance();
        } catch (Exception e) {
            throw new AssertionError("Could not instantiate client TestHazelcastFactory: " + e.getMessage());
        }
    }

    /**
     * @return a Statement which starts a separate thread that bounces members of the cluster until {@link #testRunning} is
     * {@code false}, then proceeds with the given {@code next} Statement.
     */
    public final Statement startBouncing(final Statement next) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                bouncingMembersTask = spawn(new MemberUpDownMonkey(members));
                next.evaluate();
            }
        };
    }

    /**
     * @param next Statement to evaluate before stopping bouncing members
     * @return a Statement which executes the {@code next} Statement, then sets the test's running flag to false and waits for
     * the member bouncing thread to complete.
     */
    public final Statement stopBouncing(final Statement next) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                try {
                    next.evaluate();
                } finally {
                    testRunning.set(false);
                    try {
                        bouncingMembersTask.get();
                    } catch (Exception e) {
                        // ignore
                    }
                }
            }
        };
    }

    // wait until all test tasks complete or one of them throws an exception
    private void waitForFutures(Future[] futures) {
        List<Future> futuresToWaitFor = new ArrayList<Future>(Arrays.asList(futures));
        while (!futuresToWaitFor.isEmpty()) {
            Iterator<Future> iterator = futuresToWaitFor.iterator();
            while (iterator.hasNext()) {
                boolean hasTestFailed = testFailed.get();
                Future future = iterator.next();
                try {
                    // if the test failed, try to locate immediately the future that is done and will throw an exception
                    if ((hasTestFailed && future.isDone()) || !hasTestFailed) {
                        future.get();
                        iterator.remove();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    rethrow(e.getCause());
                }
            }
        }
    }

    public static class Builder {
        private final Config memberConfig;
        private int clusterSize = DEFAULT_CLUSTER_SIZE;
        private int driversCount = DEFAULT_DRIVERS_COUNT;
        private DriverFactory driverFactory;
        private DriverType testDriverType;

        private Builder(Config memberConfig) {
            this.memberConfig = memberConfig;
        }

        public BounceMemberRule build() {
            if (testDriverType == null) {
                // guess driver: if HazelcastTestFactory class is available, then use the client driver,
                // otherwise default to always-up member as test driver
                if (isClassAvailable(null, "com.hazelcast.client.test.TestHazelcastFactory")) {
                    testDriverType = CLIENT;
                } else {
                    testDriverType = ALWAYS_UP_MEMBER;
                }
            }
            if (driverFactory == null) {
                // choose a default driver factory
                switch (testDriverType) {
                    case ALWAYS_UP_MEMBER:
                    case MEMBER:
                        driverFactory = new MemberDriverFactory();
                        break;
                    case CLIENT:
                        driverFactory = newDefaultClientDriverFactory();
                        break;
                    default:
                        throw new AssertionError("Cannot instantiate driver factory for driver type " + testDriverType);
                }
            }
            return new BounceMemberRule(new BounceTestConfiguration(clusterSize, testDriverType, memberConfig,
                    driversCount, driverFactory));
        }

        public Builder clusterSize(int clusterSize) {
            this.clusterSize = clusterSize;
            return this;
        }

        public Builder driverType(DriverType testDriverType) {
            this.testDriverType = testDriverType;
            return this;
        }

        public Builder driverCount(int count) {
            this.driversCount = count;
            return this;
        }

        public Builder driverFactory(DriverFactory driverFactory) {
            this.driverFactory = driverFactory;
            return this;
        }

        // reflectively instantiate default client-side test driver factory
        private DriverFactory newDefaultClientDriverFactory() {
            try {
                Class factoryClass = ClassLoaderUtil.loadClass(null, "com.hazelcast.client.test.bounce.ClientDriverFactory");
                return (DriverFactory) factoryClass.newInstance();
            } catch (Exception e) {
                throw new AssertionError("Could not instantiate client DriverFactory: " + e.getMessage());
            }
        }
    }

    /**
     * Shuts down and restarts members of the cluster
     */
    protected class MemberUpDownMonkey implements Runnable {
        private final HazelcastInstance[] instances;

        MemberUpDownMonkey(HazelcastInstance[] allInstances) {
            this.instances = new HazelcastInstance[allInstances.length - 1];
            // exclude 0-index instance
            System.arraycopy(allInstances, 1, instances, 0, allInstances.length - 1);
        }

        @Override
        public void run() {
            int i = 0;
            int nextInstance;
            try {
                while (testRunning.get()) {
                    instances[i].shutdown();
                    nextInstance = (i + 1) % instances.length;
                    sleepSeconds(2);

                    instances[i] = factory.newHazelcastInstance();
                    sleepSeconds(2);
                    // move to next member
                    i = nextInstance;
                }
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
    }

    /**
     * Wraps a {@code Runnable} to be executed repeatedly until testRunning becomes false or an exception is thrown
     * which sets the test as failed.
     */
    private final class TestTaskRunable implements Runnable {
        private final Runnable task;

        public TestTaskRunable(Runnable task) {
            this.task = task;
        }

        @Override
        public void run() {
            while (testRunning.get()) {
                try {
                    task.run();
                } catch (Throwable t) {
                    testFailed.set(true);
                    rethrow(t);
                }
            }
        }
    }
}
