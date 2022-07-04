/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
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
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.StringUtil.timeToString;
import static com.hazelcast.test.HazelcastTestSupport.sleepSeconds;
import static com.hazelcast.test.bounce.BounceTestConfiguration.DriverType.ALWAYS_UP_MEMBER;
import static com.hazelcast.test.bounce.BounceTestConfiguration.DriverType.CLIENT;
import static java.lang.Math.max;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * JUnit rule for testing behavior of cluster while members bounce (shutdown & startup). Key concepts:
 * <ul>
 * <li>BounceMemberRule sets up a cluster before the test's {@code @Before} methods are executed and tears
 * it down after its {@code @After} methods. One member of the cluster is kept running for the duration of
 * the test, while other members are shutdown and replaced by new instances. For example, if the
 * BounceMemberRule is configured to setup a cluster of 5 members, one will stay up while the rest 4 will
 * be shutting down and replaced by new ones while the test tasks are executed. Use
 * {@link #getSteadyMember()} to obtain a reference to the member that is always up.<br>
 * <b>Defaults: </b>cluster size 6 members<br>
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
 * {@code MEMBER} as test driver.<br>
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
 * <p>
 * For examples on how to use this rule, see {@code BounceMemberRuleTest} and {@code QueryBounceTest}.
 * <p>
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

    public static final long STALENESS_DETECTOR_DISABLED = Long.MAX_VALUE;

    private static final ILogger LOGGER = Logger.getLogger(BounceMemberRule.class);

    private static final int DEFAULT_CLUSTER_SIZE = 6;
    private static final int DEFAULT_DRIVERS_COUNT = 5;
    // amount of time wait for test task futures to complete after test duration has passed
    private static final int TEST_TASK_TIMEOUT_SECONDS = 30;
    private static final int DEFAULT_BOUNCING_INTERVAL_SECONDS = 2;
    private static final long DEFAULT_MAXIMUM_STALE_SECONDS = STALENESS_DETECTOR_DISABLED;

    private final BounceTestConfiguration bounceTestConfig;
    private final AtomicBoolean testRunning = new AtomicBoolean();
    private final AtomicBoolean testFailed = new AtomicBoolean();
    private final AtomicReferenceArray<HazelcastInstance> members;
    private final AtomicReferenceArray<HazelcastInstance> testDrivers;
    private final int bouncingIntervalSeconds;
    private final ProgressMonitor progressMonitor;

    private volatile TestHazelcastInstanceFactory factory;

    private FutureTask<Runnable> bouncingMembersTask;
    private AtomicInteger driverCounter = new AtomicInteger();
    private ExecutorService taskExecutor;

    private BounceMemberRule(BounceTestConfiguration bounceTestConfig) {
        this.bounceTestConfig = bounceTestConfig;
        this.members = new AtomicReferenceArray<HazelcastInstance>(bounceTestConfig.getClusterSize());
        this.testDrivers = new AtomicReferenceArray<HazelcastInstance>(bounceTestConfig.getDriverCount());
        this.bouncingIntervalSeconds = bounceTestConfig.getBouncingIntervalSeconds();
        this.progressMonitor = new ProgressMonitor(bounceTestConfig.getMaximumStaleSeconds());
    }

    /**
     * @return cluster member that stays up for the duration of the test
     */
    public final HazelcastInstance getSteadyMember() {
        return members.get(0);
    }

    /**
     * @return a {@code HazelcastInstance} to drive a test; if several test drivers are configured, this method will
     * rotate and return the next one.
     */
    public HazelcastInstance getNextTestDriver() {
        return testDrivers.get(driverCounter.getAndIncrement() % testDrivers.length());
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
     * <p>
     * This method blocks until all test tasks have been completed or one of them throws an exception.
     */
    public void testRepeatedly(int concurrency, Runnable task, long durationSeconds) {
        assert concurrency > 0 : "Concurrency level should be greater than 0";

        TestTaskRunnable[] tasks = new TestTaskRunnable[concurrency];
        Arrays.fill(tasks, new TestTaskRunnable(task));
        testRepeatedly(tasks, durationSeconds);
    }

    public AtomicReferenceArray<HazelcastInstance> getMembers() {
        return members;
    }

    public AtomicReferenceArray<HazelcastInstance> getTestDrivers() {
        return testDrivers;
    }

    /**
     * Submit Runnables to be executed repeatedly until test is done.
     * The {@code task} is executed in a loop until either the configured test's duration is reached or an exception
     * is thrown from the {@code task}, in which case the test is marked as failed and the exception will be propagated
     * as test failure cause.
     * <p>
     * This method blocks until all test tasks have been completed or one of them throws an exception.
     */
    public void testRepeatedly(Runnable[] tasks, long durationSeconds) {
        assert tasks != null && tasks.length > 0 : "Some tasks must be submitted for execution";

        TestTaskRunnable[] runnables = new TestTaskRunnable[tasks.length];
        for (int i = 0; i < tasks.length; i++) {
            runnables[i] = new TestTaskRunnable(tasks[i]);
        }
        testWithDuration(runnables, durationSeconds);
    }

    /**
     * Submit Runnables to be executed concurrently. Each {@code task} is executed once. Exceptions thrown from a {@code task}
     * will mark the test as failed and the exception will be propagated as test failure cause.
     * <p>
     * This method blocks until all test tasks have been completed or one of them throws an exception.
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
            Runnable task = tasks[i];
            progressMonitor.registerTask(task);
            futures[i] = taskExecutor.submit(task);
        }

        // let the test run for test duration or until failed or finished, whichever comes first
        if (durationSeconds > 0) {
            long deadline = currentTimeMillis() + SECONDS.toMillis(durationSeconds);
            LOGGER.info("Executing test tasks with deadline " + timeToString(deadline));
            while (currentTimeMillis() < deadline) {
                if (testFailed.get()) {
                    break;
                }
                sleepSeconds(1);
                try {
                    progressMonitor.checkProgress();
                } catch (AssertionError e) {
                    testRunning.set(false);
                    throw e;
                }
            }
            if (currentTimeMillis() >= deadline) {
                LOGGER.info("Test deadline reached, tearing down");
            }
            testRunning.set(false);
            waitForFutures(futures);
        } else {
            LOGGER.info("Executing test tasks");
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

        BouncingClusterStatement(Statement statement) {
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
        for (int i = 0; i < bounceTestConfig.getClusterSize(); i++) {
            members.set(i, factory.newHazelcastInstance(memberConfig));
        }

        // setup drivers
        HazelcastInstance[] drivers = bounceTestConfig.getDriverFactory().createTestDrivers(this);
        assert drivers.length == bounceTestConfig.getDriverCount()
                : "Driver factory should return " + bounceTestConfig.getDriverCount() + " test drivers.";
        for (int i = 0; i < drivers.length; i++) {
            testDrivers.set(i, drivers[i]);
        }
        testRunning.set(true);
    }

    private void tearDown() {
        try {
            LOGGER.info("Tearing down BounceMemberRule");
            if (taskExecutor != null) {
                taskExecutor.shutdownNow();
                taskExecutor = null;
            }
            // shutdown test drivers first
            if (testDrivers != null) {
                for (int i = 0; i < testDrivers.length(); i++) {
                    HazelcastInstance hz = testDrivers.get(i);
                    hz.shutdown();
                }
            }
            if (factory != null) {
                factory.shutdownAll();
            }
        } catch (Throwable t) {
            // any exceptions thrown in tearDown are not interesting and may hide the actual failure, so are not rethrown
            LOGGER.warning("Error occurred while tearing down BounceMemberRule.", t);
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
                LOGGER.info("Spawning member bouncing thread");
                bouncingMembersTask = new FutureTask<Runnable>(new MemberUpDownMonkey(), null);
                Thread bounceMembersThread = new Thread(bouncingMembersTask);
                bounceMembersThread.setDaemon(true);
                bounceMembersThread.start();
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
                        LOGGER.info("Waiting for member bouncing thread to stop...");
                        bouncingMembersTask.get();
                        LOGGER.info("Member bouncing thread stopped.");
                    } catch (Exception e) {
                        // do not rethrow
                        LOGGER.warning("Member bouncing thread failed to stop.", e);
                    }
                }
            }
        };
    }

    // wait until all test tasks complete or one of them throws an exception
    private void waitForFutures(Future[] futures) {
        // do not wait more than 30 seconds
        long deadline = currentTimeMillis() + SECONDS.toMillis(TEST_TASK_TIMEOUT_SECONDS);
        LOGGER.info("Waiting until " + timeToString(deadline) + " for test tasks to complete gracefully.");
        List<Future> futuresToWaitFor = new ArrayList<Future>(Arrays.asList(futures));
        while (!futuresToWaitFor.isEmpty() && currentTimeMillis() < deadline) {
            Iterator<Future> iterator = futuresToWaitFor.iterator();
            while (iterator.hasNext()) {
                boolean hasTestFailed = testFailed.get();
                Future future = iterator.next();
                try {
                    // if the test failed, try to locate immediately the future that is done and will throw an exception
                    if ((hasTestFailed && future.isDone()) || !hasTestFailed) {
                        future.get(1, SECONDS);
                        iterator.remove();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    throw rethrow(e.getCause());
                } catch (TimeoutException e) {
                }
            }
        }
        if (!futuresToWaitFor.isEmpty()) {
            LOGGER.warning("Test tasks did not complete within " + TEST_TASK_TIMEOUT_SECONDS + " seconds, there are still "
                    + futuresToWaitFor.size() + " unfinished test tasks.");
        }
    }

    public static class Builder {

        private final Config memberConfig;

        private int clusterSize = DEFAULT_CLUSTER_SIZE;
        private int driversCount = DEFAULT_DRIVERS_COUNT;
        private DriverFactory driverFactory;
        private DriverType testDriverType;
        private boolean useTerminate;
        private int bouncingIntervalSeconds = DEFAULT_BOUNCING_INTERVAL_SECONDS;
        private long maximumStaleSeconds = DEFAULT_MAXIMUM_STALE_SECONDS;

        private Builder(Config memberConfig) {
            this.memberConfig = memberConfig;
        }

        public BounceMemberRule build() {

            if (testDriverType == null) {
                throw new AssertionError("testDriverType must be set");
            }

            if (testDriverType == ALWAYS_UP_MEMBER) {
                assert driversCount == 1
                        : "Driver count can only be 1 when driver type is ALWAYS_UP_MEMBER but found " + driversCount;
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
                    driversCount, driverFactory, useTerminate, bouncingIntervalSeconds, maximumStaleSeconds));
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

        public Builder bouncingIntervalSeconds(int bouncingIntervalSeconds) {
            this.bouncingIntervalSeconds = bouncingIntervalSeconds;
            return this;
        }

        public Builder maximumStalenessSeconds(int maximumStalenessSeconds) {
            this.maximumStaleSeconds = maximumStalenessSeconds;
            return this;
        }

        public Builder driverFactory(DriverFactory driverFactory) {
            this.driverFactory = driverFactory;
            return this;
        }

        public Builder useTerminate(boolean value) {
            this.useTerminate = value;
            return this;
        }

        // reflectively instantiate default client-side test driver factory
        private DriverFactory newDefaultClientDriverFactory() {
            try {
                Class factoryClass = ClassLoaderUtil.loadClass(null,
                        "com.hazelcast.client.test.bounce.MultiSocketClientDriverFactory");
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
        @Override
        public void run() {
            // rotate members 1..members.length(), member.get(0) is the steady member
            int divisor = members.length() - 1;
            int i = 1;
            int nextInstance;
            try {
                while (testRunning.get()) {
                    if (bounceTestConfig.isUseTerminate()) {
                        members.get(i).getLifecycleService().terminate();
                    } else {
                        members.get(i).shutdown();
                    }
                    nextInstance = i % divisor + 1;
                    sleepSecondsWhenRunning(bouncingIntervalSeconds);
                    if (!testRunning.get()) {
                        break;
                    }
                    members.set(i, factory.newHazelcastInstance(bounceTestConfig.getMemberConfig()));
                    sleepSecondsWhenRunning(bouncingIntervalSeconds);
                    // move to next member
                    i = nextInstance;
                }
            } catch (Throwable t) {
                LOGGER.warning("Error while bouncing members", t);
            }
            LOGGER.info("Member bouncing thread exiting");
        }
    }

    private void sleepSecondsWhenRunning(int seconds) {
        long deadLine = System.nanoTime() + SECONDS.toNanos(seconds);
        while (System.nanoTime() < deadLine && testRunning.get()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    /**
     * Wraps a {@code Runnable} to be executed repeatedly until testRunning becomes false or an exception is thrown
     * which sets the test as failed.
     */
    final class TestTaskRunnable implements Runnable {

        private final Runnable task;

        private volatile long lastIterationStartedTimestamp;
        private volatile Thread currentThread;
        private volatile long iterationCounter;
        private volatile long maxLatencyNanos;

        TestTaskRunnable(Runnable task) {
            this.task = task;
        }

        @Override
        public void run() {
            while (testRunning.get()) {
                try {
                    long startedNanos = System.nanoTime();
                    lastIterationStartedTimestamp = startedNanos;
                    currentThread = Thread.currentThread();
                    task.run();
                    iterationCounter++;
                    long latencyNanos = System.nanoTime() - startedNanos;
                    maxLatencyNanos = max(maxLatencyNanos, latencyNanos);
                } catch (Throwable t) {
                    testFailed.set(true);
                    throw rethrow(t);
                }
            }
        }

        public long getLastIterationStartedTimestamp() {
            return lastIterationStartedTimestamp;
        }

        public long getIterationCounter() {
            return iterationCounter;
        }

        public long getMaxLatencyNanos() {
            return maxLatencyNanos;
        }

        public Thread getCurrentThreadOrNull() {
            return currentThread;
        }


        @Override
        public String toString() {
            return "TestTaskRunnable{"
                    + "task=" + task
                    + '}';
        }
    }
}
