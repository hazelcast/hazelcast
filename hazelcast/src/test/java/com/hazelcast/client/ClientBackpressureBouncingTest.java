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

package com.hazelcast.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.spi.ClientInvocationService;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.client.impl.spi.impl.ClientInvocationServiceImpl;
import com.hazelcast.client.test.bounce.MultiSocketClientDriverFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastOverloadException;
import com.hazelcast.internal.util.ThreadLocalRandomProvider;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.test.bounce.BounceMemberRule;
import com.hazelcast.test.bounce.BounceTestConfiguration;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

import static com.hazelcast.client.impl.clientside.ClientTestUtil.getHazelcastClientInstanceImpl;
import static com.hazelcast.client.properties.ClientProperty.BACKPRESSURE_BACKOFF_TIMEOUT_MILLIS;
import static com.hazelcast.client.properties.ClientProperty.MAX_CONCURRENT_INVOCATIONS;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static java.lang.Math.max;
import static java.lang.String.format;
import static java.lang.String.valueOf;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category(SlowTest.class)
public class ClientBackpressureBouncingTest extends HazelcastTestSupport {

    private static final long TEST_DURATION_SECONDS = 240; // 4 minutes
    private static final long TEST_TIMEOUT_MILLIS = 10 * 60 * 1000; // 10 minutes

    private static final int MAX_CONCURRENT_INVOCATION_CONFIG = 100;
    private static final int WORKER_THREAD_COUNT = 5;

    @Parameters(name = "backoffTimeoutMillis:{0}")
    public static Iterable<Object[]> parameters() {
        return asList(new Object[][]{
                {-1},
                {60000},
        });
    }

    @Rule
    public BounceMemberRule bounceMemberRule;

    private InvocationCheckingThread checkingThread;

    private long backoff;

    public ClientBackpressureBouncingTest(int backoffTimeoutMillis) {
        this.backoff = backoffTimeoutMillis;
        this.bounceMemberRule = BounceMemberRule
                .with(new Config())
                .driverType(BounceTestConfiguration.DriverType.CLIENT)
                .driverFactory(new MultiSocketClientDriverFactory(
                        new ClientConfig()
                                .setProperty(MAX_CONCURRENT_INVOCATIONS.getName(), valueOf(MAX_CONCURRENT_INVOCATION_CONFIG))
                                .setProperty(BACKPRESSURE_BACKOFF_TIMEOUT_MILLIS.getName(), valueOf(backoff))
                )).build();
    }

    @After
    public void tearDown() {
        if (checkingThread != null) {
            checkingThread.shutdown();
        }
    }

    @Test(timeout = TEST_TIMEOUT_MILLIS)
    public void testInFlightInvocationCountIsNotGrowing() {
        HazelcastInstance driver = bounceMemberRule.getNextTestDriver();
        IMap<Integer, Integer> map = driver.getMap(randomMapName());
        Runnable[] tasks = createTasks(map);

        checkingThread = new InvocationCheckingThread(driver);
        checkingThread.start();

        bounceMemberRule.testRepeatedly(tasks, TEST_DURATION_SECONDS);
        System.out.println("Finished bouncing");

        checkingThread.shutdown();
        checkingThread.assertInFlightInvocationsWereNotGrowing();
    }

    private Runnable[] createTasks(final IMap<Integer, Integer> map) {
        Runnable[] tasks = new Runnable[WORKER_THREAD_COUNT];
        for (int i = 0; i < WORKER_THREAD_COUNT; i++) {
            tasks[i] = new MyRunnable(map, i);
        }
        return tasks;
    }

    private static class InvocationCheckingThread extends Thread {

        private final long warmUpDeadline;
        private final long deadLine;
        private final ConcurrentMap<Long, ClientInvocation> invocations;

        private int maxInvocationCountObserved;
        private int maxInvocationCountObservedDuringWarmup;

        private volatile boolean running = true;

        private InvocationCheckingThread(HazelcastInstance client) {
            long durationMillis = TimeUnit.SECONDS.toMillis(TEST_DURATION_SECONDS);
            long now = System.currentTimeMillis();

            this.warmUpDeadline = now + (durationMillis / 5);
            this.deadLine = now + durationMillis;
            this.invocations = extractInvocations(client);
        }

        @Override
        public void run() {
            while (System.currentTimeMillis() < deadLine && running) {
                int currentSize = invocations.size();
                maxInvocationCountObserved = max(currentSize, maxInvocationCountObserved);
                if (System.currentTimeMillis() < warmUpDeadline) {
                    maxInvocationCountObservedDuringWarmup = max(currentSize, maxInvocationCountObservedDuringWarmup);
                }
                sleepAtLeastMillis(100);
            }
        }

        private void shutdown() {
            running = false;
            interrupt();
            assertJoinable(this);
        }

        private void assertInFlightInvocationsWereNotGrowing() {
            assertTrue("There are no invocations to be observed!", maxInvocationCountObserved > 0);

            long maximumTolerableInvocationCount = (long) (maxInvocationCountObservedDuringWarmup * 2);
            assertTrue("Apparently number of in-flight invocations is growing."
                    + " Max. number of in-flight invocation during first fifth of test duration: "
                    + maxInvocationCountObservedDuringWarmup
                    + " Max. number of in-flight invocation in total: "
                    + maxInvocationCountObserved, maxInvocationCountObserved <= maximumTolerableInvocationCount);
        }

        @SuppressWarnings("unchecked")
        private ConcurrentMap<Long, ClientInvocation> extractInvocations(HazelcastInstance client) {
            try {
                HazelcastClientInstanceImpl clientImpl = getHazelcastClientInstanceImpl(client);
                ClientInvocationService invocationService = clientImpl.getInvocationService();
                Field invocationsField = ClientInvocationServiceImpl.class.getDeclaredField("invocations");
                invocationsField.setAccessible(true);
                return (ConcurrentMap<Long, ClientInvocation>) invocationsField.get(invocationService);
            } catch (Exception e) {
                throw rethrow(e);
            }
        }
    }

    private class MyRunnable implements Runnable {

        private final BiConsumer<Integer, Throwable> callback = new CountingCallback();
        private final AtomicLong backpressureCounter = new AtomicLong();
        private final AtomicLong progressCounter = new AtomicLong();
        private final AtomicLong failureCounter = new AtomicLong();

        private final IMap<Integer, Integer> map;
        private final int workerNo;

        MyRunnable(IMap<Integer, Integer> map, int workerNo) {
            this.map = map;
            this.workerNo = workerNo;
        }

        @Override
        public void run() {
            try {
                int key = ThreadLocalRandomProvider.get().nextInt();
                map.getAsync(key).toCompletableFuture().whenCompleteAsync(callback);
            } catch (HazelcastOverloadException e) {
                if (backoff != -1) {
                    fail(format("HazelcastOverloadException should not be thrown when backoff is configured (%d ms), but got: %s",
                            backoff, e));
                }
                long current = backpressureCounter.incrementAndGet();
                if (current % 250000 == 0) {
                    System.out.println("Worker no. " + workerNo + " backpressured. counter: " + current);
                }
            }
        }

        private class CountingCallback implements BiConsumer<Integer, Throwable> {

            @Override
            public void accept(Integer integer, Throwable throwable) {
                if (throwable == null) {
                    long position = progressCounter.incrementAndGet();
                    if (position % 50000 == 0) {
                        System.out.println("Worker no. " + workerNo + " at " + position);
                    }
                } else {
                    long position = failureCounter.incrementAndGet();
                    if (position % 100 == 0) {
                        System.out.println("Failure Worker no. " + workerNo + " at " + position);
                    }
                }
            }
        }
    }
}
