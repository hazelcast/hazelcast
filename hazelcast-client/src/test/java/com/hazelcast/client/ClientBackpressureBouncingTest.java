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

package com.hazelcast.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.ClientTestUtil;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientSmartInvocationServiceImpl;
import com.hazelcast.client.test.bounce.ClientDriverFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastOverloadException;
import com.hazelcast.core.IMap;
import com.hazelcast.internal.util.ThreadLocalRandomProvider;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.test.bounce.BounceMemberRule;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.client.spi.properties.ClientProperty.MAX_CONCURRENT_INVOCATIONS;
import static java.lang.Math.max;
import static java.lang.String.valueOf;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class ClientBackpressureBouncingTest extends HazelcastTestSupport {

    private static final int MAX_CONCURRENT_INVOCATION_CONFIG = 100;
    private static final int WORKER_THREAD_COUNT = 5;
    private static final long TEST_DURATION_SECONDS = 240; //4 minutes
    private static final long TEST_TIMEOUT_MILLIS = 10 * 60 * 1000; //10 minutes

    private InvocationCheckingThread checkingThread;

    @Rule
    public BounceMemberRule bounceMemberRule = BounceMemberRule.with(new Config()).driverFactory(new ClientDriverFactory() {
        @Override
        protected ClientConfig getClientConfig(HazelcastInstance member) {
            ClientConfig clientConfig = new ClientConfig()
                    .setProperty(MAX_CONCURRENT_INVOCATIONS.getName(), valueOf(MAX_CONCURRENT_INVOCATION_CONFIG));
            clientConfig.getNetworkConfig().setRedoOperation(true);
            return clientConfig;
        }
    }).build();

    @After
    public void tearDown() throws InterruptedException {
        //just in case the thread was not stopped in the regular method
        //it could be the testRepeatedly thrown an exception
        checkingThread.join();
    }

    @Test(timeout = TEST_TIMEOUT_MILLIS)
    public void testInFlightInvocationCountIsNotGrowing() throws Exception {
        HazelcastInstance driver = bounceMemberRule.getNextTestDriver();
        final IMap<Integer, Integer> map = driver.getMap(randomMapName());
        startInvocationCheckingThread(driver);
        Runnable[] tasks = createTasks(map);
        bounceMemberRule.testRepeatedly(tasks, TEST_DURATION_SECONDS);
        System.out.println("Finished bouncing");
        checkingThread.assertInFlightInvocationsWereNotGrowing();
    }

    private void startInvocationCheckingThread(HazelcastInstance driver) throws Exception {
        checkingThread = new InvocationCheckingThread(driver);
        checkingThread.start();
    }

    private Runnable[] createTasks(final IMap<Integer, Integer> map) {
        Runnable[] tasks = new Runnable[WORKER_THREAD_COUNT];
        for (int i = 0; i < WORKER_THREAD_COUNT; i++) {
            final int workerNo = i;
            tasks[i] = new MyRunnable(map, workerNo);
        }
        return tasks;
    }

    private static class InvocationCheckingThread extends Thread {
        private final long deadLine;
        private final long warmUpDeadline;
        private final ConcurrentMap<Long, ClientInvocation> callIdMap;
        private int maxInvocationCountObserved;
        private int maxInvocationCountObservedDuringWarmup;

        private InvocationCheckingThread(HazelcastInstance client) throws Exception {
            long durationMillis = TEST_DURATION_SECONDS * 1000;
            this.warmUpDeadline = System.currentTimeMillis() + (durationMillis / 5);
            this.deadLine = System.currentTimeMillis() + durationMillis;
            this.callIdMap = extraCallIdMap(client);
        }

        @Override
        public void run() {
            while (System.currentTimeMillis() < deadLine) {
                int currentSize = callIdMap.size();
                maxInvocationCountObserved = max(currentSize, maxInvocationCountObserved);
                if (System.currentTimeMillis() < warmUpDeadline) {
                    maxInvocationCountObservedDuringWarmup = max(currentSize, maxInvocationCountObservedDuringWarmup);
                }
                sleepAtLeastMillis(100);
            }
        }

        private void assertInFlightInvocationsWereNotGrowing() throws InterruptedException {
            join();
            //make sure we are observing something
            assertTrue(maxInvocationCountObserved > 0);

            long maximumTolerableInvocationCount = (long) (maxInvocationCountObservedDuringWarmup * 1.2);
            assertTrue("Apparently number of in-flight invocations is growing. "
                    + "Max. number of in-flight invocation during first fifth of test duration: "
                    + maxInvocationCountObservedDuringWarmup
                    + " Max. number of in-flight invocation in total: "
                    + maxInvocationCountObserved, maxInvocationCountObserved <= maximumTolerableInvocationCount);
        }

        private ConcurrentMap<Long, ClientInvocation> extraCallIdMap(HazelcastInstance client) throws NoSuchFieldException, IllegalAccessException {
            HazelcastClientInstanceImpl clientImpl = ClientTestUtil.getHazelcastClientInstanceImpl(client);
            ClientSmartInvocationServiceImpl invocationService = (ClientSmartInvocationServiceImpl) clientImpl.getInvocationService();
            Field callIdMapField = ClientSmartInvocationServiceImpl.class.getSuperclass().getDeclaredField("callIdMap");
            callIdMapField.setAccessible(true);
            return (ConcurrentMap<Long, ClientInvocation>) callIdMapField.get(invocationService);
        }
    }


    private static class MyRunnable implements Runnable {
        private final IMap<Integer, Integer> map;
        private final AtomicLong progressCounter = new AtomicLong();
        private final AtomicLong failureCounter = new AtomicLong();
        private final AtomicLong backpressureCounter = new AtomicLong();
        private final ExecutionCallback<Integer> callback = new CountingCallback();
        private final int workerNo;

        public MyRunnable(IMap<Integer, Integer> map, int workerNo) {
            this.map = map;
            this.workerNo = workerNo;
        }

        @Override
        public void run() {
            try {
                int key = ThreadLocalRandomProvider.get().nextInt();
                map.getAsync(key).andThen(callback);
            } catch (HazelcastOverloadException e) {
                long current = backpressureCounter.incrementAndGet();
                if (current % 250000 == 0) {
                    System.out.println("Worker no. " + workerNo + " backpressured. counter: " + current);
                }
            }
        }

        private class CountingCallback implements ExecutionCallback<Integer> {
            @Override
            public void onResponse(Integer response) {
                long position = progressCounter.incrementAndGet();
                if (position % 10000 == 0) {
                    System.out.println("Worker no. " + workerNo + " at " + position);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                long position = failureCounter.incrementAndGet();
                if (position % 100 == 0) {
                    System.out.println("Failure Worker no. " + workerNo + " at " + position);
                }
            }
        }
    }
}