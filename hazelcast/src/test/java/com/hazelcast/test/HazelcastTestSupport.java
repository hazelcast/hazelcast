/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.test;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.PartitionService;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.TestUtil;
import org.junit.After;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class HazelcastTestSupport {

    private static final int ASSERT_TRUE_EVENTUALLY_TIMEOUT;

    static {
        System.setProperty("hazelcast.repmap.hooks.allowed", "true");
        ASSERT_TRUE_EVENTUALLY_TIMEOUT = Integer.parseInt(System.getProperty("hazelcast.assertTrueEventually.timeout", "120"));
        System.out.println("ASSERT_TRUE_EVENTUALLY_TIMEOUT = " + ASSERT_TRUE_EVENTUALLY_TIMEOUT);
    }

    private TestHazelcastInstanceFactory factory;

    public static void assertSizeEventually(int expectedSize, Collection c){
        assertSizeEventually(expectedSize, c, ASSERT_TRUE_EVENTUALLY_TIMEOUT);
    }

    public static void assertSizeEventually(final int expectedSize, final Collection c, long timeoutSeconds){
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
               assertEquals("the size of the collection is correct",expectedSize, c.size());
            }
        },timeoutSeconds);
    }

    public static void assertJoinable(Thread... threads) {
        assertJoinable(ASSERT_TRUE_EVENTUALLY_TIMEOUT, threads);
    }

    public static void assertJoinable(long timeoutSeconds, Thread... threads) {
        try {
            long remainingTimeoutMs = TimeUnit.SECONDS.toMillis(timeoutSeconds);
            for (Thread t : threads) {
                long startMs = System.currentTimeMillis();
                t.join(remainingTimeoutMs);

                if (t.isAlive()) {
                    fail("Timeout waiting for thread " + t.getName() + " to terminate");
                }

                long durationMs = System.currentTimeMillis() - startMs;
                remainingTimeoutMs -= durationMs;
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void assertOpenEventually(CountDownLatch latch) {
        assertOpenEventually(latch, ASSERT_TRUE_EVENTUALLY_TIMEOUT);
    }

    public static void assertOpenEventually(CountDownLatch latch, long timeoutSeconds) {
        try {
            boolean completed = latch.await(timeoutSeconds, TimeUnit.SECONDS);
            assertTrue("CountDownLatch failed to complete within " + timeoutSeconds + " seconds", completed);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void sleepSeconds(int seconds) {
        try {
            Thread.sleep(seconds * 1000);
        } catch (InterruptedException e) {
        }
    }

    public static void assertTrueAllTheTime(AssertTask task, long durationSeconds) {
        for (int k = 0; k < durationSeconds; k++) {
            task.run();
            sleepSeconds(1);
        }
    }

    public static void assertTrueEventually(AssertTask task, long timeoutSeconds) {
        AssertionError error = null;
        for (int k = 0; k < timeoutSeconds; k++) {
            try {
                task.run();
                return;
            } catch (AssertionError e) {
                error = e;
            }
            sleepSeconds(1);
        }

        printAllStackTraces();
        throw error;
    }

    public static void assertTrueEventually(AssertTask task) {
        assertTrueEventually(task, ASSERT_TRUE_EVENTUALLY_TIMEOUT);
    }

    public static void assertTrueDelayed5sec(AssertTask task) {
        assertTrueDelayed(5, task);
    }

    public static void assertTrueDelayed(int delaySeconds, AssertTask task) {
        sleepSeconds(delaySeconds);
        task.run();
    }

    protected final TestHazelcastInstanceFactory createHazelcastInstanceFactory(int nodeCount) {
        if (factory != null) {
            throw new IllegalStateException("Node factory is already created!");
        }
        return factory = new TestHazelcastInstanceFactory(nodeCount);
    }

    public HazelcastInstance createHazelcastInstance() {
        return createHazelcastInstanceFactory(1).newHazelcastInstance();
    }

    @After
    public final void shutdownNodeFactory() {
        final TestHazelcastInstanceFactory f = factory;
        if (f != null) {
            factory = null;
            f.shutdownAll();
        }
    }

    public static Node getNode(HazelcastInstance hz) {
        return TestUtil.getNode(hz);
    }

    protected static void warmUpPartitions(HazelcastInstance... instances) throws InterruptedException {
        TestUtil.warmUpPartitions(instances);
    }

    protected static String generateKeyOwnedBy(HazelcastInstance instance) throws InterruptedException {
        final Member localMember = instance.getCluster().getLocalMember();
        final PartitionService partitionService = instance.getPartitionService();
        int k = (int) (Math.random() * 1000);
        while (!localMember.equals(partitionService.getPartition(String.valueOf(k)).getOwner())) {
            k++;
            Thread.sleep(10);
        }
        return String.valueOf(k);
    }

    protected static String generateKeyNotOwnedBy(HazelcastInstance instance) throws InterruptedException {
        final Member localMember = instance.getCluster().getLocalMember();
        final PartitionService partitionService = instance.getPartitionService();
        int k = (int) (Math.random() * 1000);
        while (localMember.equals(partitionService.getPartition(String.valueOf(k)).getOwner())) {
            k++;
            Thread.sleep(10);
        }
        return String.valueOf(k);
    }

    public final class DummyUncheckedHazelcastTestException extends RuntimeException {

    }

    public static void printAllStackTraces() {
        Map liveThreads = Thread.getAllStackTraces();
        for (Object o : liveThreads.keySet()) {
            Thread key = (Thread) o;
            System.err.println("Thread " + key.getName());
            StackTraceElement[] trace = (StackTraceElement[]) liveThreads.get(key);
            for (StackTraceElement aTrace : trace) {
                System.err.println("\tat " + aTrace);
            }
        }
    }
}
