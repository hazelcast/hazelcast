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

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.TestUtil;
import org.junit.After;
import org.junit.ComparisonFailure;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
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

    public static String generateRandomString(int length) {
        StringBuffer sb = new StringBuffer(length);
        Random random = new Random();
        for (int k = 0; k < length; k++) {
            char c = (char) (random.nextInt(26) + 'a');
            sb.append(c);
        }
        return sb.toString();
    }

    public static void assertStartsWith(String expected, String actual) {
        if (actual != null && actual.startsWith(expected)) {
            return;
        } else if (expected instanceof String && actual instanceof String) {
            throw new ComparisonFailure("", (String) expected, (String) actual);
        } else {
            fail(format0("", expected, actual));
        }
    }

    public static void assertJoinable(Thread... threads) {
        assertJoinable(ASSERT_TRUE_EVENTUALLY_TIMEOUT, threads);
    }

    public static void interruptCurrentThread(final int delaysMs) {
        final Thread currentThread = Thread.currentThread();
        new Thread() {
            public void run() {
                sleepMillis(delaysMs);
                currentThread.interrupt();
            }
        }.start();
    }

    public static void assertIterableEquals(Iterable iter, Object... values) {
        int counter = 0;
        for (Object o : iter) {
            if (values.length < counter + 1) {
                throw new AssertionError("Iterator and values sizes are not equal");
            }
            assertEquals(values[counter], o);
            counter++;
        }

        assertEquals("Iterator and values sizes are not equal", values.length, counter);
    }

    public static void assertSizeEventually(int expectedSize, Collection c) {
        assertSizeEventually(expectedSize, c, ASSERT_TRUE_EVENTUALLY_TIMEOUT);
    }

    public static void assertSizeEventually(final int expectedSize, final Collection c, long timeoutSeconds) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals("the size of the collection is not correct", expectedSize, c.size());
            }
        }, timeoutSeconds);
    }

    public static void assertSizeEventually(int expectedSize, Map<?, ?> m) {
        assertSizeEventually(expectedSize, m, ASSERT_TRUE_EVENTUALLY_TIMEOUT);
    }

    public static void assertSizeEventually(final int expectedSize, final Map<?, ?> m, long timeoutSeconds) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals("the size of the map is not correct", expectedSize, m.size());
            }
        }, timeoutSeconds);
    }

    public static void assertClusterSizeEventually(final int expectedSize, final HazelcastInstance instance) {
        assertClusterSizeEventually(expectedSize, instance, ASSERT_TRUE_EVENTUALLY_TIMEOUT);
    }

    public static void assertClusterSizeEventually(final int expectedSize, final HazelcastInstance instance, long timeoutSeconds) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals("the size of the cluster is not correct", expectedSize, instance.getCluster().getMembers().size());
            }
        }, timeoutSeconds);
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
                if (remainingTimeoutMs <= 0) {
                    fail("Timeout waiting for thread " + t.getName() + " to terminate");
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void assertOpenEventually(CountDownLatch latch) {
        assertOpenEventually(latch, ASSERT_TRUE_EVENTUALLY_TIMEOUT);
    }

    public static void assertOpenEventually(String message, CountDownLatch latch) {
        assertOpenEventually(message, latch, ASSERT_TRUE_EVENTUALLY_TIMEOUT);
    }

    public static void assertOpenEventually(CountDownLatch latch, long timeoutSeconds) {
        assertOpenEventually(null, latch, timeoutSeconds);
    }

    public static void assertOpenEventually(String message, CountDownLatch latch, long timeoutSeconds) {
        try {
            boolean completed = latch.await(timeoutSeconds, TimeUnit.SECONDS);
            if (message == null) {
                assertTrue(format("CountDownLatch failed to complete within %d seconds , count left: %d", timeoutSeconds,
                        latch.getCount()), completed);
            } else {
                assertTrue(format("%s, failed to complete within %d seconds , count left: %d", message, timeoutSeconds,
                        latch.getCount()), completed);
            }
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

    public static String randomString() {
        return UUID.randomUUID().toString();
    }

    public static String randomMapName(String mapNamePrefix) {
        return mapNamePrefix + randomString();
    }

    public static String randomMapName() {
        return randomString();
    }

    public static void sleepMillis(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
        }
    }

    public static void assertTrueAllTheTime(AssertTask task, long durationSeconds) {
        for (int k = 0; k < durationSeconds; k++) {
            try {
                task.run();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            sleepSeconds(1);
        }
    }

    public static void assertTrueEventually(AssertTask task, long timeoutSeconds) {
        AssertionError error = null;

        //we are going to check 5 times a second.
        long iterations = timeoutSeconds * 5;
        int sleepMillis = 200;
        for (int k = 0; k < iterations; k++) {
            try {
                try {
                    task.run();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return;
            } catch (AssertionError e) {
                error = e;
            }
            sleepMillis(sleepMillis);
        }

        printAllStackTraces();
        throw error;
    }

    public static void assertTrueEventually(AssertTask task) {
        assertTrueEventually(task, ASSERT_TRUE_EVENTUALLY_TIMEOUT);
    }

    public static void assertIterableSize(int expected, Collection collection) {
        Iterator iterator = collection.iterator();
        int i = 0;
        while(iterator.hasNext()) {
            iterator.next();
            i++;
        }
        assertEquals(expected, i);
    }

    public static void assertTrueDelayed5sec(AssertTask task) {
        assertTrueDelayed(5, task);
    }

    public static void assertTrueDelayed(int delaySeconds, AssertTask task) {
        sleepSeconds(delaySeconds);
        try {
            task.run();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected final TestHazelcastInstanceFactory createHazelcastInstanceFactory(int nodeCount) {
        if (factory != null) {
            throw new IllegalStateException("Node factory is already created!");
        }
        return factory = new TestHazelcastInstanceFactory(nodeCount);
    }

    public HazelcastInstance createHazelcastInstance(Config config) {
        return createHazelcastInstanceFactory(1).newHazelcastInstance(config);
    }

    public HazelcastInstance createHazelcastInstance() {
        return createHazelcastInstance(new Config());
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

    public static void warmUpPartitions(HazelcastInstance... instances) {
        try {
            TestUtil.warmUpPartitions(instances);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static String generateKeyOwnedBy(HazelcastInstance instance) {
        final Member localMember = instance.getCluster().getLocalMember();
        final PartitionService partitionService = instance.getPartitionService();
        for (; ; ) {
            String id = UUID.randomUUID().toString();
            Partition partition = partitionService.getPartition(id);
            if (localMember.equals(partition.getOwner())) {
                return id;
            }
        }
    }

    public static String generateKeyNotOwnedBy(HazelcastInstance instance) {
        final Member localMember = instance.getCluster().getLocalMember();
        final PartitionService partitionService = instance.getPartitionService();
        for (; ; ) {
            String id = UUID.randomUUID().toString();
            Partition partition = partitionService.getPartition(id);
            if (!localMember.equals(partition.getOwner())) {
                return id;
            }
        }
    }

    public final class DummyUncheckedHazelcastTestException
            extends RuntimeException {

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

    private static String format0(String message, Object expected, Object actual) {
        String formatted = "";
        if (message != null && !message.equals("")) {
            formatted = message + " ";
        }
        String expectedString = String.valueOf(expected);
        String actualString = String.valueOf(actual);
        if (expectedString.equals(actualString)) {
            return formatted + "expected: " + formatClassAndValue(expected, expectedString) + " but was: " + formatClassAndValue(
                    actual, actualString);
        } else {
            return formatted + "expected:<" + expectedString + "> but was:<" + actualString + ">";
        }
    }

    private static String formatClassAndValue(Object value, String valueString) {
        String className = value == null ? "null" : value.getClass().getName();
        return className + "<" + valueString + ">";
    }


    public abstract class TestThread extends Thread {
        CountDownLatch startLatch;
        private volatile Throwable error;
        protected final Random random = new Random();
        private volatile boolean stopOnError = true;
        private volatile boolean stopTest = false;

        public TestThread() {
            startLatch = new CountDownLatch(1);
        }
        @Override
        public final void run() {
            try {
                startLatch.await();
                doRun();
            } catch (Throwable t) {
                if (stopOnError) {
                    stopTest = true;
                }
                t.printStackTrace();
                this.error = t;
            }
        }

        public abstract void doRun() throws Exception;

    }
}
