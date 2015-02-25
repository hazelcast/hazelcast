/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.impl.InternalOperationService;
import org.junit.After;
import org.junit.ComparisonFailure;

import java.util.Collection;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@SuppressWarnings("unused")
public abstract class HazelcastTestSupport {

    public static final int ASSERT_TRUE_EVENTUALLY_TIMEOUT;

    static {
        System.setProperty("hazelcast.repmap.hooks.allowed", "true");

        ASSERT_TRUE_EVENTUALLY_TIMEOUT = Integer.parseInt(System.getProperty("hazelcast.assertTrueEventually.timeout", "120"));
        System.out.println("ASSERT_TRUE_EVENTUALLY_TIMEOUT = " + ASSERT_TRUE_EVENTUALLY_TIMEOUT);
    }

    private TestHazelcastInstanceFactory factory;

    public static Future spawn(Runnable task) {
        FutureTask futureTask = new FutureTask(task, null);
        new Thread(futureTask).start();
        return futureTask;
    }


    public static <E> Future<E> spawn(Callable<E> task) {
        FutureTask futureTask = new FutureTask(task);
        new Thread(futureTask).start();
        return futureTask;
    }

    public static Address getAddress(HazelcastInstance hz) {
        Node node = getNode(hz);
        return node.clusterService.getThisAddress();
    }

    public static SerializationService getSerializationService(HazelcastInstance hz) {
        Node node = getNode(hz);
        return node.getSerializationService();
    }

    public static InternalOperationService getOperationService(HazelcastInstance hz) {
        Node node = getNode(hz);
        return (InternalOperationService) node.nodeEngine.getOperationService();
    }

    public static InternalPartitionService getPartitionService(HazelcastInstance hz) {
        Node node = getNode(hz);
        return node.partitionService;
    }

    @After
    public final void shutdownNodeFactory() {
        TestHazelcastInstanceFactory testHazelcastInstanceFactory = factory;
        if (testHazelcastInstanceFactory != null) {
            factory = null;
            testHazelcastInstanceFactory.terminateAll();
        }
    }

    public static void setLoggingNone() {
        System.setProperty("hazelcast.logging.type", "none");
    }

    public static void setLoggingLog4j() {
        System.setProperty("hazelcast.logging.type", "log4j");
    }

    public static void setLogLevelDebug() {
        if (isLog4jLoaded()) {
            org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.DEBUG);
        }
    }

    public static void setLogLevelInfo() {
        if (isLog4jLoaded()) {
            org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.INFO);
        }
    }

    public static void setLogLevelWarn() {
        if (isLog4jLoaded()) {
            org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.WARN);
        }
    }

    public static void setLogLevelError() {
        if (isLog4jLoaded()) {
            org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.ERROR);
        }
    }

    public static void setLogLevelFatal() {
        if (isLog4jLoaded()) {
            org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.FATAL);
        }
    }

    private static boolean isLog4jLoaded() {
        setLoggingLog4j();
        try {
            Class.forName("org.apache.log4j.Logger");
            Class.forName("org.apache.log4j.Level");
            return true;
        } catch (Throwable ignored) {
        }
        return false;
    }

    public static void sleepMillis(int millis) {
        try {
            TimeUnit.MILLISECONDS.sleep(millis);
        } catch (InterruptedException ignored) {
        }
    }

    public static void sleepSeconds(int seconds) {
        try {
            TimeUnit.SECONDS.sleep(seconds);
        } catch (InterruptedException ignored) {
        }
    }

    public static String generateRandomString(int length) {
        StringBuilder sb = new StringBuilder(length);
        Random random = new Random();
        for (int i = 0; i < length; i++) {
            char character = (char) (random.nextInt(26) + 'a');
            sb.append(character);
        }
        return sb.toString();
    }

    public static String randomString() {
        return UUID.randomUUID().toString();
    }

    public static String randomMapName() {
        return randomString();
    }

    public static String randomMapName(String mapNamePrefix) {
        return mapNamePrefix + randomString();
    }

    public static void printAllStackTraces() {
        Map liveThreads = Thread.getAllStackTraces();
        for (Object object : liveThreads.keySet()) {
            Thread key = (Thread) object;
            System.err.println("Thread " + key.getName());
            StackTraceElement[] trace = (StackTraceElement[]) liveThreads.get(key);
            for (StackTraceElement aTrace : trace) {
                System.err.println("\tat " + aTrace);
            }
        }
    }

    public static void interruptCurrentThread(final int delayMillis) {
        final Thread currentThread = Thread.currentThread();
        new Thread() {
            public void run() {
                sleepMillis(delayMillis);
                currentThread.interrupt();
            }
        }.start();
    }

    public HazelcastInstance createHazelcastInstance() {
        return createHazelcastInstance(new Config());
    }

    public HazelcastInstance createHazelcastInstance(Config config) {
        return createHazelcastInstanceFactory(1).newHazelcastInstance(config);
    }

    protected final TestHazelcastInstanceFactory createHazelcastInstanceFactory(int nodeCount) {
        if (factory != null) {
            throw new IllegalStateException("Node factory is already created!");
        }
        return factory = new TestHazelcastInstanceFactory(nodeCount);
    }

    protected final TestHazelcastInstanceFactory createHazelcastInstanceFactory(String... addresses) {
        if (factory != null) {
            throw new IllegalStateException("Node factory is already created!");
        }
        return factory = new TestHazelcastInstanceFactory(addresses);
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

     /**
     * Gets a partition id owned by this particular member.
      *
     * @param hz
     * @return
     */
    public static int getPartitionId(HazelcastInstance hz) {
        warmUpPartitions(hz);

        InternalPartitionService partitionService = getPartitionService(hz);
        for(InternalPartition p: partitionService.getPartitions() ){
            if(p.isLocal()){
                return p.getPartitionId();
            }
        }

        throw new RuntimeException("No local partitions are found for hz: "+hz.getName());
    }

    public static String generateKeyOwnedBy(HazelcastInstance instance) {
        return generateKeyInternal(instance, true);
    }

    public static String generateKeyNotOwnedBy(HazelcastInstance instance) {
        return generateKeyInternal(instance, false);
    }

    /**
     * Generates a key according to given reference instance by checking partition ownership for it.
     *
     * @param instance         reference instance for key generation.
     * @param generateOwnedKey <code>true</code> if we want a key which is owned by the given instance, otherwise
     *                         set to <code>false</code> which means generated key will not be owned by the given instance.
     * @return generated string.
     */
    private static String generateKeyInternal(HazelcastInstance instance, boolean generateOwnedKey) {
        Cluster cluster = instance.getCluster();
        checkMemberCount(generateOwnedKey, cluster);

        Member localMember = cluster.getLocalMember();
        PartitionService partitionService = instance.getPartitionService();
        for (; ; ) {
            String id = randomString();
            Partition partition = partitionService.getPartition(id);
            if (comparePartitionOwnership(generateOwnedKey, localMember, partition)) {
                return id;
            }
        }
    }

    private static void checkMemberCount(boolean generateOwnedKey, Cluster cluster) {
        if (generateOwnedKey) {
            return;
        }
        final Set<Member> members = cluster.getMembers();
        if (members.size() < 2) {
            throw new UnsupportedOperationException("Cluster has only one member, you can not generate a `not owned key`");
        }
    }

    private static boolean comparePartitionOwnership(boolean generateOwnedKey, Member member, Partition partition) {
        final Member owner = partition.getOwner();
        if (generateOwnedKey) {
            return member.equals(owner);
        } else {
            return !member.equals(owner);
        }
    }

    public static boolean isInstanceInSafeState(final HazelcastInstance instance) {
        final Node node = TestUtil.getNode(instance);
        if (node != null) {
            final InternalPartitionService ps = node.getPartitionService();
            return ps.isMemberStateSafe();
        } else {
            return true;
        }
    }

    public static void waitInstanceForSafeState(final HazelcastInstance instance) {
        assertTrueEventually(new AssertTask() {
            public void run() {
                isInstanceInSafeState(instance);
            }
        });
    }

    public static boolean isClusterInSafeState(final HazelcastInstance instance) {
        final PartitionService ps = instance.getPartitionService();
        return ps.isClusterSafe();
    }

    public static void waitClusterForSafeState(final HazelcastInstance instance) {
        assertTrueEventually(new AssertTask() {
            public void run() {
                assertTrue(isClusterInSafeState(instance));
            }
        });
    }

    public static boolean isAllInSafeState() {
        final Set<HazelcastInstance> nodeSet = HazelcastInstanceFactory.getAllHazelcastInstances();
        final HazelcastInstance[] nodes = nodeSet.toArray(new HazelcastInstance[nodeSet.size()]);
        return isAllInSafeState(nodes);
    }

    public static boolean isAllInSafeState(HazelcastInstance[] nodes) {
        for (HazelcastInstance node : nodes) {
            if (!isInstanceInSafeState(node)) {
                return false;
            }
        }
        return true;
    }

    public static void waitAllForSafeState() {
        assertTrueEventually(new AssertTask() {
            public void run() {
                assertTrue(isAllInSafeState());
            }
        });
    }

    public static void waitAllForSafeState(final HazelcastInstance... nodes) {
        assertTrueEventually(new AssertTask() {
            public void run() {
                assertTrue(isAllInSafeState(nodes));
            }
        });
    }

    public static void assertStartsWith(String expected, String actual) {
        if (actual != null && actual.startsWith(expected)) {
            return;
        }
        if (expected != null && actual != null) {
            throw new ComparisonFailure("", expected, actual);
        }
        fail(formatAssertMessage("", expected, null));
    }

    private static String formatAssertMessage(String message, Object expected, Object actual) {
        StringBuilder assertMessage = new StringBuilder();
        if (message != null && !message.isEmpty()) {
            assertMessage.append(message).append(" ");
        }
        String expectedString = String.valueOf(expected);
        String actualString = String.valueOf(actual);
        if (expectedString.equals(actualString)) {
            assertMessage.append("expected: ");
            formatClassAndValue(assertMessage, expected, expectedString);
            assertMessage.append(" but was: ");
            formatClassAndValue(assertMessage, actual, actualString);
        } else {
            assertMessage.append("expected: <").append(expectedString).append("> but was: <").append(actualString).append(">");
        }
        return assertMessage.toString();
    }

    private static void formatClassAndValue(StringBuilder message, Object value, String valueString) {
        message.append((value == null) ? "null" : value.getClass().getName()).append("<").append(valueString).append(">");
    }

    public static void assertJoinable(Thread... threads) {
        assertJoinable(ASSERT_TRUE_EVENTUALLY_TIMEOUT, threads);
    }

    public static void assertIterableEquals(Iterable iterable, Object... values) {
        int counter = 0;
        for (Object object : iterable) {
            if (values.length < counter + 1) {
                throw new AssertionError("Iterator and values sizes are not equal");
            }
            assertEquals(values[counter], object);
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
                assertEquals("the size of the collection is not correct: found-content:" + c, expectedSize, c.size());
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

    public static <E> void assertEqualsEventually(final FutureTask<E> task, final E value) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertTrue("FutureTask is not complete", task.isDone());
                assertEquals(value, task.get());
            }
        });
    }

    public static void assertClusterSize(int expectedSize, HazelcastInstance instance) {
        assertEquals("Cluster size is not correct", expectedSize, instance.getCluster().getMembers().size());
    }

    public static void assertClusterSizeEventually(int expectedSize, HazelcastInstance instance) {
        assertClusterSizeEventually(expectedSize, instance, ASSERT_TRUE_EVENTUALLY_TIMEOUT);
    }

    public static void assertClusterSizeEventually(final int expectedSize, final HazelcastInstance instance, long timeoutSeconds) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertEquals("the size of the cluster is not correct", expectedSize, instance.getCluster().getMembers().size());
            }
        }, timeoutSeconds);
    }

    public static void assertJoinable(long timeoutSeconds, Thread... threads) {
        try {
            long remainingTimeout = TimeUnit.SECONDS.toNanos(timeoutSeconds);
            for (Thread thread : threads) {
                long start = System.nanoTime();
                thread.join(remainingTimeout);

                if (thread.isAlive()) {
                    fail("Timeout waiting for thread " + thread.getName() + " to terminate");
                }

                long duration = System.nanoTime() - start;
                remainingTimeout -= duration;
                if (remainingTimeout <= 0) {
                    fail("Timeout waiting for thread " + thread.getName() + " to terminate");
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
        // we are going to check 5 times a second
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

    /**
     * This method executes the normal assertEquals with expected and actual values.
     * In addition it formats the given string with those values to provide a good assert message.
     *
     * @param message     assert message which is formatted with expected and actual values
     * @param expected    expected value which is used for assert
     * @param actual      actual value which is used for assert
     */
    public static void assertEqualsStringFormat(String message, Object expected, Object actual) {
        assertEquals(String.format(message, expected, actual), expected, actual);
    }

    public final class DummyUncheckedHazelcastTestException extends RuntimeException {
    }

    public static void assertExactlyOneSuccessfulRun(AssertTask task) {
        assertExactlyOneSuccessfulRun(task, ASSERT_TRUE_EVENTUALLY_TIMEOUT, TimeUnit.SECONDS);
    }

    public static void assertExactlyOneSuccessfulRun(AssertTask task, int giveUpTime, TimeUnit timeUnit) {
        long timeout = System.currentTimeMillis() + timeUnit.toMillis(giveUpTime);
        RuntimeException lastException = new RuntimeException("Did not try even once");
        while (System.currentTimeMillis() < timeout) {
            try {
                task.run();
                return;
            } catch (Exception e) {
                if (e instanceof RuntimeException) {
                    lastException = (RuntimeException) e;
                } else {
                    lastException = new RuntimeException(e);
                }
            }
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                lastException = new RuntimeException(e);
            }
        }
        throw lastException;
    }
}
