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

package com.hazelcast.test;

import classloading.ThreadLocalLeakTestUtils;
import com.hazelcast.client.impl.ClientEngineImpl;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeExtension;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.impl.PartitionServiceState;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.impl.operation.DefaultMapOperationProvider;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationparker.impl.OperationParkerImpl;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.partition.IPartition;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.test.jitter.JitterRule;
import org.junit.After;
import org.junit.ComparisonFailure;
import org.junit.Rule;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.test.TestPartitionUtils.getPartitionServiceState;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static java.lang.Integer.getInteger;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@SuppressWarnings({"unused", "SameParameterValue", "WeakerAccess"})
public abstract class HazelcastTestSupport {

    public static final int ASSERT_TRUE_EVENTUALLY_TIMEOUT;

    @Rule
    public JitterRule jitterRule = new JitterRule();

    static {
        ASSERT_TRUE_EVENTUALLY_TIMEOUT = getInteger("hazelcast.assertTrueEventually.timeout", 120);
        System.out.println("ASSERT_TRUE_EVENTUALLY_TIMEOUT = " + ASSERT_TRUE_EVENTUALLY_TIMEOUT);
    }

    private TestHazelcastInstanceFactory factory;

    @After
    public final void shutdownNodeFactory() {
        TestHazelcastInstanceFactory testHazelcastInstanceFactory = factory;
        if (testHazelcastInstanceFactory != null) {
            factory = null;
            testHazelcastInstanceFactory.terminateAll();
        }
    }

    // ###################################
    // ########## configuration ##########
    // ###################################

    protected Config getConfig() {
        return new Config();
    }

    // ###############################################
    // ########## HazelcastInstance factory ##########
    // ###############################################

    protected HazelcastInstance createHazelcastInstance() {
        return createHazelcastInstance(getConfig());
    }

    protected HazelcastInstance createHazelcastInstance(Config config) {
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

    protected final TestHazelcastInstanceFactory createHazelcastInstanceFactory() {
        if (factory != null) {
            throw new IllegalStateException("Node factory is already created!");
        }
        return factory = new TestHazelcastInstanceFactory();
    }

    protected final TestHazelcastInstanceFactory createHazelcastInstanceFactory(int initialPort, String... addresses) {
        if (factory != null) {
            throw new IllegalStateException("Node factory is already created!");
        }
        return factory = new TestHazelcastInstanceFactory(initialPort, addresses);
    }

    // ###########################################
    // ########## implementation getter ##########
    // ###########################################

    public static Node getNode(HazelcastInstance hz) {
        return TestUtil.getNode(hz);
    }

    public static NodeEngineImpl getNodeEngineImpl(HazelcastInstance hz) {
        return getNode(hz).nodeEngine;
    }

    public static ClientEngineImpl getClientEngineImpl(HazelcastInstance instance) {
        return getNode(instance).clientEngine;
    }

    public static ConnectionManager getConnectionManager(HazelcastInstance hz) {
        return getNode(hz).connectionManager;
    }

    public static ClusterService getClusterService(HazelcastInstance hz) {
        return getNode(hz).clusterService;
    }

    public static InternalPartitionService getPartitionService(HazelcastInstance hz) {
        return getNode(hz).partitionService;
    }

    public static InternalSerializationService getSerializationService(HazelcastInstance hz) {
        return getNode(hz).getSerializationService();
    }

    public static InternalOperationService getOperationService(HazelcastInstance hz) {
        return getNodeEngineImpl(hz).getOperationService();
    }

    public static OperationServiceImpl getOperationServiceImpl(HazelcastInstance hz) {
        return (OperationServiceImpl) getNodeEngineImpl(hz).getOperationService();
    }

    public static MetricsRegistry getMetricsRegistry(HazelcastInstance hz) {
        return getNodeEngineImpl(hz).getMetricsRegistry();
    }

    public static Address getAddress(HazelcastInstance hz) {
        return getClusterService(hz).getThisAddress();
    }

    public static Packet toPacket(HazelcastInstance local, HazelcastInstance remote, Operation operation) {
        InternalSerializationService serializationService = getSerializationService(local);
        ConnectionManager connectionManager = getConnectionManager(local);

        return new Packet(serializationService.toBytes(operation), operation.getPartitionId())
                .setPacketType(Packet.Type.OPERATION)
                .setConn(connectionManager.getConnection(getAddress(remote)));
    }

    // #####################################
    // ########## generic utility ##########
    // #####################################

    protected void checkThreadLocalsForLeaks() throws Exception {
        ThreadLocalLeakTestUtils.checkThreadLocalsForLeaks(getClass().getClassLoader());
    }

    public static void ignore(Throwable ignored) {
    }

    public static Future spawn(Runnable task) {
        FutureTask<Runnable> futureTask = new FutureTask<Runnable>(task, null);
        new Thread(futureTask).start();
        return futureTask;
    }

    public static <E> Future<E> spawn(Callable<E> task) {
        FutureTask<E> futureTask = new FutureTask<E>(task);
        new Thread(futureTask).start();
        return futureTask;
    }

    public static void interruptCurrentThread(final int delayMillis) {
        final Thread currentThread = Thread.currentThread();
        new Thread(new Runnable() {
            public void run() {
                sleepMillis(delayMillis);
                currentThread.interrupt();
            }
        }).start();
    }

    public static void printAllStackTraces() {
        StringBuilder sb = new StringBuilder();
        Map liveThreads = Thread.getAllStackTraces();
        for (Object object : liveThreads.keySet()) {
            Thread key = (Thread) object;
            sb.append("Thread ").append(key.getName());
            StackTraceElement[] trace = (StackTraceElement[]) liveThreads.get(key);
            for (StackTraceElement aTrace : trace) {
                sb.append("\tat ").append(aTrace);
            }
        }
        System.err.println(sb.toString());
    }

    // ###########################
    // ########## sleep ##########
    // ###########################

    public static void sleepMillis(int millis) {
        try {
            MILLISECONDS.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static void sleepSeconds(int seconds) {
        try {
            TimeUnit.SECONDS.sleep(seconds);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Sleeps for the given amount of time and after that, sets stop to true.
     *
     * If stop is changed to true while sleeping, the calls returns before waiting the full sleeping period.
     *
     * This method is very useful for stress tests that run for a certain amount of time. But if one of the stress tests
     * runs into a failure, the test should be aborted immediately. This is done by letting the thread set stop to true.
     *
     * @param stop            an {@link AtomicBoolean} to stop the sleep method
     * @param durationSeconds sleep duration in seconds
     */
    public static void sleepAndStop(AtomicBoolean stop, long durationSeconds) {
        for (int i = 0; i < durationSeconds; i++) {
            if (stop.get()) {
                return;
            }
            sleepSeconds(1);
        }
        stop.set(true);
    }

    public static void sleepAtLeastMillis(long sleepFor) {
        boolean interrupted = false;
        try {
            long remainingNanos = MILLISECONDS.toNanos(sleepFor);
            long sleepUntil = System.nanoTime() + remainingNanos;
            while (remainingNanos > 0) {
                try {
                    NANOSECONDS.sleep(remainingNanos);
                } catch (InterruptedException e) {
                    interrupted = true;
                } finally {
                    remainingNanos = sleepUntil - System.nanoTime();
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public static void sleepAtLeastSeconds(long seconds) {
        sleepAtLeastMillis(seconds * 1000);
    }

    // #######################################
    // ########## random generators ##########
    // #######################################

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
        return randomUUID().toString();
    }

    public static String randomMapName() {
        return randomString();
    }

    public static String randomMapName(String namePrefix) {
        return namePrefix + randomString();
    }

    public static String randomName() {
        return randomString();
    }

    // #########################################################
    // ########## HazelcastInstance random generators ##########
    // #########################################################

    public static String randomNameOwnedBy(HazelcastInstance hz) {
        return randomNameOwnedBy(hz, "");
    }

    public static String randomNameOwnedBy(HazelcastInstance instance, String prefix) {
        Member localMember = instance.getCluster().getLocalMember();
        PartitionService partitionService = instance.getPartitionService();
        while (true) {
            String id = prefix + randomString();
            Partition partition = partitionService.getPartition(id);
            if (comparePartitionOwnership(true, localMember, partition)) {
                return id;
            }
        }
    }

    public static Partition randomPartitionOwnedBy(HazelcastInstance hz) {
        List<Partition> partitions = new LinkedList<Partition>();
        for (Partition partition : hz.getPartitionService().getPartitions()) {
            if (partition.getOwner().localMember()) {
                partitions.add(partition);
            }
        }
        if (partitions.isEmpty()) {
            throw new IllegalStateException("No partitions found for HazelcastInstance:" + hz.getName());
        }
        return partitions.get((int) (Math.random() * partitions.size()));
    }

    public static String generateKeyOwnedBy(HazelcastInstance instance) {
        return generateKeyOwnedBy(instance, true);
    }

    public static String generateKeyNotOwnedBy(HazelcastInstance instance) {
        return generateKeyOwnedBy(instance, false);
    }

    /**
     * Generates a key according to given reference instance by checking partition ownership for it.
     *
     * @param instance         reference instance for key generation.
     * @param generateOwnedKey {@code true} if we want a key which is owned by the given instance, otherwise
     *                         set to {@code false} which means generated key will not be owned by the given instance.
     * @return generated string.
     */
    public static String generateKeyOwnedBy(HazelcastInstance instance, boolean generateOwnedKey) {
        Cluster cluster = instance.getCluster();
        checkMemberCount(generateOwnedKey, cluster);
        checkPartitionCountGreaterOrEqualMemberCount(instance);

        Member localMember = cluster.getLocalMember();
        PartitionService partitionService = instance.getPartitionService();
        while (true) {
            String id = randomString();
            Partition partition = partitionService.getPartition(id);
            if (comparePartitionOwnership(generateOwnedKey, localMember, partition)) {
                return id;
            }
        }
    }

    public static String generateKeyForPartition(HazelcastInstance instance, int partitionId) {
        Cluster cluster = instance.getCluster();
        checkPartitionCountGreaterOrEqualMemberCount(instance);

        Member localMember = cluster.getLocalMember();
        PartitionService partitionService = instance.getPartitionService();
        while (true) {
            String id = randomString();
            Partition partition = partitionService.getPartition(id);
            if (partition.getPartitionId() == partitionId) {
                return id;
            }
        }
    }

    public String[] generateKeysBelongingToSamePartitionsOwnedBy(HazelcastInstance instance, int keyCount) {
        int partitionId = getPartitionId(instance);
        String[] keys = new String[keyCount];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = generateKeyForPartition(instance, partitionId);
        }
        return keys;
    }

    private static boolean comparePartitionOwnership(boolean ownedBy, Member member, Partition partition) {
        Member owner = partition.getOwner();
        if (ownedBy) {
            return member.equals(owner);
        } else {
            return !member.equals(owner);
        }
    }

    private static void checkPartitionCountGreaterOrEqualMemberCount(HazelcastInstance instance) {
        Cluster cluster = instance.getCluster();
        int memberCount = cluster.getMembers().size();

        InternalPartitionService internalPartitionService = getPartitionService(instance);
        int partitionCount = internalPartitionService.getPartitionCount();

        if (partitionCount < memberCount) {
            throw new UnsupportedOperationException("Partition count should be equal or greater than member count!");
        }
    }

    // ##################################
    // ########## partition id ##########
    // ##################################

    /**
     * Gets a partition id owned by this particular member.
     */
    public static int getPartitionId(HazelcastInstance hz) {
        warmUpPartitions(hz);

        InternalPartitionService partitionService = getPartitionService(hz);
        for (IPartition partition : partitionService.getPartitions()) {
            if (partition.isLocal()) {
                return partition.getPartitionId();
            }
        }
        throw new RuntimeException("No local partitions are found for hz: " + hz.getName());
    }

    public static int getPartitionId(HazelcastInstance hz, String partitionName) {
        PartitionService partitionService = hz.getPartitionService();
        Partition partition = partitionService.getPartition(partitionName);
        return partition.getPartitionId();
    }

    // ################################################
    // ########## cluster and instance state ##########
    // ################################################

    public static void closeConnectionBetween(HazelcastInstance h1, HazelcastInstance h2) {
        if (h1 == null || h2 == null) {
            return;
        }
        Node n1 = TestUtil.getNode(h1);
        Node n2 = TestUtil.getNode(h2);
        if (n1 != null && n2 != null) {
            n1.clusterService.suspectMember(n2.getLocalMember(), null, true);
            n2.clusterService.suspectMember(n1.getLocalMember(), null, true);
        }
    }

    private static void checkMemberCount(boolean generateOwnedKey, Cluster cluster) {
        if (generateOwnedKey) {
            return;
        }
        Set<Member> members = cluster.getMembers();
        if (members.size() < 2) {
            throw new UnsupportedOperationException("Cluster has only one member, you can not generate a `not owned key`");
        }
    }

    public static void warmUpPartitions(HazelcastInstance... instances) {
        try {
            TestUtil.warmUpPartitions(instances);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void warmUpPartitions(Collection<HazelcastInstance> instances) {
        try {
            TestUtil.warmUpPartitions(instances);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean isInstanceInSafeState(HazelcastInstance instance) {
        Node node = TestUtil.getNode(instance);
        if (node == null) {
            return true;
        }
        InternalPartitionService ps = node.getPartitionService();
        return ps.isMemberStateSafe();
    }

    public static boolean isClusterInSafeState(HazelcastInstance instance) {
        PartitionService ps = instance.getPartitionService();
        return ps.isClusterSafe();
    }

    public static boolean isAllInSafeState() {
        Set<HazelcastInstance> nodeSet = HazelcastInstanceFactory.getAllHazelcastInstances();
        return isAllInSafeState(nodeSet);
    }

    public static boolean isAllInSafeState(Collection<HazelcastInstance> nodes) {
        for (HazelcastInstance node : nodes) {
            if (!isInstanceInSafeState(node)) {
                return false;
            }
        }
        return true;
    }

    public static void waitInstanceForSafeState(final HazelcastInstance instance) {
        assertTrueEventually(new AssertTask() {
            public void run() {
                isInstanceInSafeState(instance);
            }
        });
    }

    public static void waitClusterForSafeState(final HazelcastInstance instance) {
        assertTrueEventually(new AssertTask() {
            public void run() {
                assertTrue(isClusterInSafeState(instance));
            }
        });
    }

    public static void waitUntilClusterState(HazelcastInstance hz, ClusterState state, int timeoutSeconds) {
        int waited = 0;
        while (!hz.getCluster().getClusterState().equals(state)) {
            if (waited++ == timeoutSeconds) {
                break;
            }
            sleepSeconds(1);
        }
    }

    public static void waitAllForSafeState(Collection<HazelcastInstance> instances) {
        waitAllForSafeState(instances, ASSERT_TRUE_EVENTUALLY_TIMEOUT);
    }

    public static void waitAllForSafeState(final Collection<HazelcastInstance> instances, int timeoutInSeconds) {
        assertTrueEventually(new AssertTask() {
            public void run() {
                assertAllInSafeState(instances);
            }
        }, timeoutInSeconds);
    }

    public static void waitAllForSafeState(final HazelcastInstance[] nodes, int timeoutInSeconds) {
        assertTrueEventually(new AssertTask() {
            public void run() {
                assertAllInSafeState(asList(nodes));
            }
        }, timeoutInSeconds);
    }

    public static void waitAllForSafeState(HazelcastInstance... nodes) {
        if (nodes.length == 0) {
            throw new IllegalArgumentException("waitAllForSafeState(HazelcastInstance... nodes) cannot be called with"
                    + " an empty array. It's too easy to mistake it for the old argument-less waitAllForSafeState()."
                    + " The old version was removed as it was implemented via Hazelcast.getAllHazelcastInstances()"
                    + " which uses a static map internally and it's causing issues when tests are running in parallel.");
        }
        waitAllForSafeState(asList(nodes));
    }

    public static void assertAllInSafeState(Collection<HazelcastInstance> nodes) {
        Map<Address, PartitionServiceState> nonSafeStates = new HashMap<Address, PartitionServiceState>();
        for (HazelcastInstance node : nodes) {
            final PartitionServiceState state = getPartitionServiceState(node);
            if (state != PartitionServiceState.SAFE) {
                nonSafeStates.put(getAddress(node), state);
            }
        }

        assertTrue("Instances not in safe state! " + nonSafeStates, nonSafeStates.isEmpty());
    }

    public static void assertNodeStarted(HazelcastInstance instance) {
        NodeExtension nodeExtension = getNode(instance).getNodeExtension();
        assertTrue(nodeExtension.isStartCompleted());
    }

    public static void assertNodeStartedEventually(final HazelcastInstance instance) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertNodeStarted(instance);
            }
        });
    }

    // ################################
    // ########## assertions ##########
    // ################################

    public static void assertUtilityConstructor(Class clazz) {
        Constructor[] constructors = clazz.getDeclaredConstructors();
        assertEquals("there are more than 1 constructors", 1, constructors.length);

        Constructor constructor = constructors[0];
        int modifiers = constructor.getModifiers();
        assertTrue("access modifier is not private", Modifier.isPrivate(modifiers));

        constructor.setAccessible(true);
        try {
            constructor.newInstance();
        } catch (Exception e) {
            ignore(e);
        }
    }

    public static void assertEnumCoverage(Class<? extends Enum<?>> enumClass) {
        Object values = null;
        Object lastValue = null;
        try {
            values = enumClass.getMethod("values").invoke(null);
        } catch (Throwable e) {
            fail("could not invoke values() method of enum " + enumClass);
        }
        try {
            for (Object value : (Object[]) values) {
                lastValue = value;
                enumClass.getMethod("valueOf", String.class).invoke(null, value.toString());
            }
        } catch (Throwable e) {
            fail("could not invoke valueOf(" + lastValue + ") method of enum " + enumClass);
        }
    }

    public static <E> void assertContains(Collection<E> collection, E expected) {
        if (!collection.contains(expected)) {
            fail(format("Collection %s didn't contain expected '%s'", collection, expected));
        }
    }

    public static <E> void assertNotContains(Collection<E> collection, E expected) {
        if (collection.contains(expected)) {
            fail(format("Collection %s contained unexpected '%s'", collection, expected));
        }
    }

    public static <E> void assertContainsAll(Collection<E> collection, Collection<E> expected) {
        if (!collection.containsAll(expected)) {
            fail(format("Collection %s didn't contain expected %s", collection, expected));
        }
    }

    public static <E> void assertNotContainsAll(Collection<E> collection, Collection<E> expected) {
        if (collection.containsAll(expected)) {
            fail(format("Collection %s contained unexpected %s", collection, expected));
        }
    }

    public static void assertContains(String string, String expected) {
        if (!string.contains(expected)) {
            fail(format("'%s' didn't contain expected '%s'", string, expected));
        }
    }

    public static void assertNotContains(String string, String expected) {
        if (string.contains(expected)) {
            fail(format("'%s' contained unexpected '%s'", string, expected));
        }
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

    @SuppressWarnings("unchecked")
    public static <E> E assertInstanceOf(Class<E> clazz, Object object) {
        assertNotNull(object);
        assertTrue(object + " is not an instanceof " + clazz.getName(), clazz.isAssignableFrom(object.getClass()));
        return (E) object;
    }

    public static void assertJoinable(Thread... threads) {
        assertJoinable(ASSERT_TRUE_EVENTUALLY_TIMEOUT, threads);
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
            throw new RuntimeException(e);
        }
    }

    public static void assertIterableEquals(Iterable iterable, Object... values) {
        List<Object> actual = new ArrayList<Object>();
        for (Object object : iterable) {
            actual.add(object);
        }

        List expected = Arrays.asList(values);

        assertEquals("size should match", expected.size(), actual.size());
        assertEquals(expected, actual);
    }

    public static void assertCompletesEventually(final Future future) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertTrue("Future has not completed", future.isDone());
            }
        });
    }

    public static void assertSizeEventually(int expectedSize, Collection collection) {
        assertSizeEventually(expectedSize, collection, ASSERT_TRUE_EVENTUALLY_TIMEOUT);
    }

    public static void assertSizeEventually(final int expectedSize, final Collection collection, long timeoutSeconds) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals("the size of the collection is not correct: found-content:" + collection, expectedSize,
                        collection.size());
            }
        }, timeoutSeconds);
    }

    public static void assertSizeEventually(int expectedSize, Map<?, ?> map) {
        assertSizeEventually(expectedSize, map, ASSERT_TRUE_EVENTUALLY_TIMEOUT);
    }

    public static void assertSizeEventually(final int expectedSize, final Map<?, ?> map, long timeoutSeconds) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals("the size of the map is not correct", expectedSize, map.size());
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

    public static <E> void assertEqualsEventually(final Callable<E> task, final E value) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(value, task.call());
            }
        });
    }

    public static void assertClusterSize(int expectedSize, HazelcastInstance instance) {
        int clusterSize = getClusterSize(instance);
        if (expectedSize != clusterSize) {
            fail(format("Cluster size is not correct. Expected: %d Actual: %d", expectedSize, clusterSize));
        }
    }

    private static int getClusterSize(HazelcastInstance instance) {
        Set<Member> members = instance.getCluster().getMembers();
        return members == null ? 0 : members.size();
    }

    public static void assertClusterSizeEventually(int expectedSize, HazelcastInstance instance) {
        assertClusterSizeEventually(expectedSize, instance, ASSERT_TRUE_EVENTUALLY_TIMEOUT);
    }

    public static void assertClusterSizeEventually(final int expectedSize, final HazelcastInstance instance,
                                                   long timeoutSeconds) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertClusterSize(expectedSize, instance);
            }
        }, timeoutSeconds);
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
                assertTrue(format("CountDownLatch failed to complete within %d seconds, count left: %d", timeoutSeconds,
                        latch.getCount()), completed);
            } else {
                assertTrue(format("%s, failed to complete within %d seconds, count left: %d", message, timeoutSeconds,
                        latch.getCount()), completed);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void assertCountEventually(final String message, final int expectedCount, final CountDownLatch latch,
                                             long timeoutInSeconds) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (int i = 0; i < 2; i++) { // recheck to see if hasn't changed
                    if (latch.getCount() != expectedCount) {
                        throw new AssertionError("Latch count has not been met. " + message);
                    }
                    sleepMillis(50);
                }
            }
        }, timeoutInSeconds);
    }

    public static void assertAtomicEventually(final String message, final int expectedValue, final AtomicInteger atomic,
                                              int timeoutInSeconds) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (int i = 0; i < 2; i++) { // recheck to see if hasn't changed
                    if (atomic.get() != expectedValue) {
                        throw new AssertionError("Atomic value has not been met. " + message);
                    }
                    sleepMillis(50);
                }
            }
        }, timeoutInSeconds);
    }

    public static void assertAtomicEventually(final String message, final boolean expectedValue, final AtomicBoolean atomic,
                                              int timeoutInSeconds) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (int i = 0; i < 2; i++) { // recheck to see if hasn't changed
                    if (atomic.get() != expectedValue) {
                        throw new AssertionError("Atomic value has not been met. " + message);
                    }
                    sleepMillis(50);
                }
            }
        }, timeoutInSeconds);
    }

    public static void assertFieldEqualsTo(Object object, String fieldName, Object expectedValue) {
        Class<?> clazz = object.getClass();
        try {
            Field field = clazz.getDeclaredField(fieldName);
            field.setAccessible(true);
            Object actualValue = field.get(object);
            assertEquals(expectedValue, actualValue);
        } catch (NoSuchFieldException e) {
            fail("Class " + clazz + " does not have field named " + fieldName + " declared");
        } catch (IllegalAccessException e) {
            fail("Cannot access field " + fieldName + " on class " + clazz);
        }
    }

    public static void assertTrueFiveSeconds(AssertTask task) {
        assertTrueAllTheTime(task, 5);
    }

    public static void assertTrueAllTheTime(AssertTask task, long durationSeconds) {
        for (int i = 0; i < durationSeconds; i++) {
            try {
                task.run();
            } catch (Exception e) {
                throw rethrow(e);
            }
            sleepSeconds(1);
        }
    }

    public static void assertTrueEventually(AssertTask task, long timeoutSeconds) {
        AssertionError error = null;
        // we are going to check five times a second
        int sleepMillis = 200;
        long iterations = timeoutSeconds * 5;
        for (int i = 0; i < iterations; i++) {
            try {
                try {
                    task.run();
                } catch (Exception e) {
                    throw rethrow(e);
                }
                return;
            } catch (AssertionError e) {
                error = e;
            }
            sleepMillis(sleepMillis);
        }
        if (error != null) {
            throw error;
        }
        fail("assertTrueEventually() failed without AssertionError!");
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
            throw rethrow(e);
        }
    }

    /**
     * This method executes the normal assertEquals with expected and actual values.
     * In addition it formats the given string with those values to provide a good assert message.
     *
     * @param message  assert message which is formatted with expected and actual values
     * @param expected expected value which is used for assert
     * @param actual   actual value which is used for assert
     */
    public static void assertEqualsStringFormat(String message, Object expected, Object actual) {
        assertEquals(format(message, expected, actual), expected, actual);
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
                lastException = e instanceof RuntimeException ? (RuntimeException) e : new RuntimeException(e);
            }
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                lastException = new RuntimeException(e);
            }
        }
        throw lastException;
    }

    public static void assertWaitingOperationCountEventually(int expectedOpsCount, HazelcastInstance... instances) {
        for (HazelcastInstance instance : instances) {
            assertWaitingOperationCountEventually(expectedOpsCount, instance);
        }
    }

    public static void assertWaitingOperationCountEventually(final int expectedOpsCount, HazelcastInstance instance) {
        final OperationParkerImpl waitNotifyService = getOperationParkingService(instance);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(expectedOpsCount, waitNotifyService.getTotalParkedOperationCount());
            }
        });
    }

    private static OperationParkerImpl getOperationParkingService(HazelcastInstance instance) {
        return (OperationParkerImpl) getNodeEngineImpl(instance).getOperationParker();
    }

    // ###################################
    // ########## reflection utils #######
    // ###################################

    public static Object getFromField(Object target, String fieldName) {
        try {
            Field field = target.getClass().getDeclaredField(fieldName);
            if (!Modifier.isPublic(field.getModifiers())) {
                field.setAccessible(true);
            }
            return field.get(target);
        } catch (NoSuchFieldException e) {
            throw new AssertionError(e);
        } catch (IllegalAccessException e) {
            throw new AssertionError(e);
        }
    }

    // ###################################
    // ########## inner classes ##########
    // ###################################

    public static final class DummyUncheckedHazelcastTestException extends RuntimeException {
    }

    public static class DummySerializableCallable implements Callable, Serializable {

        @Override
        public Object call() throws Exception {
            return null;
        }
    }

    // ############################################
    // ########## read from backup utils ##########
    // ############################################

    protected Object readFromMapBackup(HazelcastInstance instance, String mapName, Object key) {
        return readFromMapBackup(instance, mapName, key, 1);
    }

    protected Object readFromMapBackup(HazelcastInstance instance, String mapName, Object key, int replicaIndex) {
        try {
            NodeEngine nodeEngine = getNode(instance).getNodeEngine();
            SerializationService ss = getNode(instance).getSerializationService();
            int partitionId = nodeEngine.getPartitionService().getPartitionId(key);

            MapOperation get = getMapOperationProvider().createGetOperation(mapName, ss.toData(key));
            get.setPartitionId(partitionId);
            get.setReplicaIndex(replicaIndex);

            return getNode(instance).getNodeEngine().getOperationService().invokeOnPartition(get).get();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    protected MapOperationProvider getMapOperationProvider() {
        return new DefaultMapOperationProvider();
    }
}
