/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.core.Member;
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeExtension;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.impl.PartitionServiceState;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
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
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.jitter.JitterRule;
import org.junit.After;
import org.junit.ComparisonFailure;
import org.junit.Rule;
import org.junit.experimental.categories.Category;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.internal.partition.TestPartitionUtils.getPartitionServiceState;
import static com.hazelcast.test.TestEnvironment.isRunningCompatibilityTest;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static java.lang.Integer.getInteger;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

/**
 * Base class for Hazelcast tests which provides a big number of convenient test methods.
 * <p>
 * Has built-in support for {@link TestHazelcastInstanceFactory}, {@link JitterRule} and {@link DumpBuildInfoOnFailureRule}.
 * <p>
 * Tests which are be extended in Hazelcast Enterprise, should use {@link #getConfig()} instead of {@code new Config()},
 * so the Enterprise test can override this method to return a config with a {@link com.hazelcast.config.NativeMemoryConfig}.
 */
@SuppressWarnings({"unused", "SameParameterValue", "WeakerAccess"})
public abstract class HazelcastTestSupport {

    private static final boolean EXPECT_DIFFERENT_HASHCODES = (new Object().hashCode() != new Object().hashCode());

    public static final int ASSERT_TRUE_EVENTUALLY_TIMEOUT;

    @Rule
    public JitterRule jitterRule = new JitterRule();

    @Rule
    public DumpBuildInfoOnFailureRule dumpInfoRule = new DumpBuildInfoOnFailureRule();

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

    public static Config smallInstanceConfig() {
        // make the test instances consume less resources per default
        return new Config()
                .setProperty(GroupProperty.PARTITION_COUNT.getName(), "11")
                .setProperty(GroupProperty.PARTITION_OPERATION_THREAD_COUNT.getName(), "2")
                .setProperty(GroupProperty.GENERIC_OPERATION_THREAD_COUNT.getName(), "2")
                .setProperty(GroupProperty.EVENT_THREAD_COUNT.getName(), "1");
    }

    public static Config regularInstanceConfig() {
        return new Config();
    }

    protected Config getConfig() {
        return regularInstanceConfig();
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
        return factory = createHazelcastInstanceFactory0(nodeCount);
    }

    protected final TestHazelcastInstanceFactory createHazelcastInstanceFactory(String... addresses) {
        if (factory != null) {
            throw new IllegalStateException("Node factory is already created!");
        }
        if (isRunningCompatibilityTest()) {
            throw new UnsupportedOperationException(
                    "Cannot start a factory with specific addresses when running compatibility tests");
        } else {
            return factory = new TestHazelcastInstanceFactory(addresses);
        }
    }

    protected final TestHazelcastInstanceFactory createHazelcastInstanceFactory() {
        if (factory != null) {
            throw new IllegalStateException("Node factory is already created!");
        }
        return factory = createHazelcastInstanceFactory0(null);
    }

    protected final TestHazelcastInstanceFactory createHazelcastInstanceFactory(int initialPort, String... addresses) {
        if (factory != null) {
            throw new IllegalStateException("Node factory is already created!");
        }
        if (isRunningCompatibilityTest()) {
            throw new UnsupportedOperationException(
                    "Cannot start a factory with specific addresses when running compatibility tests");
        } else {
            return factory = new TestHazelcastInstanceFactory(initialPort, addresses);
        }
    }

    @SuppressWarnings("unchecked")
    private static TestHazelcastInstanceFactory createHazelcastInstanceFactory0(Integer nodeCount) {
        if (isRunningCompatibilityTest() && BuildInfoProvider.getBuildInfo().isEnterprise()) {
            try {
                String className = "com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory";
                Class<? extends TestHazelcastInstanceFactory> compatibilityTestFactoryClass
                        = (Class<? extends TestHazelcastInstanceFactory>) Class.forName(className);
                // nodeCount is ignored when constructing compatibility test factory
                return compatibilityTestFactoryClass.getConstructor().newInstance();
            } catch (Exception e) {
                throw rethrow(e);
            }
        } else {
            return nodeCount == null ? new TestHazelcastInstanceFactory() : new TestHazelcastInstanceFactory(nodeCount);
        }
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

    public static HazelcastInstance getFirstBackupInstance(HazelcastInstance[] instances, int partitionId) {
        return getBackupInstance(instances, partitionId, 1);
    }

    public static HazelcastInstance getBackupInstance(HazelcastInstance[] instances, int partitionId, int replicaIndex) {
        InternalPartition partition = getPartitionService(instances[0]).getPartition(partitionId);
        Address backupAddress = partition.getReplicaAddress(replicaIndex);
        for (HazelcastInstance instance : instances) {
            if (instance.getCluster().getLocalMember().getAddress().equals(backupAddress)) {
                return instance;
            }
        }
        throw new AssertionError("Could not find backup member for partition " + partitionId);
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
     * Sleeps for time that is left until referenceTime + seconds. If no
     * time is left until that time, a warning is logged and no sleep
     * will happen.
     * <p>
     * Opposed to the language provided sleep constructs, this method
     * does not guarantee minimum sleep time as it assumes that no time
     * elapsed since {@code referenceTime} which is never true.
     * <p>
     * This method can be useful in hiding occasional hiccups of the
     * execution environment in tests which are sensitive to
     * oversleeping. Tests like the ones verify TTL, lease time behavior
     * are typical examples for that.
     *
     * @param referenceTime the time in milliseconds since which the
     *                      sleep end time should be calculated
     * @param seconds       desired sleep duration in seconds
     */
    public static void sleepAtMostSeconds(long referenceTime, int seconds) {
        long now = System.currentTimeMillis();
        long sleepEnd = referenceTime + SECONDS.toMillis(seconds);
        long sleepTime = sleepEnd - now;

        if (sleepTime > 0) {
            try {
                MILLISECONDS.sleep(sleepTime);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        } else {
            ILogger logger = Logger.getLogger(HazelcastTestSupport.class);
            long absSleepTime = Math.abs(sleepTime);
            logger.warning("There is no time left to sleep. We are beyond the desired end of sleep by " + absSleepTime + "ms");
        }
    }

    /**
     * Sleeps for the given amount of time and after that, sets stop to true.
     * <p>
     * If stop is changed to true while sleeping, the calls returns before waiting the full sleeping period.
     * <p>
     * This method is very useful for stress tests that run for a certain amount of time. But if one of the stress tests
     * runs into a failure, the test should be aborted immediately. This is done by letting the thread set stop to true.
     *
     * @param stop            an {@link AtomicBoolean} to stop the sleep method
     * @param durationSeconds sleep duration in seconds
     */
    public static void sleepAndStop(AtomicBoolean stop, long durationSeconds) {
        final long startMillis = System.currentTimeMillis();

        for (int i = 0; i < durationSeconds; i++) {
            if (stop.get()) {
                return;
            }
            sleepSeconds(1);

            // if the system or JVM is really stressed we may oversleep to much and get a timeout
            if (System.currentTimeMillis() - startMillis > SECONDS.toMillis(durationSeconds)) {
                break;
            }
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
    // ########## partition ID ##########
    // ##################################

    /**
     * Gets a partition ID owned by this particular member.
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
        suspectMember(n1, n2);
        suspectMember(n2, n1);
    }

    public static void suspectMember(Node suspectingNode, Node suspectedNode, String reason) {
        if (suspectingNode != null && suspectedNode != null) {
            Member suspectedMember = suspectingNode.getClusterService().getMember(suspectedNode.getLocalMember().getAddress());
            if (suspectedMember != null) {
                suspectingNode.clusterService.suspectMember(suspectedMember, reason, true);
            }
        }
    }

    public static void suspectMember(HazelcastInstance source, HazelcastInstance target) {
        suspectMember(getNode(source), getNode(target));
    }

    public static void suspectMember(Node suspectingNode, Node suspectedNode) {
        suspectMember(suspectingNode, suspectedNode, null);
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
        TestUtil.warmUpPartitions(instances);
    }

    public static void warmUpPartitions(Collection<HazelcastInstance> instances) {
        TestUtil.warmUpPartitions(instances);
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
            if (node == null) {
                continue;
            }
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
            public void run() {
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

    public static void assertPropertiesEquals(Properties expected, Properties actual) {
        if (expected == null && actual == null) {
            return;
        }

        if (expected == null || actual == null) {
            fail(formatAssertMessage("", expected, actual));
        }

        for (String key : expected.stringPropertyNames()) {
            assertEquals("Unexpected value for key " + key, expected.getProperty(key), actual.getProperty(key));
        }

        for (String key : actual.stringPropertyNames()) {
            assertEquals("Unexpected value for key " + key + " from actual object", expected.getProperty(key),
                    actual.getProperty(key));
        }
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
            long remainingTimeout = TimeUnit.SECONDS.toMillis(timeoutSeconds);
            for (Thread thread : threads) {
                long start = System.currentTimeMillis();
                thread.join(remainingTimeout);

                if (thread.isAlive()) {
                    fail("Timeout waiting for thread " + thread.getName() + " to terminate");
                }

                long duration = System.currentTimeMillis() - start;
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

        List expected = asList(values);

        assertEquals("size should match", expected.size(), actual.size());
        assertEquals(expected, actual);
    }

    public static void assertCompletesEventually(final Future future) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue("Future has not completed", future.isDone());
            }
        });
    }

    public static void assertCompletesEventually(final Future future, long timeoutSeconds) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue("Future has not completed", future.isDone());
            }
        }, timeoutSeconds);
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

    public static void assertEqualsEventually(final int expected, final AtomicInteger value) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(expected, value.get());
            }
        });
    }

    public static void assertClusterSize(int expectedSize, HazelcastInstance... instances) {
        for (int i = 0; i < instances.length; i++) {
            int clusterSize = getClusterSize(instances[i]);
            if (expectedSize != clusterSize) {
                fail(format("Cluster size is not correct. Expected: %d, actual: %d, instance index: %d", expectedSize, clusterSize, i));
            }
        }
    }

    private static int getClusterSize(HazelcastInstance instance) {
        Set<Member> members = instance.getCluster().getMembers();
        return members == null ? 0 : members.size();
    }

    public static void assertClusterSizeEventually(int expectedSize, HazelcastInstance... instances) {
        for (HazelcastInstance instance : instances) {
            assertClusterSizeEventually(expectedSize, instance, ASSERT_TRUE_EVENTUALLY_TIMEOUT);
        }
    }

    public static void assertClusterSizeEventually(int expectedSize, Collection<HazelcastInstance> instances) {
        for (HazelcastInstance instance : instances) {
            assertClusterSizeEventually(expectedSize, instance, ASSERT_TRUE_EVENTUALLY_TIMEOUT);
        }
    }

    public static void assertClusterSizeEventually(final int expectedSize, final HazelcastInstance instance,
                                                   long timeoutSeconds) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertClusterSize(expectedSize, instance);
            }
        }, timeoutSeconds);
    }

    public static void assertMasterAddress(Address masterAddress, HazelcastInstance... instances) {
        for (HazelcastInstance instance : instances) {
            assertEquals(masterAddress, getNode(instance).getMasterAddress());
        }
    }

    public static void assertMasterAddressEventually(final Address masterAddress, final HazelcastInstance... instances) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : instances) {
                    assertMasterAddress(masterAddress, instance);
                }
            }
        });
    }

    public static void assertClusterState(ClusterState expectedState, HazelcastInstance... instances) {
        for (HazelcastInstance instance : instances) {
            assertEquals("Instance " + instance.getCluster().getLocalMember(),
                    expectedState, instance.getCluster().getClusterState());
        }
    }

    public static void assertClusterStateEventually(final ClusterState expectedState, final HazelcastInstance... instances) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertClusterState(expectedState, instances);
            }
        });
    }

    public static void assertOpenEventually(CountDownLatch latch) {
        assertOpenEventually(latch, ASSERT_TRUE_EVENTUALLY_TIMEOUT);
    }

    public static void assertOpenEventually(ICountDownLatch latch) {
        assertOpenEventually(latch, ASSERT_TRUE_EVENTUALLY_TIMEOUT);
    }

    public static void assertOpenEventually(String message, CountDownLatch latch) {
        assertOpenEventually(message, new CountdownLatchAdapter(latch), ASSERT_TRUE_EVENTUALLY_TIMEOUT);
    }

    public static void assertOpenEventually(CountDownLatch latch, long timeoutSeconds) {
        assertOpenEventually(null, new CountdownLatchAdapter(latch), timeoutSeconds);
    }

    public static void assertOpenEventually(ICountDownLatch latch, long timeoutSeconds) {
        assertOpenEventually(null, new ICountdownLatchAdapter(latch), timeoutSeconds);
    }

    public static void assertOpenEventually(String message, CountDownLatch latch, long timeoutSeconds) {
        assertOpenEventually(message, new CountdownLatchAdapter(latch), timeoutSeconds);
    }

    public static void assertOpenEventually(String message, Latch latch, long timeoutSeconds) {
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
            public void run() {
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
            public void run() {
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
            public void run() {
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

    public static void assertFalseEventually(AssertTask task, long timeoutSeconds) {
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
            } catch (AssertionError e) {
                return;
            }
            sleepMillis(sleepMillis);
        }
        fail("assertFalseEventually() failed without AssertionError!");
    }

    public static void assertFalseEventually(AssertTask task) {
        assertFalseEventually(task, ASSERT_TRUE_EVENTUALLY_TIMEOUT);
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

    /**
     * This method executes the normal assertNotEquals with expected and actual values.
     * In addition it formats the given string with those values to provide a good assert message.
     *
     * @param message  assert message which is formatted with expected and actual values
     * @param expected expected value which is used for assert
     * @param actual   actual value which is used for assert
     */
    public static void assertNotEqualsStringFormat(String message, Object expected, Object actual) {
        assertNotEquals(format(message, expected, actual), expected, actual);
    }

    public static void assertBetween(String label, long actualValue, long lowerBound, long upperBound) {
        assertTrue(format("Expected %s between %d and %d, but was %d", label, lowerBound, upperBound, actualValue),
                actualValue >= lowerBound && actualValue <= upperBound);
    }

    public static void assertGreaterOrEquals(String label, long actualValue, long lowerBound) {
        assertTrue(format("Expected %s greater or equals %d, but was %d", label, lowerBound, actualValue),
                actualValue >= lowerBound);
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
            public void run() {
                assertEquals(expectedOpsCount, waitNotifyService.getTotalParkedOperationCount());
            }
        });
    }

    private static OperationParkerImpl getOperationParkingService(HazelcastInstance instance) {
        return (OperationParkerImpl) getNodeEngineImpl(instance).getOperationParker();
    }

    public static void assertThatIsNoParallelTest() {
        assertFalse("Test cannot be a ParallelTest", hasTestCategory(ParallelTest.class));
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

    /**
     * Checks if the calling test has a given {@link Category}.
     *
     * @see #getTestCategories() getTestCategories() for limitations of this method
     */
    public static boolean hasTestCategory(Class<?> testCategory) {
        for (Class<?> category : getTestCategories()) {
            if (category.isAssignableFrom(testCategory)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns the value of the {@link Category} annotation of the calling test class.
     * <p>
     * This method doesn't cover {@link Category} annotations on a test method.
     * It may also fail on test class hierarchies (the annotated class has to be in the stack trace).
     */
    public static HashSet<Class<?>> getTestCategories() {
        HashSet<Class<?>> testCategories = new HashSet<Class<?>>();
        for (StackTraceElement stackTraceElement : new Exception().getStackTrace()) {
            String className = stackTraceElement.getClassName();
            try {
                Class<?> clazz = Class.forName(className);
                Category annotation = clazz.getAnnotation(Category.class);
                if (annotation != null) {
                    List<Class<?>> categoryList = asList(annotation.value());
                    testCategories.addAll(categoryList);
                }
            } catch (Exception ignored) {
            }
        }
        if (testCategories.isEmpty()) {
            fail("Could not find any classes with a @Category annotation in the stack trace");
        }
        return testCategories;
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

    private interface Latch {
        boolean await(long timeout, TimeUnit unit)
                throws InterruptedException;
        long getCount();
    }

    private static class ICountdownLatchAdapter
            implements Latch {

        private final ICountDownLatch latch;

        ICountdownLatchAdapter(ICountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public boolean await(long timeout, TimeUnit unit)
                throws InterruptedException {
            return latch.await(timeout, unit);
        }

        @Override
        public long getCount() {
            return latch.getCount();
        }
    }

    private static class CountdownLatchAdapter
            implements Latch {

        private final CountDownLatch latch;

        CountdownLatchAdapter(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public boolean await(long timeout, TimeUnit unit)
                throws InterruptedException {
            return latch.await(timeout, unit);
        }

        @Override
        public long getCount() {
            return latch.getCount();
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

    public static void assumeThatNoJDK6() {
        String javaVersion = System.getProperty("java.version");
        assumeFalse("Java 6 used", javaVersion.startsWith("1.6."));
    }

    /**
     * Throws {@link org.junit.AssumptionViolatedException} if two new Objects have the same hashCode (e.g. when running tests
     * with static hashCode ({@code -XX:hashCode=2}).
     */
    public static void assumeDifferentHashCodes() {
        assumeTrue("Hash codes are equal for different objects", EXPECT_DIFFERENT_HASHCODES);
    }
}
