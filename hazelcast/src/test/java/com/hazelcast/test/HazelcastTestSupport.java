/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

import com.google.common.collect.Lists;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.cluster.Member;
import com.hazelcast.config.Config;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.ICountDownLatch;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.TestUtil;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.partition.IPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.impl.PartitionServiceState;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.server.ServerConnectionManager;
import com.hazelcast.internal.tpcengine.util.OS;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.function.RunnableEx;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationparker.impl.OperationParkerImpl;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.jitter.JitterRule;
import com.hazelcast.test.metrics.MetricsRule;
import junit.framework.AssertionFailedError;
import org.apache.commons.lang3.StringUtils;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assume;
import org.junit.AssumptionViolatedException;
import org.junit.ClassRule;
import org.junit.ComparisonFailure;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.function.ThrowingRunnable;
import org.junit.jupiter.api.AfterEach;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.StackWalker.Option;
import java.lang.StackWalker.StackFrame;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteOrder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static com.hazelcast.internal.partition.TestPartitionUtils.getPartitionServiceState;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.test.TestEnvironment.isRunningCompatibilityTest;
import static java.lang.Integer.getInteger;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

/**
 * Base class for Hazelcast tests which provides a big number of convenient test methods.
 * <p>
 * Has built-in support for {@link TestHazelcastInstanceFactory}, {@link JitterRule} and
 * {@link DumpBuildInfoOnFailureRule}.
 * <p>
 * Tests which are be extended in Hazelcast Enterprise, should use {@link #getConfig()}
 * instead of {@code new Config()}, so the Enterprise test can override this method to
 * return a config with a {@link com.hazelcast.config.NativeMemoryConfig}.
 */
public abstract class HazelcastTestSupport {
    public static final String JAVA_VENDOR = System.getProperty("java.vendor");

    public static final int ASSERT_TRUE_EVENTUALLY_TIMEOUT;
    public static final Duration ASSERT_TRUE_EVENTUALLY_TIMEOUT_DURATION;
    public static final int ASSERT_COMPLETES_STALL_TOLERANCE;
    public static final String PERSISTENT_MEMORY_DIRECTORIES;

    private static final String COMPAT_HZ_INSTANCE_FACTORY =
            "com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory";
    private static final boolean EXPECT_DIFFERENT_HASHCODES = (new Object().hashCode() != new Object().hashCode());
    private static final ILogger LOGGER = Logger.getLogger(HazelcastTestSupport.class);

    @ClassRule
    public static MobyNamingRule mobyNamingRule = new MobyNamingRule();

    @Rule
    public JitterRule jitterRule = new JitterRule();

    @Rule
    public MetricsRule metricsRule = new MetricsRule();

    @Rule
    public DumpBuildInfoOnFailureRule dumpInfoRule = new DumpBuildInfoOnFailureRule();

    private TestHazelcastInstanceFactory factory;

    static {
        ASSERT_TRUE_EVENTUALLY_TIMEOUT = getInteger("hazelcast.assertTrueEventually.timeout", 120);
        ASSERT_TRUE_EVENTUALLY_TIMEOUT_DURATION = Duration.ofSeconds(ASSERT_TRUE_EVENTUALLY_TIMEOUT);
        LOGGER.fine("ASSERT_TRUE_EVENTUALLY_TIMEOUT = %s", ASSERT_TRUE_EVENTUALLY_TIMEOUT);
        ASSERT_COMPLETES_STALL_TOLERANCE = getInteger("hazelcast.assertCompletes.stallTolerance", 20);
        LOGGER.fine("ASSERT_COMPLETES_STALL_TOLERANCE = %s", ASSERT_COMPLETES_STALL_TOLERANCE);
        String pmemDirectories = System.getProperty("hazelcast.persistent.memory");
        PERSISTENT_MEMORY_DIRECTORIES = pmemDirectories != null ? pmemDirectories : "/tmp/pmem0,/tmp/pmem1";
        ClusterProperty.METRICS_COLLECTION_FREQUENCY.setSystemProperty("1");
        ClusterProperty.METRICS_DEBUG.setSystemProperty("true");
        Assertions.setMaxStackTraceElementsDisplayed(100);
    }

    @AfterEach
    @After
    public final void shutdownNodeFactory() {
        LOGGER.info("Shutting down node factory as @After action");
        TestHazelcastInstanceFactory testHazelcastInstanceFactory = factory;
        if (testHazelcastInstanceFactory != null) {
            factory = null;
            testHazelcastInstanceFactory.terminateAll();
        }
    }

    public final void shutdownGracefullyNodeFactory() {
        LOGGER.info("Shutting down node factory");
        TestHazelcastInstanceFactory testHazelcastInstanceFactory = factory;
        if (testHazelcastInstanceFactory != null) {
            factory = null;
            testHazelcastInstanceFactory.shutdownAll();
        }
    }


    // ###################################
    // ########## configuration ##########
    // ###################################

    public static Config smallInstanceConfig() {
        return shrinkInstanceConfig(new Config());
    }

    public static Config smallInstanceConfigWithoutJetAndMetrics() {
        return smallInstanceConfigWithoutJetAndMetrics(new Config());
    }

    private static Config smallInstanceConfigWithoutJetAndMetrics(Config config) {
        // make the test instances consume less resources per default
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "11")
                .setProperty(ClusterProperty.PARTITION_OPERATION_THREAD_COUNT.getName(), "2")
                .setProperty(ClusterProperty.GENERIC_OPERATION_THREAD_COUNT.getName(), "2")
                .setProperty(ClusterProperty.EVENT_THREAD_COUNT.getName(), "1");

        config.setProperty("hazelcast.logging.type", "log4j2");
        config.getMetricsConfig().setEnabled(false);
        config.getJetConfig().setEnabled(false);
        return config;
    }

    public static Config shrinkInstanceConfig(Config config) {
        smallInstanceConfigWithoutJetAndMetrics(config);
        config.getMetricsConfig().setEnabled(true);
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(2);
        return config;
    }

    public static Config regularInstanceConfig() {
        Config config = new Config();
        config.getJetConfig().setEnabled(true);
        return config;
    }

    // disables auto-detection/tcp-ip network discovery on the given config and returns the same
    public static Config withoutNetworkJoin(Config config) {
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        return config;
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

    protected HazelcastInstance[] createHazelcastInstances(int nodeCount) {
        return createHazelcastInstances(getConfig(), nodeCount);
    }

    protected HazelcastInstance[] createHazelcastInstances(Config config, int nodeCount) {
        createHazelcastInstanceFactory(nodeCount);
        return factory.newInstances(config, nodeCount);
    }

    protected final TestHazelcastInstanceFactory createHazelcastInstanceFactory(int nodeCount) {
        if (factory != null) {
            throw new IllegalStateException("Node factory is already created!");
        }
        return factory = createHazelcastInstanceFactory0(nodeCount).withMetricsRule(metricsRule);
    }

    protected final TestHazelcastInstanceFactory createHazelcastInstanceFactory(String... addresses) {
        if (factory != null) {
            throw new IllegalStateException("Node factory is already created!");
        }
        if (isRunningCompatibilityTest()) {
            throw new UnsupportedOperationException(
                    "Cannot start a factory with specific addresses when running compatibility tests");
        } else {
            return factory = new TestHazelcastInstanceFactory(addresses).withMetricsRule(metricsRule);
        }
    }

    protected final TestHazelcastInstanceFactory createHazelcastInstanceFactory() {
        if (factory != null) {
            throw new IllegalStateException("Node factory is already created!");
        }
        return factory = createHazelcastInstanceFactory0(null).withMetricsRule(metricsRule);
    }

    protected final TestHazelcastInstanceFactory createHazelcastInstanceFactory(int initialPort, String... addresses) {
        if (factory != null) {
            throw new IllegalStateException("Node factory is already created!");
        }
        if (isRunningCompatibilityTest()) {
            throw new UnsupportedOperationException(
                    "Cannot start a factory with specific addresses when running compatibility tests");
        } else {
            return factory = new TestHazelcastInstanceFactory(initialPort, addresses).withMetricsRule(metricsRule);
        }
    }

    @SuppressWarnings("unchecked")
    private static TestHazelcastInstanceFactory createHazelcastInstanceFactory0(Integer nodeCount) {
        if (isRunningCompatibilityTest() && BuildInfoProvider.getBuildInfo().isEnterprise()) {
            try {
                Class<? extends TestHazelcastInstanceFactory> compatibilityTestFactoryClass
                        = (Class<? extends TestHazelcastInstanceFactory>) Class.forName(COMPAT_HZ_INSTANCE_FACTORY);
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

    public static Packet toPacket(HazelcastInstance local, HazelcastInstance remote, Operation operation) {
        InternalSerializationService serializationService = Accessors.getSerializationService(local);
        ServerConnectionManager connectionManager = Accessors.getConnectionManager(local);

        return new Packet(serializationService.toBytes(operation), operation.getPartitionId())
                .setPacketType(Packet.Type.OPERATION)
                .setConn(connectionManager.get(Accessors.getAddress(remote)));
    }

    // #####################################
    // ########## generic utility ##########
    // #####################################

    public static void ignore(Throwable ignored) {
    }

    /**
     * Note: the {@code cancel()} method on the returned Future has no effect.
     */
    public static Future<?> spawn(RunnableEx task) {
        FutureTask<Runnable> futureTask = new FutureTask<>(task, null);
        new Thread(futureTask).start();
        return futureTask;
    }

    /**
     * Note: the {@code cancel()} method on the returned Future has no effect.
     */
    public static Future<?> spawn(Runnable task) {
        return spawn(task::run);
    }

    /**
     * Note: the {@code cancel()} method on the returned Future has no effect.
     */
    public static <E> Future<E> spawn(Callable<E> task) {
        FutureTask<E> futureTask = new FutureTask<>(task);
        new Thread(futureTask).start();
        return futureTask;
    }

    public static void interruptCurrentThread(final int delayMillis) {
        final Thread currentThread = Thread.currentThread();
        new Thread(() -> {
            sleepMillis(delayMillis);
            currentThread.interrupt();
        }).start();
    }

    protected static <T> boolean containsIn(T item1, Collection<T> collection, Comparator<T> comparator) {
        for (T item2 : collection) {
            if (comparator.compare(item1, item2) == 0) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns the cartesian product of the specified lists.
     */
    public static List<Object[]> cartesianProduct(List<?>... lists) {
        return Lists.cartesianProduct(lists).stream().map(List::toArray).toList();
    }

    @SuppressWarnings("unchecked")
    public static <T, U> List<Tuple2<T, U>> cartesianProductTuple(List<T> t, List<U> u) {
        return Lists.cartesianProduct(List.of(t, u)).stream()
                .map(pair -> tuple2((T) pair.get(0), (U) pair.get(1)))
                .toList();
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
            SECONDS.sleep(seconds);
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
            long absSleepTime = Math.abs(sleepTime);
            LOGGER.warning("There is no time left to sleep. We are beyond the desired end of sleep by "
                    + absSleepTime + "ms");
        }
    }

    /**
     * Sleeps for the given amount of time and after that, sets stop to true.
     * <p>
     * If stop is changed to true while sleeping, the calls returns before waiting the full sleeping period.
     * <p>
     * This method is very useful for stress tests that run for a certain amount of time. But if one of the
     * stress tests runs into a failure, the test should be aborted immediately. This is done by letting the
     * thread set stop to true.
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

            // if the system or JVM is really stressed we may oversleep too much and get a timeout
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
        return UuidUtil.newUnsecureUuidString();
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
        List<Partition> partitions = new LinkedList<>();
        for (Partition partition : hz.getPartitionService().getPartitions()) {
            if (partition.getOwner().localMember()) {
                partitions.add(partition);
            }
        }
        if (partitions.isEmpty()) {
            throw new IllegalStateException("No partitions found for HazelcastInstance: " + hz.getName());
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
        checkPartitionCountGreaterOrEqualMemberCount(instance);

        PartitionService partitionService = instance.getPartitionService();
        while (true) {
            String id = randomString();
            Partition partition = partitionService.getPartition(id);
            if (partition.getPartitionId() == partitionId) {
                return id;
            }
        }
    }

    public static String generateKeyForPartition(HazelcastInstance instance, String prefix, int partitionId) {
        checkPartitionCountGreaterOrEqualMemberCount(instance);

        PartitionService partitionService = instance.getPartitionService();
        while (true) {
            String id = prefix + randomString();
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
        int memberCount = instance.getCluster().getMembers().size();
        int partitionCount = instance.getPartitionService().getPartitions().size();

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

        InternalPartitionService partitionService = Accessors.getPartitionService(hz);
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
        Node n1 = Accessors.getNode(h1);
        Node n2 = Accessors.getNode(h2);
        suspectMember(n1, n2);
        suspectMember(n2, n1);
    }

    public static void suspectMember(HazelcastInstance source, HazelcastInstance target) {
        suspectMember(Accessors.getNode(source), Accessors.getNode(target));
    }

    public static void suspectMember(Node suspectingNode, Node suspectedNode) {
        suspectMember(suspectingNode, suspectedNode, null);
    }

    public static void suspectMember(Node suspectingNode, Node suspectedNode, String reason) {
        if (suspectingNode != null && suspectedNode != null) {
            ClusterServiceImpl clusterService = suspectingNode.getClusterService();
            Member suspectedMember = clusterService.getMember(suspectedNode.getLocalMember().getAddress());
            if (suspectedMember != null) {
                clusterService.suspectMember(suspectedMember, reason, true);
            }
        }
    }

    private static void checkMemberCount(boolean generateOwnedKey, Cluster cluster) {
        if (generateOwnedKey) {
            return;
        }
        Set<Member> members = cluster.getMembers();
        if (members.size() < 2) {
            throw new UnsupportedOperationException(
                    "Cluster has only one member, you can not generate a `not owned key`");
        }
    }

    public static void warmUpPartitions(HazelcastInstance... instances) {
        TestUtil.warmUpPartitions(instances);
    }

    public static void warmUpPartitions(Collection<HazelcastInstance> instances) {
        TestUtil.warmUpPartitions(instances);
    }

    public static boolean isInstanceInSafeState(HazelcastInstance instance) {
        Node node = Accessors.getNode(instance);
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

    public static boolean isAllInSafeState(Collection<HazelcastInstance> nodes) {
        for (HazelcastInstance node : nodes) {
            if (!isInstanceInSafeState(node)) {
                return false;
            }
        }
        return true;
    }

    public static void waitInstanceForSafeState(final HazelcastInstance instance) {
        assertTrueEventually(() -> assertTrue(isInstanceInSafeState(instance)));
    }

    public static void waitClusterForSafeState(final HazelcastInstance instance) {
        assertTrueEventually(() -> assertTrue(isClusterInSafeState(instance)));
    }

    public static void waitAllForSafeState(Collection<HazelcastInstance> instances) {
        waitAllForSafeState(instances, ASSERT_TRUE_EVENTUALLY_TIMEOUT);
    }

    public static void waitAllForSafeState(final Collection<HazelcastInstance> instances, int timeoutInSeconds) {
        assertTrueEventually(() -> assertAllInSafeState(instances), timeoutInSeconds);
    }

    public static void waitAllForSafeState(final HazelcastInstance[] nodes, int timeoutInSeconds) {
        assertTrueEventually(() -> assertAllInSafeState(asList(nodes)), timeoutInSeconds);
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
        Map<Address, PartitionServiceState> nonSafeStates = new HashMap<>();
        for (HazelcastInstance node : nodes) {
            if (node == null) {
                continue;
            }
            final PartitionServiceState state = getPartitionServiceState(node);
            if (state != PartitionServiceState.SAFE) {
                nonSafeStates.put(Accessors.getAddress(node), state);
            }
        }

        assertThat(nonSafeStates).as("Instances not in safe state!").isEmpty();
    }

    public static void assertNoRunningInstances() {
        assertThat(Hazelcast.getAllHazelcastInstances()).as("There should be no running instances").isEmpty();
    }

    public static void assertNoRunningClientInstances() {
        assertThat(HazelcastClient.getAllHazelcastClients()).as("There should be no running client instances").isEmpty();
    }

    // ################################
    // ########## assertions ##########
    // ################################

    public static void assertUtilityConstructor(Class<?> clazz) {
        Constructor<?>[] constructors = clazz.getDeclaredConstructors();
        assertEquals("there are more than 1 constructors", 1, constructors.length);

        Constructor<?> constructor = constructors[0];
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
            fail(format("Collection %s (%d) didn't contain expected '%s'", collection, collection.size(), expected));
        }
    }

    public static <E> void assertNotContains(Collection<E> actual, E notExpected) {
        if (actual.contains(notExpected)) {
            fail(format("Collection %s (%d) contained unexpected '%s'", actual, actual.size(), notExpected));
        }
    }

    public static <E> void assertContainsAll(Collection<E> actual, Collection<E> expected) {
        if (!actual.containsAll(expected)) {
            fail(format("Collection %s (%d) didn't contain expected %s (%d)",
                    actual, actual.size(), expected, expected.size()));
        }
    }

    public static <E> void assertNotContainsAll(Collection<E> actual, Collection<E> notExpected) {
        if (actual.containsAll(notExpected)) {
            fail(format("Collection %s (%d) contained unexpected %s (%d)",
                    actual, actual.size(), notExpected, notExpected.size()));
        }
    }

    public static void assertContains(String actual, String expected) {
        if (actual == null || !actual.contains(expected)) {
            fail(format("'%s' didn't contain expected '%s'", actual, expected));
        }
    }

    public static void assertNotContains(String actual, String notExpected) {
        if (actual.contains(notExpected)) {
            fail(format("'%s' contained unexpected '%s'", actual, notExpected));
        }
    }

    public static void assertStartsWith(String expected, String actual) {
        if (actual != null && actual.startsWith(expected)) {
            return;
        }
        if (expected != null && actual != null) {
            throw new ComparisonFailure("", expected, actual);
        }
        fail(formatAssertMessage(expected, null));
    }

    public static void assertPropertiesEquals(Properties expected, Properties actual) {
        if (expected == null && actual == null) {
            return;
        }

        if (expected == null || actual == null) {
            fail(formatAssertMessage(expected, actual));
        }

        for (String key : expected.stringPropertyNames()) {
            assertEquals("Unexpected value for key " + key, expected.getProperty(key), actual.getProperty(key));
        }

        for (String key : actual.stringPropertyNames()) {
            assertEquals("Unexpected value for key " + key + " from actual object", expected.getProperty(key),
                    actual.getProperty(key));
        }
    }

    private static String formatAssertMessage(Object expected, Object actual) {
        StringBuilder assertMessage = new StringBuilder();
        String expectedString = String.valueOf(expected);
        String actualString = String.valueOf(actual);
        if (expectedString.equals(actualString)) {
            assertMessage.append("expected: ");
            formatClassAndValue(assertMessage, expected, expectedString);
            assertMessage.append(" but was: ");
            formatClassAndValue(assertMessage, actual, actualString);
        } else {
            assertMessage.append("expected: <").append(expectedString)
                    .append("> but was: <").append(actualString).append(">");
        }
        return assertMessage.toString();
    }

    private static void formatClassAndValue(StringBuilder message, Object value, String valueString) {
        message.append((value == null) ? "null" : value.getClass().getName())
                .append("<").append(valueString).append(">");
    }

    @SuppressWarnings("unchecked")
    public static <E> E assertInstanceOf(Class<E> expected, Object actual) {
        assertNotNull(actual);
        assertThat(expected).isAssignableFrom(actual.getClass());
        return (E) actual;
    }

    public static void assertJoinable(Thread... threads) {
        assertJoinable(ASSERT_TRUE_EVENTUALLY_TIMEOUT, threads);
    }

    public static void assertJoinable(long timeoutSeconds, Thread... threads) {
        try {
            long remainingTimeout = SECONDS.toMillis(timeoutSeconds);
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

    protected static <T> void assertCollection(Collection<T> expected, Collection<T> actual) {
        assertEquals(String.format("Expected collection: `%s`, actual collection: `%s`", expected, actual),
                expected.size(), actual.size());
        assertContainsAll(expected, actual);
    }

    protected static <T> void assertCollection(Collection<T> expected, Collection<T> actual, Comparator<T> comparator) {
        assertEquals(String.format("Expected collection: `%s`, actual collection: `%s`", expected, actual),
                expected.size(), actual.size());
        for (T item : expected) {
            if (!containsIn(item, actual, comparator)) {
                throw new AssertionError("Actual collection does not contain the item " + item);
            }
        }
    }

    public static void assertIterableEquals(Iterable<?> actual, Object... expected) {
        List<Object> actualList = new ArrayList<>();
        for (Object object : actual) {
            actualList.add(object);
        }

        var expectedList = asList(expected);

        assertEquals("size should match", expectedList.size(), actualList.size());
        assertEquals(expectedList, actualList);
    }

    public static void assertCompletesEventually(final Future<?> future) {
        assertCompletesEventually(future, ASSERT_TRUE_EVENTUALLY_TIMEOUT);
    }

    public static void assertCompletesEventually(final Future<?> future, long timeoutSeconds) {
        try {
            future.get(timeoutSeconds, TimeUnit.SECONDS);
        } catch (ExecutionException | RuntimeException e) {
            LOGGER.finest(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            fail(String.format("Future has not completed - %s", e));
        }
    }

    public static void assertSizeEventually(int expectedSize, Collection<?> collection) {
        assertSizeEventually(expectedSize, collection, ASSERT_TRUE_EVENTUALLY_TIMEOUT);
    }

    public static void assertSizeEventually(final int expectedSize, final Collection<?> collection,
                                            long timeoutSeconds) {
        assertTrueEventually(() -> assertEquals("the size of the collection is not correct: found-content:"
                + collection, expectedSize, collection.size()), timeoutSeconds);
    }

    public static void assertSizeEventually(int expectedSize, Supplier<Collection<?>> collectionSupplier) {
        assertSizeEventually(expectedSize, collectionSupplier, ASSERT_TRUE_EVENTUALLY_TIMEOUT);
    }

    public static void assertSizeEventually(int expectedSize, Supplier<Collection<?>> collectionSupplier,
                                            long timeoutSeconds) {
        assertTrueEventually(() -> assertEquals("the size of the collection is not correct: found-content:"
                        + collectionSupplier.get(), expectedSize, collectionSupplier.get().size()),
                timeoutSeconds);
    }

    public static void assertSizeEventually(int expectedSize, Map<?, ?> map) {
        assertSizeEventually(expectedSize, map, ASSERT_TRUE_EVENTUALLY_TIMEOUT);
    }

    public static void assertSizeEventually(final int expectedSize, final Map<?, ?> map, long timeoutSeconds) {
        assertTrueEventually(() -> assertEquals("the size of the map is not correct", expectedSize, map.size()),
                timeoutSeconds);
    }

    public static <E> void assertEqualsEventually(final FutureTask<E> task, final E expected) {
        assertTrueEventually(() -> {
            assertThat(task).as("FutureTask is not complete").isDone();
            assertEquals(expected, task.get());
        });
    }

    public static <E> void assertEqualsEventually(final Callable<E> task, final E expected) {
        assertTrueEventually(() -> assertEquals(expected, task.call()));
    }

    public static void assertEqualsEventually(final int expected, final AtomicInteger value) {
        assertTrueEventually(() -> assertEquals(expected, value.get()));
    }

    public static void assertClusterSize(int expectedSize, HazelcastInstance... instances) {
        for (int i = 0; i < instances.length; i++) {
            int clusterSize = getClusterSize(instances[i]);
            if (expectedSize != clusterSize) {
                fail(format("Cluster size is not correct. Expected: %d, actual: %d, instance index: %d",
                        expectedSize, clusterSize, i));
            }
        }
    }

    private static int getClusterSize(HazelcastInstance instance) {
        return instance.getCluster().getMembers().size();
    }

    public static void assertClusterSizeEventually(int expectedSize, HazelcastInstance... instances) {
        for (HazelcastInstance instance : instances) {
            assertClusterSizeEventually(expectedSize, instance, ASSERT_TRUE_EVENTUALLY_TIMEOUT);
        }
    }

    public static void assertClusterSizeEventually(
            int expectedSize,
            Collection<HazelcastInstance> instances,
            long timeout
    ) {
        for (HazelcastInstance instance : instances) {
            assertClusterSizeEventually(expectedSize, instance, timeout);
        }
    }

    public static void assertClusterSizeEventually(int expectedSize, Collection<HazelcastInstance> instances) {
        for (HazelcastInstance instance : instances) {
            assertClusterSizeEventually(expectedSize, instance, ASSERT_TRUE_EVENTUALLY_TIMEOUT);
        }
    }

    public static void assertClusterSizeEventually(final int expectedSize, final HazelcastInstance instance,
                                                   long timeoutSeconds) {
        assertTrueEventually(() -> assertClusterSize(expectedSize, instance), timeoutSeconds);
    }

    public static void assertMasterAddress(Address masterAddress, HazelcastInstance... instances) {
        for (HazelcastInstance instance : instances) {
            assertEquals(masterAddress, Accessors.getNode(instance).getMasterAddress());
        }
    }

    public static void assertMasterAddressEventually(final Address masterAddress,
                                                     final HazelcastInstance... instances) {
        assertTrueEventually(() -> {
            for (HazelcastInstance instance : instances) {
                assertMasterAddress(masterAddress, instance);
            }
        });
    }

    public static void assertClusterState(ClusterState expectedState, HazelcastInstance... instances) {
        for (HazelcastInstance instance : instances) {
            assertEquals("Instance " + instance.getCluster().getLocalMember(),
                    expectedState, instance.getCluster().getClusterState());
        }
    }

    public static void assertClusterStateEventually(final ClusterState expectedState,
                                                    final HazelcastInstance... instances) {
        assertTrueEventually(() -> assertClusterState(expectedState, instances));
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

    private static void assertOpenEventually(String message, Latch latch, long timeoutSeconds) {
        try {
            boolean completed = latch.await(timeoutSeconds, SECONDS);
            if (message == null) {
                assertTrue(format("CountDownLatch failed to complete within %d seconds, count left: %d",
                        timeoutSeconds, latch.getCount()), completed);
            } else {
                assertTrue(format("%s, failed to complete within %d seconds, count left: %d", message,
                        timeoutSeconds, latch.getCount()), completed);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void assertCountEventually(final String message, final int expectedCount,
                                             final CountDownLatch latch, long timeoutInSeconds) {
        assertTrueEventually(() -> {
            for (int i = 0; i < 2; i++) { // recheck to see if it hasn't changed
                if (latch.getCount() != expectedCount) {
                    throw new AssertionError("Latch count has not been met. " + message);
                }
                sleepMillis(50);
            }
        }, timeoutInSeconds);
    }

    public static void assertAtomicEventually(final String message, final int expectedValue,
                                              final AtomicInteger atomic, int timeoutInSeconds) {
        assertTrueEventually(() -> {
            for (int i = 0; i < 2; i++) { // recheck to see if it hasn't changed
                if (atomic.get() != expectedValue) {
                    throw new AssertionError("Atomic value has not been met. " + message);
                }
                sleepMillis(50);
            }
        }, timeoutInSeconds);
    }

    public static void assertAtomicEventually(final String message, final boolean expectedValue,
                                              final AtomicBoolean atomic, int timeoutInSeconds) {
        assertTrueEventually(() -> {
            for (int i = 0; i < 2; i++) { // recheck to see if it hasn't changed
                if (atomic.get() != expectedValue) {
                    throw new AssertionError("Atomic value has not been met. " + message);
                }
                sleepMillis(50);
            }
        }, timeoutInSeconds);
    }

    public static void assertTrueFiveSeconds(AssertTask task) {
        assertTrueAllTheTime(task, 5);
    }

    public static void assertTrueAllTheTime(AssertTask task, long durationSeconds) {
        for (int i = 0; i <= durationSeconds; i++) {
            try {
                task.run();
            } catch (Exception e) {
                throw rethrow(e);
            }
            // Don't wait if there is no next iteration
            if ((i + 1) <= durationSeconds) {
                sleepSeconds(1);
            }
        }
    }

    public static void assertTrueEventually(String message, AssertTask task, long timeoutSeconds) {
        AssertionError error = null;
        // we are going to check five times a second
        int sleepMillis = 200;
        long iterations = timeoutSeconds * 1000 / sleepMillis;
        long deadline = System.currentTimeMillis() + SECONDS.toMillis(timeoutSeconds);
        boolean passedTheDeadline = false;
        for (int i = 0; i < iterations && !passedTheDeadline; i++) {
            passedTheDeadline = System.currentTimeMillis() > deadline;
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
            if (!passedTheDeadline) {
                sleepMillis(sleepMillis);
            }
        }
        if (error != null) {
            throw error;
        }
        fail("assertTrueEventually() failed without AssertionError! " + message);
    }

    public static void assertTrueEventually(AssertTask task, long timeoutSeconds) {
        assertTrueEventually(null, task, timeoutSeconds);
    }


    public static void assertTrueEventually(String message, AssertTask task) {
        assertTrueEventually(message, task, ASSERT_TRUE_EVENTUALLY_TIMEOUT);
    }

    public static void assertCompletesEventually(String message, ProgressCheckerTask task) {
        assertCompletesEventually(message, task, ASSERT_TRUE_EVENTUALLY_TIMEOUT);
    }

    public static void assertCompletesEventually(ProgressCheckerTask task, long stallToleranceSeconds) {
        assertCompletesEventually(null, task, stallToleranceSeconds);
    }

    public static void assertCompletesEventually(ProgressCheckerTask task) {
        assertCompletesEventually(null, task, ASSERT_COMPLETES_STALL_TOLERANCE);
    }

    /**
     * Asserts whether the provided task eventually reports completion.
     * This form of eventual completion check has no strict time bounds.
     * Instead, it lets the checked test to continue as long as there is
     * progress observed within the provided stall tolerance time bound.
     * <p>
     * This check may be useful for tests that can provide progress
     * information to prevent unnecessary failures due to unexpectedly
     * slow progress.
     *
     * @param task                  The task that checking the progress
     *                              of some operation
     * @param stallToleranceSeconds The time in seconds that we tolerate
     *                              without progressing
     */
    public static void assertCompletesEventually(String message, ProgressCheckerTask task, long stallToleranceSeconds) {
        long taskStartTimestamp = System.currentTimeMillis();
        // we are going to check five times a second
        int sleepMillis = 200;
        List<TaskProgress> progresses = new LinkedList<>();
        long lastProgressTimestamp = System.currentTimeMillis();
        double lastProgress = 0;

        while (true) {
            try {
                TaskProgress progress = task.checkProgress();
                if (progress.isCompleted()) {
                    return;
                }
                boolean toleranceExceeded = progress.timestamp() > lastProgressTimestamp
                        + SECONDS.toMillis(stallToleranceSeconds);
                boolean progressMade = progress.progress() > lastProgress;

                // we store current progress if the task advanced or the tolerance exceeded
                if (progressMade || toleranceExceeded) {
                    progresses.add(progress);
                    lastProgressTimestamp = progress.timestamp();
                    lastProgress = progress.progress();
                }

                // if the task exceeded stall tolerance, we fail and log the history of the progress changes
                if (toleranceExceeded && !progressMade) {
                    StringBuilder sb = new StringBuilder("Stall tolerance " + stallToleranceSeconds
                            + " seconds has been exceeded without completing the task. Track of progress:\n");
                    for (TaskProgress historicProgress : progresses) {
                        long elapsedMillis = historicProgress.timestamp() - taskStartTimestamp;
                        String elapsedMillisPadded = String.format("%1$5s", elapsedMillis);
                        sb.append("\t")
                                .append(elapsedMillisPadded).append("ms: ")
                                .append(historicProgress.getProgressString())
                                .append("\n");
                    }
                    LOGGER.severe(sb.toString());
                    fail("Stall tolerance " + stallToleranceSeconds
                            + " seconds has been exceeded without completing the task. "
                            + (message != null ? message : ""));
                }

                sleepMillis(sleepMillis);
            } catch (Exception e) {
                throw rethrow(e);
            }
        }
    }

    public static void assertTrueEventually(AssertTask task) {
        assertTrueEventually(null, task, ASSERT_TRUE_EVENTUALLY_TIMEOUT);
    }

    public static void assertFalseEventually(AssertTask task, long timeoutSeconds) {
        // we are going to check five times a second
        int sleepMillis = 200;
        long iterations = timeoutSeconds * 1000 / sleepMillis;
        long deadline = System.currentTimeMillis() + SECONDS.toMillis(timeoutSeconds);
        for (int i = 0; i < iterations && System.currentTimeMillis() < deadline; i++) {
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
     * In addition, it formats the given string with those values to provide a good assert message.
     *
     * @param message  assert message which is formatted with expected and actual values
     * @param expected expected value which is used for assert
     * @param actual   actual value which is used for assert
     */
    public static void assertEqualsStringFormat(String message, Object expected, Object actual) {
        assertThat(actual).as(message, expected, actual)
                .isEqualTo(expected);
    }

    /**
     * This method executes the normal assertNotEquals with expected and actual values.
     * In addition, it formats the given string with those values to provide a good assert message.
     *
     * @param message  assert message which is formatted with expected and actual values
     * @param expected expected value which is used for assert
     * @param actual   actual value which is used for assert
     */
    public static void assertNotEqualsStringFormat(String message, Object expected, Object actual) {
        assertThat(actual).as(message, expected, actual)
                .isNotEqualTo(expected);
    }

    /** @see org.assertj.core.api.AbstractLongAssert#isBetween(Long, Long) */
    public static void assertBetween(String label, long actualValue, long lowerBound, long upperBound) {
        assertThat(actualValue).as(label).isBetween(lowerBound, upperBound);
    }

    public static void assertGreaterOrEquals(String label, long actualValue, long lowerBound) {
        assertThat(actualValue).as(label).isGreaterThanOrEqualTo(lowerBound);
    }

    public static void assertExactlyOneSuccessfulRun(AssertTask task) {
        assertExactlyOneSuccessfulRun(task, ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS);
    }

    public static void assertExactlyOneSuccessfulRun(AssertTask task, int giveUpTime, TimeUnit timeUnit) {
        long timeout = System.currentTimeMillis() + timeUnit.toMillis(giveUpTime);
        RuntimeException lastException = new RuntimeException("Did not try even once");
        while (System.currentTimeMillis() < timeout) {
            try {
                task.run();
                return;
            } catch (Exception e) {
                lastException = e instanceof RuntimeException re ? re : new RuntimeException(e);
            }
            try {
                //noinspection BusyWait
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
        assertTrueEventually(() -> assertEquals(expectedOpsCount, waitNotifyService.getTotalParkedOperationCount()));
    }

    @SuppressWarnings("unchecked")
    public static <T extends Throwable> T assertThrows(Class<T> expectedType, ThrowingRunnable r) {
        try {
            r.run();
        } catch (Throwable actualException) {
            if (expectedType.isInstance(actualException)) {
                return (T) actualException;
            } else {
                String excMsg = String.format("Unexpected %s exception type thrown with message:\n%s",
                        actualException.getClass().getName(), actualException.getMessage());
                throw new AssertionFailedError(excMsg);
            }
        }

        String excMsg = String.format("Expected %s to be thrown, but nothing was thrown.", expectedType.getName());
        throw new AssertionFailedError(excMsg);
    }

    protected static OperationParkerImpl getOperationParkingService(HazelcastInstance instance) {
        return (OperationParkerImpl) Accessors.getNodeEngineImpl(instance).getOperationParker();
    }

    public static void assertThatIsNotMultithreadedTest() {
        assertThat(Thread.currentThread()).as("Test cannot run with parallel runner")
                .isNotInstanceOf(MultithreadedTestRunnerThread.class);
    }

    // ###################################
    // ########## reflection utils #######
    // ###################################

    public static <T> T getFieldValue(Object object, String fieldName) {
        return getFieldValue(object.getClass(), object, fieldName);
    }

    public static <T> T getFieldValue(Class<?> clazz, String fieldName) {
        return getFieldValue(clazz, null, fieldName);
    }

    private static <T> T getFieldValue(Class<?> clazz, Object object, String fieldName) {
        try {
            Field field = clazz.getDeclaredField(fieldName);
            if (!field.canAccess(object)) {
                field.setAccessible(true);
            }
            return (T) field.get(object);
        } catch (Exception e) {
            throw sneakyThrow(e);
        }
    }

    /**
     * Returns the value of the {@link Category} annotation of the calling test class.
     * <p>
     * This method doesn't cover {@link Category} annotations on a test method.
     * It may also fail on test class hierarchies (the annotated class has to be in the stack trace).
     */
    public static Collection<Class<?>> getTestCategories() {
        Collection<Class<?>> testCategories = acceptOnStackTrace((frame, results) -> {
            try {
                Class<?> clazz = frame.getDeclaringClass();
                Category annotation = clazz.getAnnotation(Category.class);
                if (annotation != null) {
                    Collections.addAll(results, annotation.value());
                }
            } catch (Exception ignored) {
            }
        });
        if (testCategories.isEmpty()) {
            fail("Could not find any classes with a @Category annotation in the stack trace");
        }
        return testCategories;
    }

    // ###################################
    // ########## inner classes ##########
    // ###################################

    private interface Latch {

        boolean await(long timeout, TimeUnit unit) throws InterruptedException;

        long getCount();
    }

    private static class ICountdownLatchAdapter implements Latch {

        private final ICountDownLatch latch;

        ICountdownLatchAdapter(ICountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
            return latch.await(timeout, unit);
        }

        @Override
        public long getCount() {
            return latch.getCount();
        }
    }

    private static class CountdownLatchAdapter implements Latch {

        private final CountDownLatch latch;

        CountdownLatchAdapter(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
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
        try {
            NodeEngine nodeEngine = Accessors.getNode(instance).getNodeEngine();
            SerializationService ss = Accessors.getNode(instance).getSerializationService();
            int partitionId = nodeEngine.getPartitionService().getPartitionId(key);

            MapOperation get = getMapOperationProvider(instance, mapName).createGetOperation(mapName, ss.toData(key));
            get.setPartitionId(partitionId);
            get.setReplicaIndex(1);

            return Accessors.getNode(instance).getNodeEngine().getOperationService().invokeOnPartition(get).get();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    protected MapOperationProvider getMapOperationProvider(HazelcastInstance instance, String mapName) {
        MapService mapService = Accessors.getNodeEngineImpl(instance).getService(MapService.SERVICE_NAME);
        return mapService.getMapServiceContext().getMapOperationProvider(mapName);
    }

    // ######################################
    // ########## test assumptions ##########
    // ######################################

    public static void assumeHadoopSupportsIbmPlatform() {
        boolean missingIbmLoginModule = true;
        try {
            Class.forName("com.ibm.security.auth.module.JAASLoginModule");
            missingIbmLoginModule = false;
        } catch (ClassNotFoundException ignored) {
        }
        assumeFalse("Skipping due Hadoop authentication issues. See https://github.com/apache/hadoop/pull/4537",
                JAVA_VENDOR.contains("IBM") && missingIbmLoginModule);
    }

    public static void assumeThatNoWindowsOS() {
        assumeFalse("Skipping on Windows", OS.isWindows());
    }

    public static void assumeThatLinuxOS() {
        Assume.assumeTrue("Only Linux platform supported", OS.isLinux());
    }

    public static void assumeNoArm64Architecture() {
        Assume.assumeFalse("Not supported on arm64 (aarch64) architecture", "aarch64".equals(OS.osArch()));
    }

    /**
     * Throws {@link AssumptionViolatedException} if two new Objects have the same hashCode,
     * e.g. when running tests with static hashCode ({@code -XX:hashCode=2}).
     */
    public static void assumeDifferentHashCodes() {
        assumeTrue("Hash codes are equal for different objects", EXPECT_DIFFERENT_HASHCODES);
    }

    /**
     * Throws {@link AssumptionViolatedException} if the given {@link InternalSerializationService}
     * is not configured with the assumed {@link ByteOrder}.
     *
     * @param serializationService the {@link InternalSerializationService} to check
     * @param assumedByteOrder     the assumed {@link ByteOrder}
     */
    public static void assumeConfiguredByteOrder(InternalSerializationService serializationService,
                                                 ByteOrder assumedByteOrder) {
        ByteOrder configuredByteOrder = serializationService.getByteOrder();
        assumeThat(configuredByteOrder)
                .as("Assumed configured byte order %s, but was %s", assumedByteOrder, configuredByteOrder)
                .isEqualTo(assumedByteOrder);
    }

    /**
     * Walk the stack trace and execute the provided {@code BiConsumer} on each {@code StackTraceElement}
     * encountered while walking the stack trace.
     * <p>
     * The {@code BiConsumer} expects {@code StackTraceElement, List<V>} arguments; any
     * result from the {@code BiConsumer} should be added to the {@code results} list which is
     * returned as the result of this method.
     */
    private static <V> Collection<V> acceptOnStackTrace(BiConsumer<StackFrame, Collection<V>> consumer) {
        Collection<V> results = new ArrayList<>();
        StackWalker.getInstance(Option.RETAIN_CLASS_REFERENCE)
                .forEach(stackFrame -> consumer.accept(stackFrame, results));
        return results;
    }

    public static void destroyAllDistributedObjects(HazelcastInstance hz) {
        Collection<DistributedObject> distributedObjects = hz.getDistributedObjects();
        for (DistributedObject object : distributedObjects) {
            object.destroy();
        }
    }

    /**
     * Returns raw byte[] of supplied file.
     *
     * @param testFile the file to get bytes from.
     * @return the raw byte contents.
     */
    protected static byte[] getTestFileBytes(File testFile) {
        try (InputStream is = testFile.toURI().toURL().openStream()) {
            return is.readAllBytes();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** Converts Windows line separators ({@code \r\n}) to {@code \r} */
    protected static String convertWindowsLineSeperators(String str) {
        return StringUtils.remove(str, StringUtils.CR);
    }
}
