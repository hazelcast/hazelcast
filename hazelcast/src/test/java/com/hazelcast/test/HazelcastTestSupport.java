/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.test.TestHazelcastFactory;
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
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.NodeExtension;
import com.hazelcast.instance.impl.TestUtil;
import com.hazelcast.internal.TestSupport;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.partition.IPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.impl.PartitionServiceState;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.server.ServerConnectionManager;
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
import org.junit.After;
import org.junit.AssumptionViolatedException;
import org.junit.ClassRule;
import org.junit.Rule;

import java.nio.ByteOrder;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.partition.TestPartitionUtils.getPartitionServiceState;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static com.hazelcast.test.TestEnvironment.isRunningCompatibilityTest;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
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
public abstract class HazelcastTestSupport extends TestSupport {
    private static final String COMPAT_HZ_INSTANCE_FACTORY = "com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory";

    @Rule
    public MetricsRule metricsRule = new MetricsRule();

    @ClassRule
    public static MobyNamingRule mobyNamingRule = new MobyNamingRule();
    private TestHazelcastInstanceFactory factory;

    @Rule
    public DumpBuildInfoOnFailureRule dumpInfoRule = new DumpBuildInfoOnFailureRule();

    static {
        ClusterProperty.METRICS_COLLECTION_FREQUENCY.setSystemProperty("1");
        ClusterProperty.METRICS_DEBUG.setSystemProperty("true");
    }

    @After
    public final void shutdownNodeFactory() {
        TestHazelcastInstanceFactory testHazelcastInstanceFactory = factory;
        if (testHazelcastInstanceFactory != null) {
            factory = null;
            testHazelcastInstanceFactory.terminateAll();
        }
    }

    // #####################################
    // ########## generic utility ##########
    // #####################################

    protected void checkThreadLocalsForLeaks() throws Exception {
        ThreadLocalLeakTestUtils.checkThreadLocalsForLeaks(getClass().getClassLoader());
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

    protected void assertNoRunningInstancesEventually(String methodName, TestHazelcastFactory hazelcastFactory) {
        // check for running Hazelcast instances
        assertTrueEventually(() -> {
            Collection<HazelcastInstance> instances = hazelcastFactory.getAllHazelcastInstances();
            if (!instances.isEmpty()) {
                fail("After " + methodName + " following instances haven't been shut down: " + instances);
            }
        });
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
        Cluster cluster = instance.getCluster();
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
        Cluster cluster = instance.getCluster();
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
        assertTrueEventually(() -> assertTrue(isInstanceInSafeState(instance)));
    }

    public static void waitClusterForSafeState(final HazelcastInstance instance) {
        assertTrueEventually(() -> assertTrue(isClusterInSafeState(instance)));
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
                nonSafeStates.put(Accessors.getAddress(node), state);
            }
        }

        assertTrue("Instances not in safe state! " + nonSafeStates, nonSafeStates.isEmpty());
    }

    public static void assertNoRunningInstances() {
        assertThat(Hazelcast.getAllHazelcastInstances()).as("There should be no running instances").isEmpty();
    }

    public static void assertNoRunningClientInstances() {
        assertThat(HazelcastClient.getAllHazelcastClients()).as("There should be no running client instances").isEmpty();
    }

    public static void assertNodeStarted(HazelcastInstance instance) {
        NodeExtension nodeExtension = Accessors.getNode(instance).getNodeExtension();
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
        Set<Member> members = instance.getCluster().getMembers();
        return members == null ? 0 : members.size();
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

    public static void assertMasterAddressEventually(final Address masterAddress, final HazelcastInstance... instances) {
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

    public static void assertClusterStateEventually(final ClusterState expectedState, final HazelcastInstance... instances) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertClusterState(expectedState, instances);
            }
        });
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

    private static OperationParkerImpl getOperationParkingService(HazelcastInstance instance) {
        return (OperationParkerImpl) Accessors.getNodeEngineImpl(instance).getOperationParker();
    }

    public static void assertThatIsNotMultithreadedTest() {
        assertFalse("Test cannot run with parallel runner", Thread.currentThread() instanceof MultithreadedTestRunnerThread);
    }

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
        return readFromMapBackup(instance, mapName, key, 1);
    }

    protected Object readFromMapBackup(HazelcastInstance instance, String mapName, Object key, int replicaIndex) {
        try {
            NodeEngine nodeEngine = Accessors.getNode(instance).getNodeEngine();
            SerializationService ss = Accessors.getNode(instance).getSerializationService();
            int partitionId = nodeEngine.getPartitionService().getPartitionId(key);

            MapOperation get = getMapOperationProvider(instance, mapName).createGetOperation(mapName, ss.toData(key));
            get.setPartitionId(partitionId);
            get.setReplicaIndex(replicaIndex);

            return Accessors.getNode(instance).getNodeEngine().getOperationService().invokeOnPartition(get).get();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    protected MapOperationProvider getMapOperationProvider(HazelcastInstance instance, String mapName) {
        MapService mapService = Accessors.getNodeEngineImpl(instance).getService(MapService.SERVICE_NAME);
        return mapService.getMapServiceContext().getMapOperationProvider(mapName);
    }

    /**
     * Throws {@link AssumptionViolatedException} if the given {@link InternalSerializationService} is not configured
     * with the assumed {@link ByteOrder}.
     *
     * @param serializationService the {@link InternalSerializationService} to check
     * @param assumedByteOrder     the assumed {@link ByteOrder}
     */
    public static void assumeConfiguredByteOrder(InternalSerializationService serializationService,
                                                 ByteOrder assumedByteOrder) {
        ByteOrder configuredByteOrder = serializationService.getByteOrder();
        assumeTrue(format("Assumed configured byte order %s, but was %s", assumedByteOrder, configuredByteOrder),
                configuredByteOrder.equals(assumedByteOrder));
    }

    public static void destroyAllDistributedObjects(HazelcastInstance hz) {
        Collection<DistributedObject> distributedObjects = hz.getDistributedObjects();
        for (DistributedObject object : distributedObjects) {
            object.destroy();
        }
    }

}
