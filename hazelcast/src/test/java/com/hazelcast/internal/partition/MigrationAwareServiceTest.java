/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.partition;

import com.hazelcast.config.Config;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.internal.properties.GroupProperty;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.tcp.FirewallingMockConnectionManager;
import com.hazelcast.nio.tcp.PacketFilter;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.impl.SpiDataSerializerHook;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.spi.partition.MigrationEndpoint;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.test.TestPartitionUtils.getReplicaVersions;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class/*, ParallelTest.class*/})
//@Ignore // related issue https://github.com/hazelcast/hazelcast/issues/5444
public class MigrationAwareServiceTest extends HazelcastTestSupport {

    private static final String BACKUP_COUNT_PROP = "backups.count";
    private static final int PARTITION_COUNT = 111;
    private static final int PARALLEL_REPLICATIONS = 10;
    private static final int BACKUP_SYNC_INTERVAL = 1;
    private static final float BACKUP_BLOCK_RATIO = 0.65f;

    @Parameterized.Parameters(name = "backups:{0},nodes:{1}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][] {
                {1, 2},
                {1, InternalPartition.MAX_REPLICA_COUNT},
                {2, 3},
                {2, InternalPartition.MAX_REPLICA_COUNT},
                {3, 4},
                {3, InternalPartition.MAX_REPLICA_COUNT},
                {6, 10}
        });
    }

    private TestHazelcastInstanceFactory factory;

    @Parameterized.Parameter(0)
    public int backupCount;

    @Parameterized.Parameter(1)
    public int nodeCount;

    private boolean antiEntropyEnabled = false;
    private boolean withoutService = false;

    @Before
    public void setup() {
        factory = createHazelcastInstanceFactory(10);
    }

    @Test
    public void testPartitionData_whenNodesStartedSequentially() throws InterruptedException {
        Config config = getConfig(backupCount);

        HazelcastInstance hz = factory.newHazelcastInstance(config);
        fill(hz);
        assertSizeAndData();

        for (int i = 1; i <= nodeCount; i++) {
            startNodes(config, 1);
            assertSizeAndData();
        }
    }

    @Test
    public void testPartitionData_whenNodesStartedParallel() throws InterruptedException {
        Config config = getConfig(backupCount);

        HazelcastInstance hz = factory.newHazelcastInstance(config);
        fill(hz);
        assertSizeAndData();

        startNodes(config, nodeCount);
        assertSizeAndData();
    }

    @Test
    public void testPartitionData_whenBackupNodesTerminated() throws InterruptedException {
        Config config = getConfig(backupCount);

        HazelcastInstance hz = factory.newHazelcastInstance(config);
        startNodes(config, nodeCount);
        warmUpPartitions(factory.getAllHazelcastInstances());

        fill(hz);
        assertSizeAndData();

        terminateNodes(backupCount);
        assertSizeAndData();
    }

    @Test(timeout = 6000 * 10 * 10)
    public void testPartitionAssignments_whenNodesStartedTerminated() throws InterruptedException {
        withoutService = true;
        Config config = getConfig(backupCount);

        HazelcastInstance hz = factory.newHazelcastInstance(config);
        warmUpPartitions(hz);

        int size = 1;
        while (size < (nodeCount + 1)) {
            startNodes(config, backupCount + 1);
            size += (backupCount + 1);

            terminateNodes(backupCount);
            size -= backupCount;

            assertPartitionAssignments();
        }
    }

    @Test(timeout = 6000 * 10 * 10)
    public void testPartitionAssignments_whenNodesStartedTerminated_withRestart() throws InterruptedException {
        withoutService = true;
        Config config = getConfig(backupCount);

        HazelcastInstance hz = factory.newHazelcastInstance(config);
        warmUpPartitions(hz);

        Collection<Address> addresses = Collections.emptySet();

        int size = 1;
        while (size < (nodeCount + 1)) {
            int startCount = (backupCount + 1) - addresses.size();
            startNodes(config, addresses);
            startNodes(config, startCount);
            size += (backupCount + 1);

            addresses = terminateNodes(backupCount);
            size -= backupCount;

            assertPartitionAssignments();
        }
    }

    @Test(timeout = 6000 * 10 * 10)
    public void testPartitionData_whenBackupNodesStartedTerminated() throws InterruptedException {
        Config config = getConfig(backupCount);

        HazelcastInstance hz = factory.newHazelcastInstance(config);
        fill(hz);
        assertSizeAndData();

        int size = 1;
        while (size < (nodeCount + 1)) {
            startNodes(config, backupCount + 1);
            size += (backupCount + 1);

            assertSizeAndData();

            terminateNodes(backupCount);
            size -= backupCount;

            assertSizeAndData();
        }
    }

    private void assertPartitionAssignments() {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                Collection<HazelcastInstance> instances = factory.getAllHazelcastInstances();
                final int actualBackupCount = Math.min(backupCount, instances.size() - 1);

                for (HazelcastInstance hz : instances) {
                    Node node = getNode(hz);
                    InternalPartitionService partitionService = node.getPartitionService();
                    InternalPartition[] partitions = partitionService.getInternalPartitions();

                    for (InternalPartition partition : partitions) {
                        for (int i = 0; i <= actualBackupCount; i++) {
                            Address replicaAddress = partition.getReplicaAddress(i);
                            assertNotNull("Replica " + i + " is not found in " + partition, replicaAddress);
                            assertTrue("Not member: " + replicaAddress, node.getClusterService().getMember(replicaAddress) != null);
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testPartitionData_withAntiEntropy() throws InterruptedException {
        antiEntropyEnabled = true;

        HazelcastInstance[] instances = factory.newInstances(getConfig(backupCount), nodeCount);
        for (HazelcastInstance instance : instances) {
            Node node = getNode(instance);
            FirewallingMockConnectionManager cm = (FirewallingMockConnectionManager) node.getConnectionManager();
            cm.setPacketFilter(new BackupPacketFilter(node.getSerializationService(), BACKUP_BLOCK_RATIO));
        }
        warmUpPartitions(instances);

        for (HazelcastInstance instance : instances) {
            fill(instance);
        }

        assertSizeAndData();
    }

    private void fill(HazelcastInstance hz) {
        NodeEngine nodeEngine = getNode(hz).nodeEngine;
        for (int i = 0; i < PARTITION_COUNT; i++) {
            nodeEngine.getOperationService().invokeOnPartition(null, new SamplePutOperation(), i);
        }
    }

    private void startNodes(final Config config, int count) throws InterruptedException {
        if (count == 1) {
            factory.newHazelcastInstance(config);
        } else {
            final CountDownLatch latch = new CountDownLatch(count);
            for (int i = 0; i < count; i++) {
                new Thread() {
                    public void run() {
                        factory.newHazelcastInstance(config);
                        latch.countDown();
                    }
                }.start();
            }
            assertTrue(latch.await(2, TimeUnit.MINUTES));
        }
    }

    private void startNodes(final Config config, Collection<Address> addresses) throws InterruptedException {
        if (addresses.size() == 1) {
            factory.newHazelcastInstance(addresses.iterator().next(), config);
        } else {
            final CountDownLatch latch = new CountDownLatch(addresses.size());
            for (final Address address : addresses) {
                new Thread() {
                    public void run() {
                        factory.newHazelcastInstance(address, config);
                        latch.countDown();
                    }
                }.start();
            }
            assertTrue(latch.await(2, TimeUnit.MINUTES));
        }
    }

    private Collection<Address> terminateNodes(int count) throws InterruptedException {
        List<HazelcastInstance> instances = new ArrayList<HazelcastInstance>(factory.getAllHazelcastInstances());
        Collections.shuffle(instances);

        if (count == 1) {
            HazelcastInstance hz = instances.get(0);
            Address address = getNode(hz).getThisAddress();
            TestUtil.terminateInstance(hz);
            return Collections.singleton(address);
        } else {
            int min = Math.min(count, instances.size());
            final CountDownLatch latch = new CountDownLatch(min);
            Collection<Address> addresses = new HashSet<Address>();

            for (int i = 0; i < min; i++) {
                final HazelcastInstance hz = instances.get(i);
                addresses.add(getNode(hz).getThisAddress());

                new Thread() {
                    public void run() {
                        TestUtil.terminateInstance(hz);
                        latch.countDown();
                    }
                }.start();
            }
            assertTrue(latch.await(2, TimeUnit.MINUTES));
            return addresses;
        }
    }

    private void assertSizeAndData() {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                Collection<HazelcastInstance> instances = factory.getAllHazelcastInstances();
                final int actualBackupCount = Math.min(backupCount, instances.size() - 1);
                final int expectedSize = PARTITION_COUNT * (actualBackupCount + 1);

                int total = 0;
                for (HazelcastInstance hz : instances) {
                    SampleMigrationAwareService service = getService(hz);
                    total += service.size();

                    Node node = getNode(hz);
                    InternalPartitionService partitionService = node.getPartitionService();
                    InternalPartition[] partitions = partitionService.getInternalPartitions();
                    Address thisAddress = node.getThisAddress();

                    // find leaks
                    for (Integer p : service.data.keySet()) {
                        int replicaIndex = partitions[p].getReplicaIndex(thisAddress);
                        assertThat("Partition: " + p + " is leaking on " + thisAddress,
                                replicaIndex, allOf(greaterThanOrEqualTo(0), lessThanOrEqualTo(backupCount)));
                    }

                    // find missing
                    for (InternalPartition partition : partitions) {
                        int replicaIndex = partition.getReplicaIndex(thisAddress);
                        if (replicaIndex >= 0 && replicaIndex <= backupCount) {
                            assertTrue("Partition: " + partition.getPartitionId() + ", replica: " + replicaIndex
                                    + " data is missing on " + thisAddress,
                                    service.data.containsKey(partition.getPartitionId()));
                        }
                    }

                    // check values
                    for (InternalPartition partition : partitions) {
                        if (partition.isLocal()) {
                            int partitionId = partition.getPartitionId();
                            long[] replicaVersions = getReplicaVersions(node, partitionId);

                            for (int replica = 1; replica <= actualBackupCount; replica++) {
                                Address address = partition.getReplicaAddress(replica);
                                assertNotNull("Replica: " + replica + " is not found in " + partition, address);

                                HazelcastInstance backupInstance = factory.getInstance(address);
                                assertNotNull("Instance for " + address + " is not found! -> " + partition, backupInstance);

                                Node backupNode = getNode(backupInstance);
                                assertNotNull(backupNode);

                                long[] backupReplicaVersions = getReplicaVersions(backupNode, partitionId);
                                for (int i = replica - 1; i < actualBackupCount; i++) {
                                    assertEquals("Replica version mismatch! Owner: " + thisAddress + ", Backup: " + address
                                            + ", Partition: " + partition + ", Replica: " + (i + 1) + " owner versions: " + Arrays.toString(replicaVersions) + " backup versions: " + Arrays.toString(backupReplicaVersions),
                                            replicaVersions[i], backupReplicaVersions[i]);
                                }

                                SampleMigrationAwareService backupService = getService(backupInstance);
                                assertEquals("Wrong data! Partition: " + partitionId + ", replica: " + replica + " on "
                                        + address + " has stale value! " + Arrays.toString(backupReplicaVersions),
                                        service.data.get(partitionId), backupService.data.get(partitionId));
                            }
                        }
                    }
                }
                assertEquals("Missing data!", expectedSize, total);
            }
        });
    }

    private SampleMigrationAwareService getService(HazelcastInstance hz) {
        Node node = getNode(hz);
        return node.nodeEngine.getService(SampleMigrationAwareService.SERVICE_NAME);
    }

    private Config getConfig(int backupCount) {
        Config config = new Config();

        if (!withoutService) {
            ServiceConfig serviceConfig = new ServiceConfig()
                    .setEnabled(true).setName(SampleMigrationAwareService.SERVICE_NAME)
                    .setClassName(SampleMigrationAwareService.class.getName())
                    .addProperty(BACKUP_COUNT_PROP, String.valueOf(backupCount));
            config.getServicesConfig().addServiceConfig(serviceConfig);
        }

        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), String.valueOf(PARTITION_COUNT));
        config.setProperty(GroupProperty.MIGRATION_MIN_DELAY_ON_MEMBER_REMOVED_SECONDS.getName(), String.valueOf(1));
        config.setProperty(GroupProperty.PARTITION_BACKUP_SYNC_INTERVAL.getName(), String.valueOf(BACKUP_SYNC_INTERVAL));

        config.setProperty("hazelcast.logging.type", "log4j");

        int parallelReplications = antiEntropyEnabled ? PARALLEL_REPLICATIONS : 0;
        config.setProperty(GroupProperty.PARTITION_MAX_PARALLEL_REPLICATIONS.getName(), String.valueOf(parallelReplications));
        return config;
    }

    private static class SampleMigrationAwareService implements ManagedService, MigrationAwareService {

        private static final String SERVICE_NAME = "SampleMigrationAwareService";

        private final ConcurrentMap<Integer, Integer> data = new ConcurrentHashMap<Integer, Integer>();

        private volatile int backupCount;

        private volatile ILogger logger;

        @Override
        public void init(NodeEngine nodeEngine, Properties properties) {
            backupCount = Integer.parseInt(properties.getProperty(BACKUP_COUNT_PROP, "1"));
            logger = nodeEngine.getLogger(getClass());
        }

        @Override
        public void reset() {
        }

        @Override
        public void shutdown(boolean terminate) {
        }

        int inc(int partitionId) {
            Integer count = data.get(partitionId);
            if (count == null) {
                count = 1;
            } else {
                count++;
            }
            data.put(partitionId, count);
            return count;
        }

        int size() {
            return data.size();
        }

        @Override
        public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
//            logger.info(event.toString() + "\n");
            if (event.getReplicaIndex() > backupCount) {
                return null;
            }
            if (!data.containsKey(event.getPartitionId())) {
                throw new HazelcastException("No data found for " + event);
            }
            return new SampleReplicationOperation(data.get(event.getPartitionId()));
        }

        @Override
        public void beforeMigration(PartitionMigrationEvent event) {
        }

        @Override
        public void commitMigration(PartitionMigrationEvent event) {
//            logger.info("COMMIT: " + event.toString() + "\n");
            if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
                if (event.getNewReplicaIndex() == -1 || event.getNewReplicaIndex() > backupCount) {
                    data.remove(event.getPartitionId());
                }
            }
            if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
                if (event.getNewReplicaIndex() > backupCount) {
                    assertNull(data.get(event.getPartitionId()));
                }
            }
        }

        @Override
        public void rollbackMigration(PartitionMigrationEvent event) {
//            logger.info("ROLLBACK: " + event.toString() + "\n");
            if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
                if (event.getCurrentReplicaIndex() == -1 || event.getCurrentReplicaIndex() > backupCount) {
                    data.remove(event.getPartitionId());
                }
            }
        }

        @Override
        public void clearPartitionReplica(int partitionId) {
//            logger.info("CLEAR: " + partitionId);
//            new UnsupportedOperationException().printStackTrace();
            data.remove(partitionId);
        }
    }

    private static class SamplePutOperation extends AbstractOperation implements BackupAwareOperation {

        int value;

        public SamplePutOperation() {
        }

        @Override
        public void run() throws Exception {
            SampleMigrationAwareService service = getService();
            value = service.inc(getPartitionId());
        }

        @Override
        public boolean shouldBackup() {
            return true;
        }

        @Override
        public int getSyncBackupCount() {
            SampleMigrationAwareService service = getService();
            return service.backupCount;
        }

        @Override
        public int getAsyncBackupCount() {
            return 0;
        }

        @Override
        public Operation getBackupOperation() {
            return new SampleBackupPutOperation(value);
        }

        @Override
        public String getServiceName() {
            return SampleMigrationAwareService.SERVICE_NAME;
        }
    }

    private static class SampleBackupPutOperation extends AbstractOperation {

        int value;

        public SampleBackupPutOperation() {
        }

        public SampleBackupPutOperation(int value) {
            this.value = value;
        }

        @Override
        public void run() throws Exception {
            SampleMigrationAwareService service = getService();
            service.data.put(getPartitionId(), value);
        }

        @Override
        public String getServiceName() {
            return SampleMigrationAwareService.SERVICE_NAME;
        }

        @Override
        protected void writeInternal(ObjectDataOutput out) throws IOException {
            super.writeInternal(out);
            out.writeInt(value);
        }

        @Override
        protected void readInternal(ObjectDataInput in) throws IOException {
            super.readInternal(in);
            value = in.readInt();
        }
    }

    private static class SampleReplicationOperation extends SampleBackupPutOperation {

        public SampleReplicationOperation() {
        }

        public SampleReplicationOperation(int value) {
            super(value);
        }
    }

    private static class BackupPacketFilter implements PacketFilter {
        final InternalSerializationService serializationService;
        final float blockRatio;

        public BackupPacketFilter(InternalSerializationService serializationService, float blockRatio) {
            this.serializationService = serializationService;
            this.blockRatio = blockRatio;
        }

        @Override
        public boolean allow(Packet packet, Address endpoint) {
            return !packet.isFlagSet(Packet.FLAG_OP) || allowOperation(packet);
        }

        private boolean allowOperation(Packet packet) {
            try {
                ObjectDataInput input = serializationService.createObjectDataInput(packet);
                boolean identified = input.readBoolean();
                if (identified) {
                    int factory = input.readInt();
                    int type = input.readInt();
                    boolean isBackup = factory == SpiDataSerializerHook.F_ID && type == SpiDataSerializerHook.BACKUP;
                    return !isBackup || Math.random() > blockRatio;
                }
            } catch (IOException e) {
                throw new HazelcastException(e);
            }
            return true;
        }
    }
}
