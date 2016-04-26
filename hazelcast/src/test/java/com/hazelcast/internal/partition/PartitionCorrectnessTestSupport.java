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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.internal.partition.service.TestMigrationAwareService;
import com.hazelcast.internal.partition.service.TestIncrementOperation;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.Before;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.test.TestPartitionUtils.getReplicaVersions;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public abstract class PartitionCorrectnessTestSupport extends HazelcastTestSupport {

    private static final int PARTITION_COUNT = 111;
    private static final int PARALLEL_REPLICATIONS = 10;
    private static final int BACKUP_SYNC_INTERVAL = 1;

    TestHazelcastInstanceFactory factory;

    @Parameterized.Parameter(0)
    public int backupCount;

    @Parameterized.Parameter(1)
    public int nodeCount;

    @Before
    public void setup() {
        factory = createHazelcastInstanceFactory(10);
    }

    void fillData(HazelcastInstance hz) {
        NodeEngine nodeEngine = getNode(hz).nodeEngine;
        for (int i = 0; i < PARTITION_COUNT; i++) {
            nodeEngine.getOperationService().invokeOnPartition(null, new TestIncrementOperation(), i);
        }
    }

    Collection<HazelcastInstance> startNodes(final Config config, int count) throws InterruptedException {
        if (count == 1) {
            return Collections.singleton(factory.newHazelcastInstance(config));
        } else {
            Collection<HazelcastInstance> instances = new ArrayList<HazelcastInstance>(count);
            final Collection<HazelcastInstance> syncInstances = Collections.synchronizedCollection(instances);

            final CountDownLatch latch = new CountDownLatch(count);
            for (int i = 0; i < count; i++) {
                new Thread() {
                    public void run() {
                        HazelcastInstance instance = factory.newHazelcastInstance(config);
                        syncInstances.add(instance);
                        latch.countDown();
                    }
                }.start();
            }
            assertTrue(latch.await(2, TimeUnit.MINUTES));
            return instances;
        }
    }

    void startNodes(final Config config, Collection<Address> addresses) throws InterruptedException {
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

    Collection<Address> terminateNodes(int count) throws InterruptedException {
        List<HazelcastInstance> instances = new ArrayList<HazelcastInstance>(factory.getAllHazelcastInstances());
        assertThat(instances.size(), greaterThanOrEqualTo(count));

        Collections.shuffle(instances);

        if (count == 1) {
            HazelcastInstance hz = instances.get(0);
            Address address = getNode(hz).getThisAddress();
            TestUtil.terminateInstance(hz);
            return Collections.singleton(address);
        } else {
            final CountDownLatch latch = new CountDownLatch(count);
            Collection<Address> addresses = new HashSet<Address>();

            for (int i = 0; i < count; i++) {
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

    void assertSizeAndDataEventually() {
        assertTrueEventually(new AssertSizeAndDataTask());
    }

    void assertSizeAndData() throws InterruptedException {
        Collection<HazelcastInstance> instances = factory.getAllHazelcastInstances();
        final int actualBackupCount = Math.min(backupCount, instances.size() - 1);
        final int expectedSize = PARTITION_COUNT * (actualBackupCount + 1);

        int total = 0;
        for (HazelcastInstance hz : instances) {
            TestMigrationAwareService service = getService(hz);
            total += service.size();

            Node node = getNode(hz);
            InternalPartitionService partitionService = node.getPartitionService();
            InternalPartition[] partitions = partitionService.getInternalPartitions();
            Address thisAddress = node.getThisAddress();

            // find leaks
            for (Integer p : service.keys()) {
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
                            service.contains(partition.getPartitionId()));
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
                        assertNotNull("Versions null on " + backupNode.address + ", partitionId: " + partitionId, backupReplicaVersions);

                        for (int i = replica - 1; i < actualBackupCount; i++) {
                            assertEquals("Replica version mismatch! Owner: " + thisAddress + ", Backup: " + address
                                            + ", Partition: " + partition + ", Replica: " + (i + 1) + " owner versions: "
                                            + Arrays.toString(replicaVersions) + " backup versions: " + Arrays.toString(backupReplicaVersions),
                                    replicaVersions[i], backupReplicaVersions[i]);
                        }

                        TestMigrationAwareService backupService = getService(backupInstance);
                        assertEquals("Wrong data! Partition: " + partitionId + ", replica: " + replica + " on "
                                        + address + " has stale value! " + Arrays.toString(backupReplicaVersions),
                                service.get(partitionId), backupService.get(partitionId));
                    }
                }
            }
        }
        assertEquals("Missing data!", expectedSize, total);
    }

    private TestMigrationAwareService getService(HazelcastInstance hz) {
        Node node = getNode(hz);
        return node.nodeEngine.getService(TestMigrationAwareService.SERVICE_NAME);
    }

    Config getConfig(boolean withService, boolean antiEntropyEnabled) {
        Config config = new Config();

        if (withService) {
            ServiceConfig serviceConfig = TestMigrationAwareService.createServiceConfig(backupCount);
            config.getServicesConfig().addServiceConfig(serviceConfig);
        }

        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), String.valueOf(PARTITION_COUNT));
        config.setProperty(GroupProperty.MIGRATION_MIN_DELAY_ON_MEMBER_REMOVED_SECONDS.getName(), String.valueOf(1));
        config.setProperty(GroupProperty.PARTITION_BACKUP_SYNC_INTERVAL.getName(), String.valueOf(BACKUP_SYNC_INTERVAL));

        int parallelReplications = antiEntropyEnabled ? PARALLEL_REPLICATIONS : 0;
        config.setProperty(GroupProperty.PARTITION_MAX_PARALLEL_REPLICATIONS.getName(), String.valueOf(parallelReplications));
        return config;
    }

    private class AssertSizeAndDataTask extends AssertTask {
        @Override
        public void run() throws Exception {
            assertSizeAndData();
        }
    }
}
