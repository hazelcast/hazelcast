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

package com.hazelcast.spi;

import com.hazelcast.config.Config;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.partition.MigrationEndpoint;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.instance.GroupProperties.PROP_PARTITION_COUNT;
import static com.hazelcast.instance.GroupProperties.PROP_PARTITION_MAX_PARALLEL_REPLICATIONS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class MigrationAwareServiceTest extends HazelcastTestSupport {

    private static final String BACKUP_COUNT_PROP = "backups.count";
    private static final int PARTITION_COUNT = 271;
    private static final int PARALLEL_REPLICATIONS = PARTITION_COUNT / 3;

    private TestHazelcastInstanceFactory factory;

    @Before
    public void setup() {
        factory = createHazelcastInstanceFactory(10);
    }

    @Test
    public void testPartitionDataSize_whenNodesStartedSequentially_withSingleBackup() throws InterruptedException {
       testPartitionDataSize_whenNodesStartedSequentially(1);
    }

    @Test
    public void testPartitionDataSize_whenNodesStartedSequentially_withTwoBackups() throws InterruptedException {
        testPartitionDataSize_whenNodesStartedSequentially(2);
    }

    @Test
    public void testPartitionDataSize_whenNodesStartedSequentially_withThreeBackups() throws InterruptedException {
        testPartitionDataSize_whenNodesStartedSequentially(3);
    }

    private void testPartitionDataSize_whenNodesStartedSequentially(int backupCount) throws InterruptedException {
        Config config = getConfig(backupCount);

        HazelcastInstance hz = factory.newHazelcastInstance(config);
        fill(hz);
        assertSize(backupCount);

        for (int i = 1; i < backupCount + 3; i++) {
            startNodes(config, 1);
            assertSize(backupCount);
        }
    }

    @Test
    public void testPartitionDataSize_whenNodesStartedParallel_withSingleBackup() throws InterruptedException {
        testPartitionDataSize_whenNodesStartedParallel(1);
    }

    @Test
    public void testPartitionDataSize_whenNodesStartedParallel_withTwoBackups() throws InterruptedException {
        testPartitionDataSize_whenNodesStartedParallel(2);
    }

    @Test
    public void testPartitionDataSize_whenNodesStartedParallel_withThreeBackups() throws InterruptedException {
        testPartitionDataSize_whenNodesStartedParallel(3);
    }

    private void testPartitionDataSize_whenNodesStartedParallel(int backupCount) throws InterruptedException {
        Config config = getConfig(backupCount);

        HazelcastInstance hz = factory.newHazelcastInstance(config);
        fill(hz);
        assertSize(backupCount);

        startNodes(config, backupCount + 3);
        assertSize(backupCount);
    }

    @Test
    public void testPartitionDataSize_whenBackupNodesTerminated_withSingleBackup() throws InterruptedException {
        testPartitionDataSize_whenBackupNodesTerminated(1);
    }

    @Test
    public void testPartitionDataSize_whenBackupNodesTerminated_withTwoBackups() throws InterruptedException {
        testPartitionDataSize_whenBackupNodesTerminated(2);
    }

    @Test
    public void testPartitionDataSize_whenBackupNodesTerminated_withThreeBackups() throws InterruptedException {
        testPartitionDataSize_whenBackupNodesTerminated(3);
    }

    private void testPartitionDataSize_whenBackupNodesTerminated(int backupCount) throws InterruptedException {
        Config config = getConfig(backupCount);

        startNodes(config, backupCount + 4);
        HazelcastInstance hz = factory.getAllHazelcastInstances().iterator().next();
        fill(hz);
        assertSize(backupCount);

        terminateNodes(backupCount);
        assertSize(backupCount);
    }

    @Test
    public void testPartitionDataSize_whenNodeGracefullyShutdown() throws InterruptedException {
        Config config = getConfig(1);

        HazelcastInstance hz = factory.newHazelcastInstance(config);
        fill(hz);

        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        hz2.shutdown();

        assertSize(1);
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

    private void terminateNodes(int count) throws InterruptedException {
        List<HazelcastInstance> instances = new ArrayList<HazelcastInstance>(factory.getAllHazelcastInstances());
        Collections.shuffle(instances);

        if (count == 1) {
            TestUtil.terminateInstance(instances.get(0));
        } else {
            int min = Math.min(count, instances.size());
            final CountDownLatch latch = new CountDownLatch(min);

            for (int i = 0; i < min; i++) {
                final HazelcastInstance hz = instances.get(i);
                new Thread() {
                    public void run() {
                        TestUtil.terminateInstance(hz);
                        latch.countDown();
                    }
                }.start();
            }
            assertTrue(latch.await(2, TimeUnit.MINUTES));
        }
    }

    private void assertSize(final int backupCount) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                Collection<HazelcastInstance> instances = factory.getAllHazelcastInstances();
                int expectedSize = PARTITION_COUNT * Math.min(backupCount + 1, instances.size());

                int total = 0;
                for (HazelcastInstance hz : instances) {
                    SampleMigrationAwareService service = getService(hz);
                    total += service.size();
                }
                assertEquals(expectedSize, total);
            }
        });
    }

    private SampleMigrationAwareService getService(HazelcastInstance hz) {
        Node node = getNode(hz);
        return node.nodeEngine.getService(SampleMigrationAwareService.SERVICE_NAME);
    }

    private Config getConfig(int backupCount) {
        Config config = new Config();
        ServiceConfig serviceConfig = new ServiceConfig()
                .setEnabled(true).setName(SampleMigrationAwareService.SERVICE_NAME)
                .setClassName(SampleMigrationAwareService.class.getName())
                .addProperty(BACKUP_COUNT_PROP, String.valueOf(backupCount));

        config.getServicesConfig().addServiceConfig(serviceConfig);
        config.setProperty(PROP_PARTITION_COUNT, String.valueOf(PARTITION_COUNT));
        config.setProperty(PROP_PARTITION_MAX_PARALLEL_REPLICATIONS, String.valueOf(PARALLEL_REPLICATIONS));
        return config;
    }

    private static class SampleMigrationAwareService implements ManagedService, MigrationAwareService {

        static final String SERVICE_NAME = "SampleMigrationAwareService";

        private final ConcurrentMap<Integer, Object> data
                = new ConcurrentHashMap<Integer, Object>();

        private volatile int backupCount;
        private volatile NodeEngine nodeEngine;

        @Override
        public void init(NodeEngine nodeEngine, Properties properties) {
            this.nodeEngine = nodeEngine;
            backupCount = Integer.parseInt(properties.getProperty(BACKUP_COUNT_PROP, "1"));
        }

        @Override
        public void reset() {
        }

        @Override
        public void shutdown(boolean terminate) {
        }

        int size() {
            return data.size();
        }

        @Override
        public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
            if (event.getReplicaIndex() > backupCount) {
                return null;
            }
            if (!data.containsKey(event.getPartitionId())) {
                throw new HazelcastException("No data found for " + event);
            }
            return new SampleReplicationOperation();
        }

        @Override
        public void beforeMigration(PartitionMigrationEvent event) {
        }

        @Override
        public void commitMigration(PartitionMigrationEvent event) {
            if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
                data.remove(event.getPartitionId());
            }
        }

        @Override
        public void rollbackMigration(PartitionMigrationEvent event) {
            if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
                data.remove(event.getPartitionId());
            }
        }

        @Override
        public void clearPartitionReplica(int partitionId) {
            data.remove(partitionId);
        }
    }

    private static class SamplePutOperation extends AbstractOperation implements BackupAwareOperation {
        @Override
        public void run() throws Exception {
            SampleMigrationAwareService service = getService();
            service.data.put(getPartitionId(), Boolean.TRUE);
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
            return new SampleBackupPutOperation();
        }

        @Override
        public String getServiceName() {
            return SampleMigrationAwareService.SERVICE_NAME;
        }
    }

    private static class SampleBackupPutOperation extends AbstractOperation {
        @Override
        public void run() throws Exception {
            SampleMigrationAwareService service = getService();
            service.data.put(getPartitionId(), Boolean.TRUE);
        }

        @Override
        public String getServiceName() {
            return SampleMigrationAwareService.SERVICE_NAME;
        }
    }

    private static class SampleReplicationOperation extends AbstractOperation {

        public SampleReplicationOperation() {
        }

        @Override
        public void run() throws Exception {
            // artificial latency!
            randomLatency();
            SampleMigrationAwareService service = getService();
            service.data.put(getPartitionId(), Boolean.TRUE);
        }

        private void randomLatency() {
            long duration = (long) (Math.random() * 100);
            LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(duration) + 100);
        }

        @Override
        public String getServiceName() {
            return SampleMigrationAwareService.SERVICE_NAME;
        }
    }

}
