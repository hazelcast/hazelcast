/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.partition.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigAccessor;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.partition.PartitionMigrationEvent;
import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.internal.partition.PartitionReplicaVersionsView;
import com.hazelcast.internal.partition.service.TestGetOperation;
import com.hazelcast.internal.partition.service.TestIncrementOperation;
import com.hazelcast.internal.partition.service.TestMigrationAwareService;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.ExpectedRuntimeException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.partition.MigrationEndpoint.DESTINATION;
import static com.hazelcast.internal.partition.MigrationEndpoint.SOURCE;
import static com.hazelcast.internal.partition.TestPartitionUtils.getDefaultReplicaVersions;
import static com.hazelcast.internal.partition.TestPartitionUtils.getPartitionReplicaVersionsView;
import static com.hazelcast.internal.partition.impl.MigrationCommitTest.resetInternalMigrationListener;
import static com.hazelcast.test.Accessors.getAddress;
import static com.hazelcast.test.Accessors.getNode;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static com.hazelcast.test.Accessors.getOperationService;
import static com.hazelcast.test.Accessors.getPartitionService;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MigrationCommitServiceTest extends HazelcastTestSupport {

    private static final int NODE_COUNT = 4;
    private static final int PARTITION_COUNT = 10;
    private static final int BACKUP_COUNT = 6;
    private static final int PARTITION_ID_TO_MIGRATE = 0;

    private volatile CountDownLatch blockMigrationStartLatch;

    private TestHazelcastInstanceFactory factory;

    private HazelcastInstance[] instances;

    @Before
    public void setup() throws Exception {
        blockMigrationStartLatch = new CountDownLatch(1);
        factory = createHazelcastInstanceFactory(NODE_COUNT);
        instances = factory.newInstances(createConfig(), NODE_COUNT);
        warmUpPartitions(instances);
        waitAllForSafeState(instances);

        OperationServiceImpl operationService = getOperationService(instances[0]);
        for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
            operationService.invokeOnPartition(null, new TestIncrementOperation(), partitionId).get();
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
                    InternalPartitionService partitionService = getPartitionService(instances[0]);
                    InternalPartition partition = partitionService.getPartition(partitionId);

                    // given that NODE_COUNT < BACKUP_COUNT, there can be at most
                    // NODE_COUNT replicas overall.
                    final int replicasCount = Math.min(BACKUP_COUNT + 1, NODE_COUNT);
                    for (int i = 0; i < replicasCount; i++) {
                        // replica assignment should complete, so that when we later
                        // reference a replica, we do not get NPE.
                        Address replicaAddress = partition.getReplicaAddress(i);
                        assertNotNull(replicaAddress);

                        TestMigrationAwareService service = getService(replicaAddress);
                        assertNotNull(service.get(partitionId));
                    }
                }
            }
        });

        for (HazelcastInstance instance : instances) {
            TestMigrationAwareService service = getNodeEngineImpl(instance).getService(TestMigrationAwareService.SERVICE_NAME);
            service.clearEvents();
        }
    }

    @After
    public void after() {
        blockMigrationStartLatch.countDown();
    }

    @Test
    public void testPartitionOwnerMoveCommit() throws Exception {
        int replicaIndexToClear = NODE_COUNT - 1;
        int replicaIndexToMigrate = 0;
        testSuccessfulMoveMigration(PARTITION_ID_TO_MIGRATE, replicaIndexToClear, replicaIndexToMigrate);
    }

    @Test
    public void testPartitionOwnerMoveRollback() throws Exception {
        int replicaIndexToClear = NODE_COUNT - 1;
        int replicaIndexToMigrate = 0;
        testFailedMoveMigration(PARTITION_ID_TO_MIGRATE, replicaIndexToClear, replicaIndexToMigrate);
    }

    @Test
    public void testPartitionBackupMoveCommit() throws Exception {
        int replicaIndexToClear = NODE_COUNT - 1;
        int replicaIndexToMigrate = NODE_COUNT - 2;
        testSuccessfulMoveMigration(PARTITION_ID_TO_MIGRATE, replicaIndexToClear, replicaIndexToMigrate);
    }

    @Test
    public void testPartitionBackupMoveRollback() throws Exception {
        int replicaIndexToClear = NODE_COUNT - 1;
        int replicaIndexToMigrate = NODE_COUNT - 2;
        testFailedMoveMigration(PARTITION_ID_TO_MIGRATE, replicaIndexToClear, replicaIndexToMigrate);
    }

    private void testSuccessfulMoveMigration(int partitionId, int replicaIndexToClear, int replicaIndexToMigrate)
            throws Exception {
        PartitionReplica destination = clearReplicaIndex(partitionId, replicaIndexToClear);
        MigrationInfo migration = createMoveMigration(partitionId, replicaIndexToMigrate, destination);

        migrateWithSuccess(migration);

        assertMigrationSourceCommit(migration);
        assertMigrationDestinationCommit(migration);

        assertPartitionDataAfterMigrations();
    }

    private void testFailedMoveMigration(int partitionId, int replicaIndexToClear, int replicaIndexToMigrate) throws Exception {
        PartitionReplica destination = clearReplicaIndex(partitionId, replicaIndexToClear);
        MigrationInfo migration = createMoveMigration(partitionId, replicaIndexToMigrate, destination);

        migrateWithFailure(migration);

        assertMigrationSourceRollback(migration);
        assertMigrationDestinationRollback(migration);

        assertPartitionDataAfterMigrations();
    }

    @Test
    public void testPartitionOwnerShiftDownCommit() throws Exception {
        int oldReplicaIndex = 0;
        int replicaIndexToClear = NODE_COUNT - 1;
        int newReplicaIndex = NODE_COUNT - 1;
        testSuccessfulShiftDownMigration(PARTITION_ID_TO_MIGRATE, replicaIndexToClear, oldReplicaIndex, newReplicaIndex);
    }

    @Test
    public void testPartitionOwnerShiftDownRollback() throws Exception {
        int oldReplicaIndex = 0;
        int replicaIndexToClear = NODE_COUNT - 1;
        int newReplicaIndex = NODE_COUNT - 1;
        testFailedShiftDownMigration(PARTITION_ID_TO_MIGRATE, replicaIndexToClear, oldReplicaIndex, newReplicaIndex);
    }

    @Test
    public void testPartitionBackupShiftDownCommit() throws Exception {
        int oldReplicaIndex = 1;
        int replicaIndexToClear = NODE_COUNT - 1;
        int newReplicaIndex = NODE_COUNT - 1;
        testSuccessfulShiftDownMigration(PARTITION_ID_TO_MIGRATE, replicaIndexToClear, oldReplicaIndex, newReplicaIndex);
    }

    @Test
    public void testPartitionBackupShiftDownRollback() throws Exception {
        int oldReplicaIndex = 1;
        int replicaIndexToClear = NODE_COUNT - 1;
        int newReplicaIndex = NODE_COUNT - 1;
        testFailedShiftDownMigration(PARTITION_ID_TO_MIGRATE, replicaIndexToClear, oldReplicaIndex, newReplicaIndex);
    }

    private void testSuccessfulShiftDownMigration(int partitionId, int replicaIndexToClear, int oldReplicaIndex,
                                                  int newReplicaIndex) throws Exception {
        PartitionReplica destination = clearReplicaIndex(partitionId, replicaIndexToClear);
        MigrationInfo migration = createShiftDownMigration(partitionId, oldReplicaIndex, newReplicaIndex, destination);
        migrateWithSuccess(migration);

        assertMigrationSourceCommit(migration);
        assertMigrationDestinationCommit(migration);
        assertPartitionDataAfterMigrations();
    }

    private void testFailedShiftDownMigration(int partitionId, int replicaIndexToClear, int oldReplicaIndex,
                                              int newReplicaIndex) throws Exception {
        PartitionReplica destination = clearReplicaIndex(partitionId, replicaIndexToClear);
        MigrationInfo migration = createShiftDownMigration(partitionId, oldReplicaIndex, newReplicaIndex, destination);
        migrateWithFailure(migration);

        assertMigrationSourceRollback(migration);
        assertMigrationDestinationRollback(migration);
        assertPartitionDataAfterMigrations();
    }

    @Test
    public void testPartitionBackupCopyCommit() throws Exception {
        PartitionReplica destination = clearReplicaIndex(PARTITION_ID_TO_MIGRATE, NODE_COUNT - 1);
        MigrationInfo migration = createCopyMigration(PARTITION_ID_TO_MIGRATE, NODE_COUNT - 1, destination);

        migrateWithSuccess(migration);

        assertMigrationDestinationCommit(migration);

        assertPartitionDataAfterMigrations();
    }

    @Test
    public void testPartitionBackupCopyRollback() throws Exception {
        PartitionReplica destination = clearReplicaIndex(PARTITION_ID_TO_MIGRATE, NODE_COUNT - 1);
        MigrationInfo migration = createCopyMigration(PARTITION_ID_TO_MIGRATE, NODE_COUNT - 1, destination);

        migrateWithFailure(migration);

        assertMigrationDestinationRollback(migration);

        assertPartitionDataAfterMigrations();
    }

    @Test
    public void testPartitionBackupShiftUpCommitWithNonNullOwnerOfReplicaIndex() throws Exception {
        int oldReplicaIndex = NODE_COUNT - 1;
        int newReplicaIndex = NODE_COUNT - 2;
        MigrationInfo migration = createShiftUpMigration(PARTITION_ID_TO_MIGRATE, oldReplicaIndex, newReplicaIndex);

        migrateWithSuccess(migration);

        assertMigrationSourceCommit(migration);
        assertMigrationDestinationCommit(migration);

        assertPartitionDataAfterMigrations();
    }

    @Test
    public void testPartitionBackupShiftUpRollbackWithNonNullOwnerOfReplicaIndex() throws Exception {
        int oldReplicaIndex = NODE_COUNT - 1;
        int newReplicaIndex = NODE_COUNT - 2;
        MigrationInfo migration = createShiftUpMigration(PARTITION_ID_TO_MIGRATE, oldReplicaIndex, newReplicaIndex);

        migrateWithFailure(migration);

        assertMigrationSourceRollback(migration);
        assertMigrationDestinationRollback(migration);

        assertPartitionDataAfterMigrations();
    }

    @Test
    public void testPartitionBackupShiftUpCommitWithNullOwnerOfReplicaIndex() throws Exception {
        int oldReplicaIndex = NODE_COUNT - 1;
        int newReplicaIndex = NODE_COUNT - 2;
        clearReplicaIndex(PARTITION_ID_TO_MIGRATE, newReplicaIndex);
        MigrationInfo migration = createShiftUpMigration(PARTITION_ID_TO_MIGRATE, oldReplicaIndex, newReplicaIndex);

        migrateWithSuccess(migration);

        assertMigrationDestinationCommit(migration);

        assertPartitionDataAfterMigrations();
    }

    @Test
    public void testPartitionBackupShiftUpRollbackWithNullOwnerOfReplicaIndex() throws Exception {
        int oldReplicaIndex = NODE_COUNT - 1;
        int newReplicaIndex = NODE_COUNT - 2;
        clearReplicaIndex(PARTITION_ID_TO_MIGRATE, newReplicaIndex);
        MigrationInfo migration = createShiftUpMigration(PARTITION_ID_TO_MIGRATE, oldReplicaIndex, newReplicaIndex);

        migrateWithFailure(migration);

        assertMigrationDestinationRollback(migration);

        assertPartitionDataAfterMigrations();
    }

    private MigrationInfo createMoveMigration(int partitionId, int replicaIndex, PartitionReplica destination) {
        InternalPartition partition = getPartition(instances[0], partitionId);
        PartitionReplica source = partition.getReplica(replicaIndex);

        return new MigrationInfo(partitionId, source, destination, replicaIndex, -1, -1, replicaIndex);
    }

    private MigrationInfo createShiftDownMigration(int partitionId, int oldReplicaIndex, int newReplicaIndex,
                                                   PartitionReplica destination) {
        InternalPartitionImpl partition = getPartition(instances[0], partitionId);
        PartitionReplica source = partition.getReplica(oldReplicaIndex);

        return new MigrationInfo(partitionId, source, destination, oldReplicaIndex,
                newReplicaIndex, -1, oldReplicaIndex);
    }

    private MigrationInfo createCopyMigration(int partitionId, int copyReplicaIndex, PartitionReplica destination) {
        return new MigrationInfo(partitionId, null, destination, -1, -1, -1, copyReplicaIndex);
    }

    private MigrationInfo createShiftUpMigration(int partitionId, int oldReplicaIndex, int newReplicaIndex) {
        InternalPartitionImpl partition = getPartition(instances[0], partitionId);
        PartitionReplica source = partition.getReplica(newReplicaIndex);
        PartitionReplica destination = partition.getReplica(oldReplicaIndex);

        return new MigrationInfo(partitionId, source, destination, newReplicaIndex, -1, oldReplicaIndex, newReplicaIndex);
    }

    private PartitionReplica clearReplicaIndex(final int partitionId, int replicaIndexToClear) {
        final InternalPartitionServiceImpl partitionService
                = (InternalPartitionServiceImpl) getPartitionService(instances[0]);
        InternalPartitionImpl partition = (InternalPartitionImpl) partitionService.getPartition(partitionId);

        final PartitionReplica oldReplicaOwner = partition.getReplica(replicaIndexToClear);

        partition.setReplica(replicaIndexToClear, null);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                partitionService.checkClusterPartitionRuntimeStates();
                for (HazelcastInstance instance : instances) {
                    assertEquals(partitionService.getPartitionStateStamp(), getPartitionService(instance).getPartitionStateStamp());
                }
            }
        });

        HazelcastInstance oldReplicaOwnerInstance = factory.getInstance(oldReplicaOwner.address());
        ClearReplicaRunnable op = new ClearReplicaRunnable(partitionId, getNodeEngineImpl(oldReplicaOwnerInstance));
        getOperationService(oldReplicaOwnerInstance).execute(op);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                PartitionReplicaVersionsView replicaVersionsView
                        = getPartitionReplicaVersionsView(getNode(factory.getInstance(oldReplicaOwner.address())), partitionId);
                for (ServiceNamespace namespace : replicaVersionsView.getNamespaces()) {
                    assertArrayEquals(new long[InternalPartition.MAX_BACKUP_COUNT], replicaVersionsView.getVersions(namespace));
                }
            }
        });

        TestMigrationAwareService migrationAwareService = getService(oldReplicaOwner.address());
        migrationAwareService.clearPartitionReplica(partitionId);

        for (HazelcastInstance instance : instances) {
            TestMigrationAwareService service = getNodeEngineImpl(instance)
                                                         .getService(TestMigrationAwareService.SERVICE_NAME);

            service.clearEvents();
        }

        return oldReplicaOwner;
    }

    private void migrateWithSuccess(final MigrationInfo migration) {
        InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) getPartitionService(instances[0]);
        partitionService.getMigrationManager().scheduleMigration(migration);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : factory.getAllHazelcastInstances()) {
                    InternalPartitionImpl partition = getPartition(instance, migration.getPartitionId());
                    assertEquals(partition.getReplicaAddress(migration.getDestinationNewReplicaIndex()),
                            migration.getDestinationAddress());
                }
            }
        });
    }

    private void migrateWithFailure(final MigrationInfo migration) {
        if (!getAddress(instances[0]).equals(migration.getDestinationAddress())) {
            HazelcastInstance destinationInstance = factory.getInstance(migration.getDestinationAddress());
            RejectMigrationOnComplete destinationListener = new RejectMigrationOnComplete(destinationInstance);
            InternalPartitionServiceImpl destinationPartitionService
                    = (InternalPartitionServiceImpl) getPartitionService(destinationInstance);
            destinationPartitionService.getMigrationManager().setMigrationInterceptor(destinationListener);
        }

        InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) getPartitionService(instances[0]);

        CountDownMigrationRollbackOnMaster masterListener = new CountDownMigrationRollbackOnMaster(migration);
        partitionService.getMigrationManager().setMigrationInterceptor(masterListener);
        partitionService.getMigrationManager().scheduleMigration(migration);
        assertOpenEventually(masterListener.latch);
    }

    private void assertMigrationSourceCommit(final MigrationInfo migration) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                TestMigrationAwareService service = getService(migration.getSourceAddress());

                String msg = getAssertMessage(migration, service);

                assertFalse(service.getBeforeEvents().isEmpty());
                assertFalse(service.getCommitEvents().isEmpty());

                PartitionMigrationEvent beforeEvent = service.getBeforeEvents().get(0);
                PartitionMigrationEvent sourceCommitEvent = service.getCommitEvents().get(0);

                assertSourcePartitionMigrationEvent(msg, beforeEvent, migration);
                assertSourcePartitionMigrationEvent(msg, sourceCommitEvent, migration);

                assertReplicaVersionsAndServiceData(msg, migration.getSourceAddress(), migration.getPartitionId(),
                        migration.getSourceNewReplicaIndex());
            }
        });
    }

    private void assertMigrationSourceRollback(final MigrationInfo migration) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                TestMigrationAwareService service = getService(migration.getSourceAddress());

                String msg = getAssertMessage(migration, service);

                assertFalse(service.getBeforeEvents().isEmpty());
                assertFalse(service.getRollbackEvents().isEmpty());

                PartitionMigrationEvent beforeEvent = service.getBeforeEvents().get(0);
                PartitionMigrationEvent rollbackEvent = service.getRollbackEvents().get(0);

                assertSourcePartitionMigrationEvent(msg, beforeEvent, migration);
                assertSourcePartitionMigrationEvent(msg, rollbackEvent, migration);

                assertReplicaVersionsAndServiceData(msg, migration.getSourceAddress(), migration.getPartitionId(),
                        migration.getSourceCurrentReplicaIndex());
            }
        });
    }

    private void assertMigrationDestinationCommit(final MigrationInfo migration) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                TestMigrationAwareService service = getService(migration.getDestinationAddress());

                String msg = getAssertMessage(migration, service);

                assertFalse(service.getBeforeEvents().isEmpty());
                assertFalse(service.getCommitEvents().isEmpty());

                PartitionMigrationEvent beforeEvent = service.getBeforeEvents().get(0);
                PartitionMigrationEvent commitEvent = service.getCommitEvents().get(0);

                assertDestinationPartitionMigrationEvent(msg, beforeEvent, migration);
                assertDestinationPartitionMigrationEvent(msg, commitEvent, migration);

                assertTrue(msg, service.contains(migration.getPartitionId()));

                assertReplicaVersionsAndServiceData(msg, migration.getDestinationAddress(), migration.getPartitionId(),
                        migration.getDestinationNewReplicaIndex());
            }
        });
    }

    private void assertMigrationDestinationRollback(final MigrationInfo migration) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                TestMigrationAwareService service = getService(migration.getDestinationAddress());

                String msg = getAssertMessage(migration, service);

                assertFalse(service.getBeforeEvents().isEmpty());
                assertFalse(service.getRollbackEvents().isEmpty());

                PartitionMigrationEvent beforeEvent = service.getBeforeEvents().get(0);
                PartitionMigrationEvent destinationRollbackEvent = service.getRollbackEvents().get(0);

                assertDestinationPartitionMigrationEvent(msg, beforeEvent, migration);
                assertDestinationPartitionMigrationEvent(msg, destinationRollbackEvent, migration);

                assertReplicaVersionsAndServiceData(msg, migration.getDestinationAddress(), migration.getPartitionId(),
                        migration.getDestinationCurrentReplicaIndex());
            }
        });
    }

    private void assertReplicaVersionsAndServiceData(String msg, Address address, int partitionId, int replicaIndex) {
        TestMigrationAwareService service = getService(address);

        boolean shouldContainData = replicaIndex != -1 && replicaIndex <= BACKUP_COUNT;
        assertEquals(msg, shouldContainData, service.contains(partitionId));

        long[] replicaVersions = getDefaultReplicaVersions(getNode(factory.getInstance(address)), partitionId);

        msg = msg + " , ReplicaVersions: " + Arrays.toString(replicaVersions);
        if (shouldContainData) {
            if (replicaIndex == 0) {
                assertArrayEquals(msg, replicaVersions, new long[]{1, 1, 1, 1, 1, 1});
            } else {
                for (int i = 1; i < InternalPartition.MAX_BACKUP_COUNT; i++) {
                    if (i < replicaIndex || i > BACKUP_COUNT) {
                        assertEquals(msg + " at index: " + i, 0, replicaVersions[i - 1]);
                    } else {
                        assertEquals(msg + " at index: " + i, 1, replicaVersions[i - 1]);
                    }
                }
            }
        } else {
            assertArrayEquals(msg, replicaVersions, new long[]{0, 0, 0, 0, 0, 0});
        }
    }

    private void assertPartitionDataAfterMigrations() throws Exception {
        OperationServiceImpl operationService = getOperationService(instances[0]);
        for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
            assertNotNull(operationService.invokeOnPartition(null, new TestGetOperation(), partitionId)
                                          .get(1, TimeUnit.MINUTES));
        }
    }

    private String getAssertMessage(MigrationInfo migration, TestMigrationAwareService service) {
        return migration + " -> BeforeEvents: " + service.getBeforeEvents()
                + ", CommitEvents: " + service.getCommitEvents()
                + ", RollbackEvents: " + service.getRollbackEvents();
    }

    private void assertSourcePartitionMigrationEvent(String msg, PartitionMigrationEvent event, MigrationInfo migration) {
        assertEquals(msg, SOURCE, event.getMigrationEndpoint());
        assertEquals(msg, migration.getSourceCurrentReplicaIndex(), event.getCurrentReplicaIndex());
        assertEquals(msg, migration.getSourceNewReplicaIndex(), event.getNewReplicaIndex());
    }

    private void assertDestinationPartitionMigrationEvent(String msg, PartitionMigrationEvent event, MigrationInfo migration) {
        assertEquals(msg, DESTINATION, event.getMigrationEndpoint());
        assertEquals(msg, migration.getDestinationCurrentReplicaIndex(), event.getCurrentReplicaIndex());
        assertEquals(msg, migration.getDestinationNewReplicaIndex(), event.getNewReplicaIndex());
    }

    private Config createConfig() {
        Config config = smallInstanceConfig();

        ServiceConfig serviceConfig = TestMigrationAwareService.createServiceConfig(BACKUP_COUNT);
        ConfigAccessor.getServicesConfig(config).addServiceConfig(serviceConfig);
        config.setProperty(ClusterProperty.PARTITION_MAX_PARALLEL_REPLICATIONS.getName(), "0");
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), String.valueOf(PARTITION_COUNT));

        return config;
    }

    private InternalPartitionImpl getPartition(HazelcastInstance instance, int partitionId) {
        InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) getPartitionService(instance);
        return (InternalPartitionImpl) partitionService.getPartition(partitionId);
    }

    private TestMigrationAwareService getService(Address address) {
        return getNodeEngineImpl(factory.getInstance(address)).getService(TestMigrationAwareService.SERVICE_NAME);
    }

    private static class RejectMigrationOnComplete implements MigrationInterceptor {

        private final HazelcastInstance instance;

        RejectMigrationOnComplete(HazelcastInstance instance) {
            this.instance = instance;
        }

        @Override
        public void onMigrationComplete(MigrationParticipant participant, MigrationInfo migrationInfo, boolean success) {
            resetInternalMigrationListener(instance);
            throw new ExpectedRuntimeException("migration is failed intentionally on complete");
        }
    }

    private class CountDownMigrationRollbackOnMaster implements MigrationInterceptor {

        private final CountDownLatch latch = new CountDownLatch(1);

        private final MigrationInfo expected;

        private volatile boolean blockMigrations;

        CountDownMigrationRollbackOnMaster(MigrationInfo expected) {
            this.expected = expected;
        }

        @Override
        public void onMigrationStart(MigrationParticipant participant, MigrationInfo migrationInfo) {
            if (participant == MigrationParticipant.MASTER && blockMigrations) {
                assertOpenEventually(blockMigrationStartLatch);
            }
        }

        @Override
        public void onMigrationComplete(MigrationParticipant participant, MigrationInfo migrationInfo, boolean success) {
            if (participant == MigrationParticipant.DESTINATION) {
                throw new ExpectedRuntimeException("migration is failed intentionally on complete");
            }
        }

        @Override
        public void onMigrationRollback(MigrationParticipant participant, MigrationInfo migrationInfo) {
            if (participant == MigrationParticipant.MASTER && expected.equals(migrationInfo)) {
                blockMigrations = true;
                latch.countDown();
            }
        }
    }

    static final class ClearReplicaRunnable implements PartitionSpecificRunnable {

        private final int partitionId;
        private final NodeEngineImpl nodeEngine;

        ClearReplicaRunnable(int partitionId, NodeEngineImpl nodeEngine) {
            this.partitionId = partitionId;
            this.nodeEngine = nodeEngine;
        }

        @Override
        public void run() {
            InternalPartitionServiceImpl partitionService = nodeEngine.getService(InternalPartitionService.SERVICE_NAME);
            PartitionReplicaManager replicaManager = partitionService.getReplicaManager();
            replicaManager.cancelReplicaSync(partitionId);
            for (ServiceNamespace namespace : replicaManager.getNamespaces(partitionId)) {
                replicaManager.clearPartitionReplicaVersions(partitionId, namespace);
            }
        }

        @Override
        public int getPartitionId() {
            return partitionId;
        }
    }
}
