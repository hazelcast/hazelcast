package com.hazelcast.internal.partition.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.partition.service.TestGetOperation;
import com.hazelcast.internal.partition.service.TestIncrementOperation;
import com.hazelcast.internal.partition.service.TestMigrationAwareService;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.ExpectedRuntimeException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.TestPartitionUtils;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.internal.partition.impl.MigrationCommitTest.resetInternalMigrationListener;
import static com.hazelcast.spi.partition.MigrationEndpoint.DESTINATION;
import static com.hazelcast.spi.partition.MigrationEndpoint.SOURCE;
import static com.hazelcast.test.TestPartitionUtils.getReplicaVersions;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MigrationCommitServiceTest
        extends HazelcastTestSupport {

    private static final int NODE_COUNT = 4;

    private static final int PARTITION_COUNT = 10;

    private static final int BACKUP_COUNT = 6;

    private static final int PARTITION_ID_TO_MIGRATE = 0;

    private volatile CountDownLatch blockMigrationStartLatch;

    private TestHazelcastInstanceFactory factory;

    private HazelcastInstance[] instances;

    @Before
    public void setup()
            throws ExecutionException, InterruptedException {
        blockMigrationStartLatch = new CountDownLatch(1);
        factory = createHazelcastInstanceFactory(NODE_COUNT);
        instances = factory.newInstances(createConfig(), NODE_COUNT);
        warmUpPartitions(instances);
        waitAllForSafeState(instances);

        final InternalOperationService operationService = getOperationService(instances[0]);
        for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
            operationService.invokeOnPartition(null, new TestIncrementOperation(), partitionId).get();
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
                    final InternalPartitionService partitionService = getPartitionService(instances[0]);
                    final InternalPartition partition = partitionService.getPartition(partitionId);
                    for (int i = 0; i <= BACKUP_COUNT; i++) {
                        final Address replicaAddress = partition.getReplicaAddress(i);
                        if (replicaAddress != null) {
                            final TestMigrationAwareService service = getService(replicaAddress);
                            assertNotNull(service.get(partitionId));
                        }
                    }
                }
            }
        });

        for (HazelcastInstance instance : instances) {
            final TestMigrationAwareService service = getNodeEngineImpl(instance)
                    .getService(TestMigrationAwareService.SERVICE_NAME);

            service.clearEvents();
        }
    }

    @After
    public void after() {
        blockMigrationStartLatch.countDown();
    }

    @Test
    public void testPartitionOwnerMoveCommit()
            throws InterruptedException, TimeoutException, ExecutionException {
        final int replicaIndexToClear = NODE_COUNT - 1, replicaIndexToMigrate = 0;
        testSuccessfulMoveMigration(PARTITION_ID_TO_MIGRATE, replicaIndexToClear, replicaIndexToMigrate);
    }

    @Test
    public void testPartitionOwnerMoveRollback()
            throws InterruptedException, TimeoutException, ExecutionException {
        final int replicaIndexToClear = NODE_COUNT - 1, replicaIndexToMigrate = 0;
        testFailedMoveMigration(PARTITION_ID_TO_MIGRATE, replicaIndexToClear, replicaIndexToMigrate);
    }

    @Test
    public void testPartitionBackupMoveCommit()
            throws InterruptedException, TimeoutException, ExecutionException {
        final int replicaIndexToClear = NODE_COUNT - 1, replicaIndexToMigrate = NODE_COUNT - 2;
        testSuccessfulMoveMigration(PARTITION_ID_TO_MIGRATE, replicaIndexToClear, replicaIndexToMigrate);
    }

    @Test
    public void testPartitionBackupMoveRollback()
            throws InterruptedException, TimeoutException, ExecutionException {
        final int replicaIndexToClear = NODE_COUNT - 1, replicaIndexToMigrate = NODE_COUNT - 2;
        testFailedMoveMigration(PARTITION_ID_TO_MIGRATE, replicaIndexToClear, replicaIndexToMigrate);
    }

    private void testSuccessfulMoveMigration(final int partitionId, final int replicaIndexToClear,
                                             final int replicaIndexToMigrate)
            throws InterruptedException, TimeoutException, ExecutionException {
        final Address destination = clearReplicaIndex(partitionId, replicaIndexToClear);
        final MigrationInfo migration = createMoveMigration(partitionId, replicaIndexToMigrate, destination);

        migrateWithSuccess(migration);

        assertMigrationSourceCommit(migration);
        assertMigrationDestinationCommit(migration);

        assertPartitionDataAfterMigrations();
    }

    private void testFailedMoveMigration(final int partitionId, final int replicaIndexToClear, final int replicaIndexToMigrate)
            throws InterruptedException, TimeoutException, ExecutionException {
        final Address destination = clearReplicaIndex(partitionId, replicaIndexToClear);
        final MigrationInfo migration = createMoveMigration(partitionId, replicaIndexToMigrate, destination);

        migrateWithFailure(migration);

        assertMigrationSourceRollback(migration);
        assertMigrationDestinationRollback(migration);

        assertPartitionDataAfterMigrations();
    }

    @Test
    public void testPartitionOwnerShiftDownCommit()
            throws InterruptedException, TimeoutException, ExecutionException {
        final int oldReplicaIndex = 0, replicaIndexToClear = NODE_COUNT - 1, newReplicaIndex = NODE_COUNT - 1;
        testSuccessfulShiftDownMigration(PARTITION_ID_TO_MIGRATE, replicaIndexToClear, oldReplicaIndex, newReplicaIndex);
    }

    @Test
    public void testPartitionOwnerShiftDownRollback()
            throws InterruptedException, TimeoutException, ExecutionException {
        final int oldReplicaIndex = 0, replicaIndexToClear = NODE_COUNT - 1, newReplicaIndex = NODE_COUNT - 1;
        testFailedShiftDownMigration(PARTITION_ID_TO_MIGRATE, replicaIndexToClear, oldReplicaIndex, newReplicaIndex);
    }

    @Test
    public void testPartitionBackupShiftDownCommit()
            throws InterruptedException, TimeoutException, ExecutionException {
        final int oldReplicaIndex = 1, replicaIndexToClear = NODE_COUNT - 1, newReplicaIndex = NODE_COUNT - 1;
        testSuccessfulShiftDownMigration(PARTITION_ID_TO_MIGRATE, replicaIndexToClear, oldReplicaIndex, newReplicaIndex);
    }

    @Test
    public void testPartitionBackupShiftDownRollback()
            throws InterruptedException, TimeoutException, ExecutionException {
        final int oldReplicaIndex = 1, replicaIndexToClear = NODE_COUNT - 1, newReplicaIndex = NODE_COUNT - 1;
        testFailedShiftDownMigration(PARTITION_ID_TO_MIGRATE, replicaIndexToClear, oldReplicaIndex, newReplicaIndex);
    }

    private void testSuccessfulShiftDownMigration(final int partitionId, final int replicaIndexToClear,
                                                  final int oldReplicaIndex, final int newReplicaIndex)
            throws InterruptedException, TimeoutException, ExecutionException {
        final Address destination = clearReplicaIndex(partitionId, replicaIndexToClear);
        final MigrationInfo migration = createShiftDownMigration(partitionId, oldReplicaIndex, newReplicaIndex, destination);
        migrateWithSuccess(migration);

        assertMigrationSourceCommit(migration);
        assertMigrationDestinationCommit(migration);
        assertPartitionDataAfterMigrations();
    }

    private void testFailedShiftDownMigration(final int partitionId, final int replicaIndexToClear,
                                              final int oldReplicaIndex, final int newReplicaIndex)
            throws InterruptedException, TimeoutException, ExecutionException {
        final Address destination = clearReplicaIndex(partitionId, replicaIndexToClear);
        final MigrationInfo migration = createShiftDownMigration(partitionId, oldReplicaIndex, newReplicaIndex, destination);
        migrateWithFailure(migration);

        assertMigrationSourceRollback(migration);
        assertMigrationDestinationRollback(migration);
        assertPartitionDataAfterMigrations();
    }

    @Test
    public void testPartitionBackupCopyCommit()
            throws InterruptedException, TimeoutException, ExecutionException {
        final Address destination = clearReplicaIndex(PARTITION_ID_TO_MIGRATE, NODE_COUNT - 1);
        final MigrationInfo migration = createCopyMigration(PARTITION_ID_TO_MIGRATE, NODE_COUNT - 1, destination);

        migrateWithSuccess(migration);

        assertMigrationDestinationCommit(migration);

        assertPartitionDataAfterMigrations();
    }

    @Test
    public void testPartitionBackupCopyRollback()
            throws InterruptedException, TimeoutException, ExecutionException {
        final Address destination = clearReplicaIndex(PARTITION_ID_TO_MIGRATE, NODE_COUNT - 1);
        final MigrationInfo migration = createCopyMigration(PARTITION_ID_TO_MIGRATE, NODE_COUNT - 1, destination);

        migrateWithFailure(migration);

        assertMigrationDestinationRollback(migration);

        assertPartitionDataAfterMigrations();
    }

    @Test
    public void testPartitionBackupShiftUpCommitWithNonNullOwnerOfReplicaIndex()
            throws InterruptedException, TimeoutException, ExecutionException {
        final int oldReplicaIndex = NODE_COUNT - 1, newReplicaIndex = NODE_COUNT - 2;
        final MigrationInfo migration = createShiftUpMigration(PARTITION_ID_TO_MIGRATE, oldReplicaIndex, newReplicaIndex);

        migrateWithSuccess(migration);

        assertMigrationSourceCommit(migration);
        assertMigrationDestinationCommit(migration);

        assertPartitionDataAfterMigrations();
    }

    @Test
    public void testPartitionBackupShiftUpRollbackWithNonNullOwnerOfReplicaIndex()
            throws InterruptedException, TimeoutException, ExecutionException {
        final int oldReplicaIndex = NODE_COUNT - 1, newReplicaIndex = NODE_COUNT - 2;
        final MigrationInfo migration = createShiftUpMigration(PARTITION_ID_TO_MIGRATE, oldReplicaIndex, newReplicaIndex);

        migrateWithFailure(migration);

        assertMigrationSourceRollback(migration);
        assertMigrationDestinationRollback(migration);

        assertPartitionDataAfterMigrations();
    }

    @Test
    public void testPartitionBackupShiftUpCommitWithNullOwnerOfReplicaIndex()
            throws InterruptedException, TimeoutException, ExecutionException {
        final int oldReplicaIndex = NODE_COUNT - 1, newReplicaIndex = NODE_COUNT - 2;
        clearReplicaIndex(PARTITION_ID_TO_MIGRATE, newReplicaIndex);
        final MigrationInfo migration = createShiftUpMigration(PARTITION_ID_TO_MIGRATE, oldReplicaIndex, newReplicaIndex);

        migrateWithSuccess(migration);

        assertMigrationDestinationCommit(migration);

        assertPartitionDataAfterMigrations();
    }

    @Test
    public void testPartitionBackupShiftUpRollbackWithNullOwnerOfReplicaIndex()
            throws InterruptedException, TimeoutException, ExecutionException {
        final int oldReplicaIndex = NODE_COUNT - 1, newReplicaIndex = NODE_COUNT - 2;
        clearReplicaIndex(PARTITION_ID_TO_MIGRATE, newReplicaIndex);
        final MigrationInfo migration = createShiftUpMigration(PARTITION_ID_TO_MIGRATE, oldReplicaIndex, newReplicaIndex);

        migrateWithFailure(migration);

        assertMigrationDestinationRollback(migration);

        assertPartitionDataAfterMigrations();
    }

    private MigrationInfo createMoveMigration(final int partitionId, final int replicaIndex, final Address destination) {
        final InternalPartition partition = getPartition(instances[0], partitionId);
        final Address source = partition.getReplicaAddress(replicaIndex);
        final String sourceUuid = getMemberUuid(source);
        final String destinationUuid = getMemberUuid(destination);

        return new MigrationInfo(partitionId, source, sourceUuid, destination, destinationUuid, replicaIndex, -1, -1, replicaIndex);
    }

    private MigrationInfo createShiftDownMigration(final int partitionId, final int oldReplicaIndex, final int newReplicaIndex,
                                                   final Address destination) {
        final InternalPartitionImpl partition = getPartition(instances[0], partitionId);
        final Address source = partition.getReplicaAddress(oldReplicaIndex);
        final String sourceUuid = getMemberUuid(source);
        final String destinationUuid = getMemberUuid(destination);

        return new MigrationInfo(partitionId, source, sourceUuid, destination, destinationUuid, oldReplicaIndex, newReplicaIndex, -1,
                oldReplicaIndex);
    }

    private MigrationInfo createCopyMigration(final int partitionId, final int copyReplicaIndex, final Address destination) {
        return new MigrationInfo(partitionId, null, null, destination, getMemberUuid(destination), -1, -1, -1, copyReplicaIndex);
    }

    private MigrationInfo createShiftUpMigration(final int partitionId, final int oldReplicaIndex, final int newReplicaIndex) {
        final InternalPartitionImpl partition = getPartition(instances[0], partitionId);
        final Address source = partition.getReplicaAddress(newReplicaIndex);
        final String sourceUuid = getMemberUuid(source);
        final Address destination = partition.getReplicaAddress(oldReplicaIndex);
        final String destinationUuid = getMemberUuid(destination);
        return new MigrationInfo(partitionId, source, sourceUuid, destination, destinationUuid, newReplicaIndex, -1, oldReplicaIndex, newReplicaIndex);
    }

    private String getMemberUuid(Address address) {
        return address != null ? getNodeEngineImpl(factory.getInstance(address)).getLocalMember().getUuid() : null;
    }

    private Address clearReplicaIndex(final int partitionId, final int replicaIndexToClear)
            throws ExecutionException, InterruptedException {
        final InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) getPartitionService(instances[0]);
        final InternalPartitionImpl partition = (InternalPartitionImpl) partitionService.getPartition(partitionId);

        final Address oldReplicaOwner = partition.getReplicaAddress(replicaIndexToClear);

        partition.setReplicaAddress(replicaIndexToClear, null);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertTrue(partitionService.syncPartitionRuntimeState());
            }
        });

        final HazelcastInstance oldReplicaOwnerInstance = factory.getInstance(oldReplicaOwner);
        ClearReplicaRunnable op = new ClearReplicaRunnable(partitionId, getNodeEngineImpl(oldReplicaOwnerInstance));
        getOperationService(oldReplicaOwnerInstance).execute(op);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                final long[] replicaVersions = TestPartitionUtils
                        .getReplicaVersions(factory.getInstance(oldReplicaOwner), partitionId);
                assertArrayEquals(new long[InternalPartition.MAX_BACKUP_COUNT], replicaVersions);
            }
        });

        TestMigrationAwareService migrationAwareService = getService(oldReplicaOwner);
        migrationAwareService.clearPartitionReplica(partitionId);

        for (HazelcastInstance instance : instances) {
            final TestMigrationAwareService service = getNodeEngineImpl(instance)
                    .getService(TestMigrationAwareService.SERVICE_NAME);

            service.clearEvents();
        }

        return oldReplicaOwner;
    }

    private void migrateWithSuccess(final MigrationInfo migration) {
        final InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) getPartitionService(instances[0]);
        partitionService.getMigrationManager().scheduleMigration(migration);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                for (HazelcastInstance instance : factory.getAllHazelcastInstances()) {
                    final InternalPartitionImpl partition = getPartition(instance, migration.getPartitionId());
                    assertEquals(partition.getReplicaAddress(migration.getDestinationNewReplicaIndex()), migration.getDestination());
                }
            }
        });
    }

    private void migrateWithFailure(final MigrationInfo migration) {
        if (!getAddress(instances[0]).equals(migration.getDestination())) {
            final HazelcastInstance destinationInstance = factory.getInstance(migration.getDestination());
            final RejectMigrationOnComplete destinationListener = new RejectMigrationOnComplete(destinationInstance);
            final InternalPartitionServiceImpl destinationPartitionService = (InternalPartitionServiceImpl) getPartitionService(
                    destinationInstance);
            destinationPartitionService.getMigrationManager().setInternalMigrationListener(destinationListener);
        }

        final InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) getPartitionService(instances[0]);

        final CountDownMigrationRollbackOnMaster masterListener = new CountDownMigrationRollbackOnMaster(migration);
        partitionService.getMigrationManager().setInternalMigrationListener(masterListener);
        partitionService.getMigrationManager().scheduleMigration(migration);
        assertOpenEventually(masterListener.latch);
    }

    private void assertMigrationSourceCommit(final MigrationInfo migration)
            throws InterruptedException {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                final TestMigrationAwareService service = getService(migration.getSource());

                final String msg = getAssertMessage(migration, service);

                assertFalse(service.getBeforeEvents().isEmpty());
                assertFalse(service.getCommitEvents().isEmpty());

                final PartitionMigrationEvent beforeEvent = service.getBeforeEvents().get(0);
                final PartitionMigrationEvent sourceCommitEvent = service.getCommitEvents().get(0);

                assertSourcePartitionMigrationEvent(msg, beforeEvent, migration);
                assertSourcePartitionMigrationEvent(msg, sourceCommitEvent, migration);

                assertReplicaVersionsAndServiceData(msg, migration.getSource(), migration.getPartitionId(),
                        migration.getSourceNewReplicaIndex());
            }
        });
    }

    private void assertMigrationSourceRollback(final MigrationInfo migration)
            throws InterruptedException {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                final TestMigrationAwareService service = getService(migration.getSource());

                final String msg = getAssertMessage(migration, service);

                assertFalse(service.getBeforeEvents().isEmpty());
                assertFalse(service.getRollbackEvents().isEmpty());

                final PartitionMigrationEvent beforeEvent = service.getBeforeEvents().get(0);
                final PartitionMigrationEvent rollbackEvent = service.getRollbackEvents().get(0);

                assertSourcePartitionMigrationEvent(msg, beforeEvent, migration);
                assertSourcePartitionMigrationEvent(msg, rollbackEvent, migration);

                assertReplicaVersionsAndServiceData(msg, migration.getSource(), migration.getPartitionId(),
                        migration.getSourceCurrentReplicaIndex());
            }
        });
    }

    private void assertMigrationDestinationCommit(final MigrationInfo migration)
            throws InterruptedException {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                final TestMigrationAwareService service = getService(migration.getDestination());

                final String msg = getAssertMessage(migration, service);

                assertFalse(service.getBeforeEvents().isEmpty());
                assertFalse(service.getCommitEvents().isEmpty());

                final PartitionMigrationEvent beforeEvent = service.getBeforeEvents().get(0);
                final PartitionMigrationEvent commitEvent = service.getCommitEvents().get(0);

                assertDestinationPartitionMigrationEvent(msg, beforeEvent, migration);
                assertDestinationPartitionMigrationEvent(msg, commitEvent, migration);

                assertTrue(msg, service.contains(migration.getPartitionId()));

                assertReplicaVersionsAndServiceData(msg, migration.getDestination(), migration.getPartitionId(),
                        migration.getDestinationNewReplicaIndex());
            }
        });
    }

    private void assertMigrationDestinationRollback(final MigrationInfo migration)
            throws InterruptedException {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                final TestMigrationAwareService service = getService(migration.getDestination());

                final String msg = getAssertMessage(migration, service);

                assertFalse(service.getBeforeEvents().isEmpty());
                assertFalse(service.getRollbackEvents().isEmpty());

                final PartitionMigrationEvent beforeEvent = service.getBeforeEvents().get(0);
                final PartitionMigrationEvent destinationRollbackEvent = service.getRollbackEvents().get(0);

                assertDestinationPartitionMigrationEvent(msg, beforeEvent, migration);
                assertDestinationPartitionMigrationEvent(msg, destinationRollbackEvent, migration);

                assertReplicaVersionsAndServiceData(msg, migration.getDestination(), migration.getPartitionId(),
                        migration.getDestinationCurrentReplicaIndex());
            }
        });
    }

    private void assertReplicaVersionsAndServiceData(String msg, final Address address, final int partitionId,
                                                     final int replicaIndex)
            throws InterruptedException {
        final TestMigrationAwareService service = getService(address);

        final boolean shouldContainData = replicaIndex != -1 && replicaIndex <= BACKUP_COUNT;
        assertEquals(msg, shouldContainData, service.contains(partitionId));

        final long[] replicaVersions = getReplicaVersions(factory.getInstance(address), partitionId);

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

    private void assertPartitionDataAfterMigrations()
            throws ExecutionException, InterruptedException, TimeoutException {
        final InternalOperationService operationService = getOperationService(instances[0]);
        for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
            assertNotNull(operationService.invokeOnPartition(null, new TestGetOperation(), partitionId)
                    .get(10, TimeUnit.SECONDS));
        }
    }

    private String getAssertMessage(MigrationInfo migration, TestMigrationAwareService service) {
        return migration + " -> BeforeEvents: " + service.getBeforeEvents() + " , CommitEvents: " + service.getCommitEvents()
                + " , RollbackEvents: " + service.getRollbackEvents();
    }

    private void assertSourcePartitionMigrationEvent(final String msg, final PartitionMigrationEvent event,
                                                     final MigrationInfo migration) {
        assertEquals(msg, SOURCE, event.getMigrationEndpoint());
        assertEquals(msg, migration.getSourceCurrentReplicaIndex(), event.getCurrentReplicaIndex());
        assertEquals(msg, migration.getSourceNewReplicaIndex(), event.getNewReplicaIndex());
    }

    private void assertDestinationPartitionMigrationEvent(final String msg, final PartitionMigrationEvent event,
                                                          final MigrationInfo migration) {
        assertEquals(msg, DESTINATION, event.getMigrationEndpoint());
        assertEquals(msg, migration.getDestinationCurrentReplicaIndex(), event.getCurrentReplicaIndex());
        assertEquals(msg, migration.getDestinationNewReplicaIndex(), event.getNewReplicaIndex());
    }

    private Config createConfig() {
        final Config config = new Config();

        ServiceConfig serviceConfig = TestMigrationAwareService.createServiceConfig(BACKUP_COUNT);
        config.getServicesConfig().addServiceConfig(serviceConfig);
        config.setProperty(GroupProperty.PARTITION_MAX_PARALLEL_REPLICATIONS.getName(), "0");
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), String.valueOf(PARTITION_COUNT));

        return config;
    }

    private InternalPartitionImpl getPartition(final HazelcastInstance instance, final int partitionId) {
        final InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) getPartitionService(instance);
        return (InternalPartitionImpl) partitionService.getPartition(partitionId);
    }

    private TestMigrationAwareService getService(final Address address) {
        return getNodeEngineImpl(factory.getInstance(address)).getService(TestMigrationAwareService.SERVICE_NAME);
    }

    private static class RejectMigrationOnComplete
            extends InternalMigrationListener {

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

    private class CountDownMigrationRollbackOnMaster
            extends InternalMigrationListener {

        private final CountDownLatch latch = new CountDownLatch(1);

        private final MigrationInfo expected;

        private volatile boolean blockMigrations;

        CountDownMigrationRollbackOnMaster(MigrationInfo expected) {
            this.expected = expected;
        }

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

        public ClearReplicaRunnable(int partitionId, NodeEngineImpl nodeEngine) {
            this.partitionId = partitionId;
            this.nodeEngine = nodeEngine;
        }

        @Override
        public void run()  {
            final InternalPartitionServiceImpl partitionService = nodeEngine.getService(InternalPartitionService.SERVICE_NAME);
            partitionService.getReplicaManager().cancelReplicaSync(partitionId);
            partitionService.getReplicaManager().clearPartitionReplicaVersions(partitionId);
        }


        @Override
        public int getPartitionId() {
            return partitionId;
        }

    }

}
