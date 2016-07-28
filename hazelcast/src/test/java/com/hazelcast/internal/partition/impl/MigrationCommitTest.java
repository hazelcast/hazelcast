package com.hazelcast.internal.partition.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.partition.impl.InternalMigrationListenerTest.InternalMigrationListenerImpl;
import com.hazelcast.internal.partition.impl.InternalMigrationListenerTest.MigrationProgressNotification;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.partition.impl.InternalMigrationListenerTest.MigrationProgressEvent.COMMIT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MigrationCommitTest
        extends HazelcastTestSupport {

    private static final int PARTITION_COUNT = 2;

    private TestHazelcastInstanceFactory factory;

    @Before
    public void init() {
        factory = createHazelcastInstanceFactory(3);
    }

    @Test
    public void shouldCommitMigrationWhenMasterIsMigrationSource() {
        final HazelcastInstance hz1 = factory.newHazelcastInstance(createConfig());

        final Config config2 = createConfig();
        config2.setLiteMember(true);

        final HazelcastInstance hz2 = factory.newHazelcastInstance(config2);

        warmUpPartitions(hz1, hz2);
        waitAllForSafeState(hz1, hz2);

        final HazelcastInstance hz3 = factory.newHazelcastInstance(createConfig());

        warmUpPartitions(hz3);
        waitAllForSafeState(hz1, hz2, hz3);

        final InternalPartition hz1Partition = getOwnedPartition(hz1);
        final InternalPartition hz3Partition = getOwnedPartition(hz3);
        assertNotNull(hz1Partition);
        assertNotNull(hz3Partition);
        assertNotEquals(hz1Partition, hz3Partition);
        assertFalse(hz1Partition.isMigrating());
        assertFalse(hz3Partition.isMigrating());
    }

    @Test
    public void shouldCommitMigrationWhenMasterIsDestination() {
        final HazelcastInstance hz1 = factory.newHazelcastInstance(createConfig());

        warmUpPartitions(hz1);

        final HazelcastInstance hz2 = factory.newHazelcastInstance(createConfig());

        warmUpPartitions(hz1, hz2);
        waitAllForSafeState(hz1, hz2);

        hz2.getLifecycleService().terminate();

        assertClusterSizeEventually(1, hz1);
        waitAllForSafeState(hz1);

        final InternalPartition partition0 = getPartitionService(hz1).getPartition(0);
        final InternalPartition partition1 = getPartitionService(hz1).getPartition(1);

        assertEquals(getAddress(hz1), partition0.getOwnerOrNull());
        assertEquals(getAddress(hz1), partition1.getOwnerOrNull());
        assertFalse(partition0.isMigrating());
        assertFalse(partition1.isMigrating());
    }

    @Test
    public void shouldCommitMigrationWhenMasterIsNotMigrationEndpoint() {
        final Config config1 = createConfig();
        config1.setLiteMember(true);

        final HazelcastInstance hz1 = factory.newHazelcastInstance(config1);

        final HazelcastInstance hz2 = factory.newHazelcastInstance(createConfig());

        warmUpPartitions(hz1, hz2);
        waitAllForSafeState(hz1, hz2);

        final HazelcastInstance hz3 = factory.newHazelcastInstance(createConfig());

        warmUpPartitions(hz3);
        waitAllForSafeState(hz1, hz2, hz3);

        final InternalPartition hz2Partition = getOwnedPartition(hz2);
        final InternalPartition hz3Partition = getOwnedPartition(hz3);
        assertNotNull(hz2Partition);
        assertNotNull(hz3Partition);
        assertNotEquals(hz2Partition, hz3Partition);
        assertFalse(hz2Partition.isMigrating());
        assertFalse(hz3Partition.isMigrating());
    }

    @Test
    public void shouldRollbackMigrationWhenMasterCrashesBeforeCommit() {
        final CountDownLatch migrationStartLatch = new CountDownLatch(1);
        final Config config1 = createConfig();
        config1.setLiteMember(true);

        final HazelcastInstance hz1 = factory.newHazelcastInstance(config1);

        final TerminateOtherMemberOnMigrationComplete listener2 = new TerminateOtherMemberOnMigrationComplete(migrationStartLatch);

        final Config config2 = createConfig();
        config2.addListenerConfig(new ListenerConfig(listener2));
        final HazelcastInstance hz2 = factory.newHazelcastInstance(config2);

        listener2.other = hz1;
        migrationStartLatch.countDown();

        warmUpPartitions(hz1, hz2);
        waitAllForSafeState(hz1, hz2);

        final CollectMigrationTaskOnRollback listener3 = new CollectMigrationTaskOnRollback();
        final Config config3 = createConfig();
        config3.addListenerConfig(new ListenerConfig(listener3));
        final HazelcastInstance hz3 = factory.newHazelcastInstance(config3);

        assertClusterSizeEventually(2, hz2);
        assertClusterSizeEventually(2, hz3);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertTrue(listener2.rollback);
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertTrue(listener3.rollback);
            }
        });

        waitAllForSafeState(hz2, hz3);

        factory.terminateAll();
    }

    @Test
    public void shouldRollbackMigrationWhenDestinationCrashesBeforeCommit() {
        final CountDownLatch migrationStartLatch = new CountDownLatch(1);
        final Config config1 = createConfig();
        config1.setLiteMember(true);
        final TerminateOtherMemberOnMigrationComplete masterListener = new TerminateOtherMemberOnMigrationComplete(
                migrationStartLatch);
        config1.addListenerConfig(new ListenerConfig(masterListener));

        final HazelcastInstance hz1 = factory.newHazelcastInstance(config1);

        final HazelcastInstance hz2 = factory.newHazelcastInstance(createConfig());

        warmUpPartitions(hz1, hz2);
        waitAllForSafeState(hz1, hz2);

        final HazelcastInstance hz3 = factory.newHazelcastInstance(createConfig());

        masterListener.other = hz3;
        migrationStartLatch.countDown();

        sleepAtLeastSeconds(10);

        waitAllForSafeState(hz1, hz2);

        final InternalPartition partition0 = getPartitionService(hz2).getPartition(0);
        final InternalPartition partition1 = getPartitionService(hz2).getPartition(1);

        assertEquals(getAddress(hz2), partition0.getOwnerOrNull());
        assertEquals(getAddress(hz2), partition1.getOwnerOrNull());
        assertFalse(partition0.isMigrating());
        assertFalse(partition1.isMigrating());
        assertTrue(masterListener.rollback);
    }

    @Test
    public void shouldCommitMigrationWhenMasterCrashesAfterDestinationCommit() {
        final CountDownLatch migrationStartLatch = new CountDownLatch(1);
        final Config config1 = createConfig();
        config1.setLiteMember(true);

        final HazelcastInstance hz1 = factory.newHazelcastInstance(config1);

        final Config config2 = createConfig();
        final CollectMigrationTaskOnCommit sourceListener = new CollectMigrationTaskOnCommit();
        config2.addListenerConfig(new ListenerConfig(sourceListener));
        final HazelcastInstance hz2 = factory.newHazelcastInstance(config2);

        warmUpPartitions(hz1, hz2);
        waitAllForSafeState(hz1, hz2);

        final Config config3 = createConfig();
        final TerminateOtherMemberOnMigrationCommit destinationListener = new TerminateOtherMemberOnMigrationCommit(
                migrationStartLatch);
        destinationListener.other = hz1;
        config3.addListenerConfig(new ListenerConfig(destinationListener));
        final HazelcastInstance hz3 = factory.newHazelcastInstance(config3);

        migrationStartLatch.countDown();

        waitAllForSafeState(hz2, hz3);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertTrue(sourceListener.commit);
            }
        });

        final InternalPartition hz2Partition = getOwnedPartition(hz2);
        final InternalPartition hz3Partition = getOwnedPartition(hz3);
        assertNotNull(hz2Partition);
        assertNotNull(hz3Partition);
        assertNotEquals(hz2Partition, hz3Partition);
        assertFalse(hz2Partition.isMigrating());
        assertFalse(hz3Partition.isMigrating());
    }

    @Test
    public void shouldCommitMigrationWhenSourceFailsDuringCommit() {
        final CountDownLatch migrationStartLatch = new CountDownLatch(1);
        final Config config1 = createConfig();
        config1.setLiteMember(true);
        final TerminateOtherMemberOnMigrationComplete masterListener = new TerminateOtherMemberOnMigrationComplete(
                migrationStartLatch);
        config1.addListenerConfig(new ListenerConfig(masterListener));

        final HazelcastInstance hz1 = factory.newHazelcastInstance(config1);

        final HazelcastInstance hz2 = factory.newHazelcastInstance(createConfig());

        warmUpPartitions(hz1, hz2);
        waitAllForSafeState(hz1, hz2);

        final Config config3 = createConfig();
        final InternalMigrationListenerImpl targetListener = new InternalMigrationListenerImpl();
        config3.addListenerConfig(new ListenerConfig(targetListener));
        final HazelcastInstance hz3 = factory.newHazelcastInstance(config3);

        masterListener.other = hz2;
        migrationStartLatch.countDown();

        waitAllForSafeState(hz1, hz3);

        final InternalPartition partition0 = getPartitionService(hz3).getPartition(0);
        final InternalPartition partition1 = getPartitionService(hz3).getPartition(1);

        assertEquals(getAddress(hz3), partition0.getOwnerOrNull());
        assertEquals(getAddress(hz3), partition1.getOwnerOrNull());
        assertFalse(partition0.isMigrating());
        assertFalse(partition1.isMigrating());
        assertFalse(masterListener.rollback);
        final List<MigrationProgressNotification> notifications = targetListener.getNotifications();
        assertFalse(notifications.isEmpty());
        assertEquals(COMMIT, notifications.get(notifications.size() - 1).event);
    }

    @Test
    public void shouldRollbackMigrationWhenDestinationCrashesDuringCommit() {
        final CountDownLatch migrationStartLatch = new CountDownLatch(1);
        final Config config1 = createConfig();
        config1.setLiteMember(true);
        final DelayMigrationStartOnMaster masterListener = new DelayMigrationStartOnMaster(migrationStartLatch);
        config1.addListenerConfig(new ListenerConfig(masterListener));

        final HazelcastInstance hz1 = factory.newHazelcastInstance(config1);

        final HazelcastInstance hz2 = factory.newHazelcastInstance(createConfig());

        warmUpPartitions(hz1, hz2);
        waitAllForSafeState(hz1, hz2);

        final CountDownLatch terminationLatch = new CountDownLatch(1);
        final TerminateOnMigrationCommit memberListener = new TerminateOnMigrationCommit(terminationLatch);
        final Config config3 = createConfig();
        config3.addListenerConfig(new ListenerConfig(memberListener));
        final HazelcastInstance hz3 = factory.newHazelcastInstance(config3);

        warmUpPartitions(hz1, hz2, hz3);

        migrationStartLatch.countDown();

        waitAllForSafeState(hz1, hz2);

        final InternalPartition partition0 = getPartitionService(hz1).getPartition(0);
        final InternalPartition partition1 = getPartitionService(hz1).getPartition(1);

        assertEquals(getAddress(hz2), partition0.getOwnerOrNull());
        assertEquals(getAddress(hz2), partition1.getOwnerOrNull());
        assertFalse(partition0.isMigrating());
        assertFalse(partition1.isMigrating());
        assertTrue(masterListener.rollback.get());

        terminationLatch.countDown();
    }

    @Test
    public void shouldRetryMigrationIfParticipantPartitionTableVersionFallsBehind() {
        final CountDownLatch migrationStartLatch = new CountDownLatch(1);
        final Config config1 = createConfig();
        config1.setLiteMember(true);
        final IncrementPartitionTableOnMigrationStart masterListener = new IncrementPartitionTableOnMigrationStart(
                migrationStartLatch);
        config1.addListenerConfig(new ListenerConfig(masterListener));

        final HazelcastInstance hz1 = factory.newHazelcastInstance(config1);

        final HazelcastInstance hz2 = factory.newHazelcastInstance(createConfig());

        warmUpPartitions(hz1, hz2);
        waitAllForSafeState(hz1, hz2);

        final Config config3 = createConfig();
        final InternalMigrationListenerImpl targetListener = new InternalMigrationListenerImpl();
        config3.addListenerConfig(new ListenerConfig(targetListener));
        final HazelcastInstance hz3 = factory.newHazelcastInstance(config3);

        migrationStartLatch.countDown();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertTrue(masterListener.failed);
            }
        });

        waitAllForSafeState(hz1, hz2, hz3);
    }

    @Test
    public void shouldEvictCompletedMigrationsWhenAllMembersAckPublishedPartitionTableAfterSuccessfulMigration() {
        final CountDownLatch migrationStartLatch = new CountDownLatch(1);
        final Config config1 = createConfig();
        config1.setLiteMember(true);
        final CollectMigrationTaskOnCommit masterListener = new CollectMigrationTaskOnCommit();
        config1.addListenerConfig(new ListenerConfig(masterListener));

        final HazelcastInstance hz1 = factory.newHazelcastInstance(config1);

        final InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) getPartitionService(hz1);
        final MigrationManager migrationManager = partitionService.getMigrationManager();

        final HazelcastInstance hz2 = factory.newHazelcastInstance(createConfig());

        warmUpPartitions(hz1, hz2);
        waitAllForSafeState(hz1, hz2);

        final Config config3 = createConfig();
        final DelayMigrationStart destinationListener = new DelayMigrationStart(migrationStartLatch);

        config3.addListenerConfig(new ListenerConfig(destinationListener));
        final HazelcastInstance hz3 = factory.newHazelcastInstance(config3);

        migrationStartLatch.countDown();

        waitAllForSafeState(hz1, hz2, hz3);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertTrue(masterListener.commit);
                assertTrue(migrationManager.getCompletedMigrationsCopy().isEmpty());
            }
        });
    }

    @Test
    public void shouldNotEvictCompletedMigrationsWhenSomeMembersDoNotAckPublishedPartitionTableAfterSuccessfulMigration() {
        final CountDownLatch migrationStartLatch = new CountDownLatch(1);
        final CountDownLatch migrationCommitLatch = new CountDownLatch(1);
        final Config config1 = createConfig();
        config1.setLiteMember(true);
        final AssertNonEmptyCompletedMigrationsOnSecondMigrationStart masterListener = new AssertNonEmptyCompletedMigrationsOnSecondMigrationStart();
        config1.addListenerConfig(new ListenerConfig(masterListener));

        final HazelcastInstance hz1 = factory.newHazelcastInstance(config1);

        final Config config2 = createConfig();
        config2.addListenerConfig(new ListenerConfig(new DelayMigrationCommit(migrationCommitLatch)));
        final HazelcastInstance hz2 = factory.newHazelcastInstance(config2);

        warmUpPartitions(hz1, hz2);
        waitAllForSafeState(hz1, hz2);

        factory.newHazelcastInstance(createConfig());

        migrationStartLatch.countDown();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertNotNull(masterListener.nonEmptyCompletedMigrationsVerified);
                assertTrue(masterListener.nonEmptyCompletedMigrationsVerified);
            }
        });

        migrationCommitLatch.countDown();
    }

    private Config createConfig() {
        final Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_MAX_PARALLEL_REPLICATIONS.getName(), "0");
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), String.valueOf(PARTITION_COUNT));
        return config;
    }

    private InternalPartition getOwnedPartition(final HazelcastInstance instance) {
        final InternalPartitionService partitionService = getPartitionService(instance);
        final Address address = getAddress(instance);
        if (address.equals(partitionService.getPartitionOwner(0))) {
            return partitionService.getPartition(0);
        } else if (address.equals(partitionService.getPartitionOwner(1))) {
            return partitionService.getPartition(1);
        }
        return null;
    }

    static void resetInternalMigrationListener(final HazelcastInstance instance) {
        final InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) getPartitionService(instance);
        partitionService.resetInternalMigrationListener();
    }

    private static class IncrementPartitionTableOnMigrationStart
            extends InternalMigrationListener
            implements HazelcastInstanceAware {

        private final CountDownLatch migrationStartLatch;

        private final AtomicReference<MigrationInfo> migrationInfoRef = new AtomicReference<MigrationInfo>();

        private volatile boolean failed;

        private HazelcastInstance instance;

        public IncrementPartitionTableOnMigrationStart(CountDownLatch migrationStartLatch) {
            this.migrationStartLatch = migrationStartLatch;
        }

        @Override
        public void onMigrationStart(MigrationParticipant participant, MigrationInfo migrationInfo) {
            if (failed) {
                System.err.println("Ignoring new migration start: " + migrationInfo + " as participant: " + participant
                        + " since expected migration is already committed");
                return;
            }

            assertOpenEventually(migrationStartLatch);

            if (migrationInfoRef.compareAndSet(null, migrationInfo)) {
                final InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) getPartitionService(
                        instance);
                final PartitionStateManager partitionStateManager = partitionService.getPartitionStateManager();
                partitionStateManager.incrementVersion();
            } else {
                System.err.println("COLLECT COMMIT START FAILED! curr: " + migrationInfoRef.get() + " new: " + migrationInfo);
            }
        }

        @Override
        public void onMigrationComplete(MigrationParticipant participant, MigrationInfo migrationInfo, boolean success) {
            if (failed) {
                System.err.println("Ignoring new migration complete: " + migrationInfo + " as participant: " + participant
                        + " since expected migration is already completed");
                return;
            }

            MigrationInfo collected = migrationInfoRef.get();
            failed = !success && migrationInfo.equals(collected);
            if (failed) {
                resetInternalMigrationListener(instance);
            } else {
                System.err.println(
                        "collect complete failed! collected migration: " + collected + " rollback migration: " + migrationInfo
                                + " participant: " + participant + " success: " + success);
            }
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            this.instance = instance;
        }

    }

    private static class AssertNonEmptyCompletedMigrationsOnSecondMigrationStart
            extends InternalMigrationListener
            implements HazelcastInstanceAware {

        private volatile HazelcastInstance instance;

        private volatile boolean start;

        private volatile Boolean nonEmptyCompletedMigrationsVerified = null;

        @Override
        public void onMigrationStart(MigrationParticipant participant, MigrationInfo migrationInfo) {
            if (start) {
                final InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) getPartitionService(
                        instance);
                final MigrationManager migrationManager = partitionService.getMigrationManager();
                nonEmptyCompletedMigrationsVerified = !migrationManager.getCompletedMigrationsCopy().isEmpty();
                resetInternalMigrationListener(instance);
            } else {
                start = true;
            }
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            this.instance = instance;
        }

    }

    private static class DelayMigrationCommit
            extends InternalMigrationListener
            implements HazelcastInstanceAware {

        private final CountDownLatch migrationCommitLatch;

        private volatile HazelcastInstance instance;

        public DelayMigrationCommit(CountDownLatch migrationCommitLatch) {
            this.migrationCommitLatch = migrationCommitLatch;
        }

        @Override
        public void onMigrationCommit(MigrationParticipant participant, MigrationInfo migrationInfo) {
            assertOpenEventually(migrationCommitLatch);
            resetInternalMigrationListener(instance);
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            this.instance = instance;
        }

    }

    private static class DelayMigrationStart
            extends InternalMigrationListener
            implements HazelcastInstanceAware {

        private final CountDownLatch migrationStartLatch;

        private volatile HazelcastInstance instance;

        public DelayMigrationStart(CountDownLatch migrationStartLatch) {
            this.migrationStartLatch = migrationStartLatch;
        }

        @Override
        public void onMigrationStart(MigrationParticipant participant, MigrationInfo migrationInfo) {
            assertOpenEventually(migrationStartLatch);
            resetInternalMigrationListener(instance);
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            this.instance = instance;
        }
    }

    private static class DelayMigrationStartOnMaster
            extends InternalMigrationListener
            implements HazelcastInstanceAware {

        private final CountDownLatch migrationStartLatch;

        private final AtomicBoolean rollback = new AtomicBoolean();

        private volatile HazelcastInstance instance;

        public DelayMigrationStartOnMaster(CountDownLatch migrationStartLatch) {
            this.migrationStartLatch = migrationStartLatch;
        }

        @Override
        public void onMigrationStart(MigrationParticipant participant, MigrationInfo migrationInfo) {
            assertOpenEventually(migrationStartLatch);
        }

        @Override
        public void onMigrationRollback(MigrationParticipant participant, MigrationInfo migrationInfo) {
            rollback.compareAndSet(false, true);
            resetInternalMigrationListener(instance);
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            this.instance = instance;
        }
    }

    private static class TerminateOtherMemberOnMigrationComplete
            extends InternalMigrationListener
            implements HazelcastInstanceAware {

        private final CountDownLatch migrationStartLatch;

        private volatile boolean rollback;

        private volatile HazelcastInstance instance;

        private volatile HazelcastInstance other;

        public TerminateOtherMemberOnMigrationComplete(CountDownLatch migrationStartLatch) {
            this.migrationStartLatch = migrationStartLatch;
        }

        @Override
        public void onMigrationStart(MigrationParticipant participant, MigrationInfo migrationInfo) {
            assertOpenEventually(migrationStartLatch);
        }

        @Override
        public void onMigrationComplete(MigrationParticipant participant, MigrationInfo migrationInfo, boolean success) {
            if (!success) {
                System.err.println("ERR: migration is not successful");
            }

            final int memberCount = instance.getCluster().getMembers().size();
            spawn(new Runnable() {
                @Override
                public void run() {
                    other.getLifecycleService().terminate();
                }
            });
            assertClusterSizeEventually(memberCount - 1, instance);
        }

        @Override
        public void onMigrationRollback(MigrationParticipant participant, MigrationInfo migrationInfo) {
            rollback = true;

            resetInternalMigrationListener(instance);
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            this.instance = instance;
        }

    }

    private static class TerminateOtherMemberOnMigrationCommit
            extends InternalMigrationListener
            implements HazelcastInstanceAware {

        private final CountDownLatch migrationStartLatch;

        private volatile HazelcastInstance instance;

        private volatile HazelcastInstance other;

        public TerminateOtherMemberOnMigrationCommit(CountDownLatch migrationStartLatch) {
            this.migrationStartLatch = migrationStartLatch;
        }

        @Override
        public void onMigrationStart(MigrationParticipant participant, MigrationInfo migrationInfo) {
            assertOpenEventually(migrationStartLatch);
        }

        @Override
        public void onMigrationCommit(MigrationParticipant participant, MigrationInfo migrationInfo) {
            final int memberCount = instance.getCluster().getMembers().size();
            spawn(new Runnable() {
                @Override
                public void run() {
                    other.getLifecycleService().terminate();
                }
            });
            assertClusterSizeEventually(memberCount - 1, instance);
            resetInternalMigrationListener(instance);
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            this.instance = instance;
        }

    }

    private static class TerminateOnMigrationCommit
            extends InternalMigrationListener
            implements HazelcastInstanceAware {

        private final CountDownLatch latch;

        private volatile HazelcastInstance instance;

        public TerminateOnMigrationCommit(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void onMigrationCommit(MigrationParticipant participant, MigrationInfo migrationInfo) {
            spawn(new Runnable() {
                @Override
                public void run() {
                    instance.getLifecycleService().terminate();
                }
            });

            assertOpenEventually(latch);
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            this.instance = instance;
        }

    }

    private static class CollectMigrationTaskOnCommit
            extends InternalMigrationListener
            implements HazelcastInstanceAware {

        private final AtomicReference<MigrationInfo> migrationInfoRef = new AtomicReference<MigrationInfo>();

        private volatile boolean commit;

        private volatile HazelcastInstance instance;

        @Override
        public void onMigrationStart(MigrationParticipant participant, MigrationInfo migrationInfo) {
            if (commit) {
                System.err.println("Ignoring new migration start: " + migrationInfo + " as participant: " + participant
                        + " since expected migration is already committed");
                return;
            }

            if (!migrationInfoRef.compareAndSet(null, migrationInfo)) {
                System.err.println("COLLECT COMMIT START FAILED! curr: " + migrationInfoRef.get() + " new: " + migrationInfo);
            }
        }

        @Override
        public void onMigrationCommit(MigrationParticipant participant, MigrationInfo migrationInfo) {
            if (commit) {
                System.err.println("Ignoring new migration commit: " + migrationInfo + " as participant: " + participant
                        + " since expected migration is already committed");
                return;
            }

            MigrationInfo collected = migrationInfoRef.get();
            commit = migrationInfo.equals(collected);
            if (commit) {
                resetInternalMigrationListener(instance);
            } else {
                System.err.println(
                        "collect commit failed! collected migration: " + collected + " rollback migration: " + migrationInfo
                                + " participant: " + participant);
            }
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            this.instance = instance;
        }

    }

    private static class CollectMigrationTaskOnRollback
            extends InternalMigrationListener
            implements HazelcastInstanceAware {

        private final AtomicReference<MigrationInfo> migrationInfoRef = new AtomicReference<MigrationInfo>();

        private volatile boolean rollback;

        private volatile HazelcastInstance instance;

        @Override
        public void onMigrationStart(MigrationParticipant participant, MigrationInfo migrationInfo) {
            if (rollback) {
                System.err.println("Ignoring new migration start: " + migrationInfo + " as participant: " + participant
                        + " since expected migration is already rolled back");
                return;
            }

            if (!migrationInfoRef.compareAndSet(null, migrationInfo)) {
                System.err.println("COLLECT ROLLBACK START FAILED! curr: " + migrationInfoRef.get() + " new: " + migrationInfo);
            }
        }

        @Override
        public void onMigrationRollback(MigrationParticipant participant, MigrationInfo migrationInfo) {
            if (rollback) {
                System.err.println("Ignoring new migration roll back: " + migrationInfo + " as participant: " + participant
                        + " since expected migration is already rolled back");
                return;
            }

            MigrationInfo collected = migrationInfoRef.get();
            rollback = migrationInfo.equals(collected);
            if (rollback) {
                resetInternalMigrationListener(instance);
            } else {
                System.err.println(
                        "collect rollback failed! collected migration: " + collected + " rollback migration: " + migrationInfo
                                + " participant: " + participant);
            }
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            this.instance = instance;
        }

    }

}
