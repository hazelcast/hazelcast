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

import static com.hazelcast.internal.partition.MigrationInfo.MigrationStatus.SUCCESS;
import static com.hazelcast.internal.partition.impl.InternalMigrationListenerTest.MigrationProgressEvent.COMMIT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MigrationCommitTest extends HazelcastTestSupport {

    private static final int PARTITION_COUNT = 2;

    private TestHazelcastInstanceFactory factory;

    @Before
    public void init() {
        factory = createHazelcastInstanceFactory(3);
    }

    @Test
    public void shouldCommitMigrationWhenMasterIsMigrationSource() {
        HazelcastInstance hz1 = factory.newHazelcastInstance(createConfig());

        Config config2 = createConfig();
        config2.setLiteMember(true);

        HazelcastInstance hz2 = factory.newHazelcastInstance(config2);

        warmUpPartitions(hz1, hz2);
        waitAllForSafeState(hz1, hz2);

        HazelcastInstance hz3 = factory.newHazelcastInstance(createConfig());

        warmUpPartitions(hz3);
        waitAllForSafeState(hz1, hz2, hz3);

        InternalPartition hz1Partition = getOwnedPartition(hz1);
        InternalPartition hz3Partition = getOwnedPartition(hz3);
        assertNotNull(hz1Partition);
        assertNotNull(hz3Partition);
        assertNotEquals(hz1Partition, hz3Partition);
        assertFalse(hz1Partition.isMigrating());
        assertFalse(hz3Partition.isMigrating());
    }

    @Test
    public void shouldCommitMigrationWhenMasterIsDestination() {
        HazelcastInstance hz1 = factory.newHazelcastInstance(createConfig());

        warmUpPartitions(hz1);

        HazelcastInstance hz2 = factory.newHazelcastInstance(createConfig());

        warmUpPartitions(hz1, hz2);
        waitAllForSafeState(hz1, hz2);

        hz2.getLifecycleService().terminate();

        assertClusterSizeEventually(1, hz1);
        waitAllForSafeState(hz1);

        InternalPartition partition0 = getPartitionService(hz1).getPartition(0);
        InternalPartition partition1 = getPartitionService(hz1).getPartition(1);

        assertEquals(getAddress(hz1), partition0.getOwnerOrNull());
        assertEquals(getAddress(hz1), partition1.getOwnerOrNull());
        assertFalse(partition0.isMigrating());
        assertFalse(partition1.isMigrating());
    }

    @Test
    public void shouldCommitMigrationWhenMasterIsNotMigrationEndpoint() {
        Config config1 = createConfig();
        config1.setLiteMember(true);

        HazelcastInstance hz1 = factory.newHazelcastInstance(config1);

        HazelcastInstance hz2 = factory.newHazelcastInstance(createConfig());

        warmUpPartitions(hz1, hz2);
        waitAllForSafeState(hz1, hz2);

        HazelcastInstance hz3 = factory.newHazelcastInstance(createConfig());

        warmUpPartitions(hz3);
        waitAllForSafeState(hz1, hz2, hz3);

        InternalPartition hz2Partition = getOwnedPartition(hz2);
        InternalPartition hz3Partition = getOwnedPartition(hz3);
        assertNotNull(hz2Partition);
        assertNotNull(hz3Partition);
        assertNotEquals(hz2Partition, hz3Partition);
        assertFalse(hz2Partition.isMigrating());
        assertFalse(hz3Partition.isMigrating());
    }

    @Test
    public void shouldRollbackMigrationWhenMasterCrashesBeforeCommit() {
        Config config1 = createConfig();
        config1.setLiteMember(true);
        // hold the migrations until all nodes join so that there will be no retries / failed migrations etc.
        CountDownLatch migrationStartLatch = new CountDownLatch(1);
        DelayMigrationStart masterListener = new DelayMigrationStart(migrationStartLatch);
        config1.addListenerConfig(new ListenerConfig(masterListener));

        HazelcastInstance hz1 = factory.newHazelcastInstance(config1);

        HazelcastInstance hz2 = factory.newHazelcastInstance(createConfig());

        warmUpPartitions(hz1, hz2);
        waitAllForSafeState(hz1, hz2);

        Config config3 = createConfig();
        final TerminateOtherMemberOnMigrationComplete listener3
                = new TerminateOtherMemberOnMigrationComplete(migrationStartLatch);
        listener3.other = hz1;
        config3.addListenerConfig(new ListenerConfig(listener3));
        HazelcastInstance hz3 = factory.newHazelcastInstance(config3);

        assertClusterSizeEventually(3, hz2);
        assertClusterSize(3, hz1, hz3);

        migrationStartLatch.countDown();

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
        Config config1 = createConfig();
        config1.setLiteMember(true);
        // hold the migrations until all nodes join so that there will be no retries / failed migrations etc.
        CountDownLatch migrationStartLatch = new CountDownLatch(1);
        TerminateOtherMemberOnMigrationComplete masterListener = new TerminateOtherMemberOnMigrationComplete(
                migrationStartLatch);
        config1.addListenerConfig(new ListenerConfig(masterListener));

        HazelcastInstance hz1 = factory.newHazelcastInstance(config1);

        HazelcastInstance hz2 = factory.newHazelcastInstance(createConfig());

        warmUpPartitions(hz1, hz2);
        waitAllForSafeState(hz1, hz2);

        HazelcastInstance hz3 = factory.newHazelcastInstance(createConfig());
        masterListener.other = hz3;

        assertClusterSize(3, hz1, hz3);
        assertClusterSizeEventually(3, hz2);

        migrationStartLatch.countDown();

        waitAllForSafeState(hz1, hz2);

        InternalPartition partition0 = getPartitionService(hz2).getPartition(0);
        InternalPartition partition1 = getPartitionService(hz2).getPartition(1);

        assertEquals(getAddress(hz2), partition0.getOwnerOrNull());
        assertEquals(getAddress(hz2), partition1.getOwnerOrNull());
        assertFalse(partition0.isMigrating());
        assertFalse(partition1.isMigrating());
        assertTrue(masterListener.rollback);
    }

    @Test
    public void shouldCommitMigrationWhenMasterCrashesAfterDestinationCommit() {
        Config config1 = createConfig();
        config1.setLiteMember(true);
        // hold the migrations until all nodes join so that there will be no retries / failed migrations etc.
        CountDownLatch migrationStartLatch = new CountDownLatch(1);
        DelayMigrationStart masterListener = new DelayMigrationStart(migrationStartLatch);
        config1.addListenerConfig(new ListenerConfig(masterListener));

        HazelcastInstance hz1 = factory.newHazelcastInstance(config1);

        Config config2 = createConfig();
        final CollectMigrationTaskOnCommit sourceListener = new CollectMigrationTaskOnCommit(migrationStartLatch);
        config2.addListenerConfig(new ListenerConfig(sourceListener));
        HazelcastInstance hz2 = factory.newHazelcastInstance(config2);

        warmUpPartitions(hz1, hz2);
        waitAllForSafeState(hz1, hz2);

        Config config3 = createConfig();

        TerminateOtherMemberOnMigrationCommit destinationListener = new TerminateOtherMemberOnMigrationCommit();
        destinationListener.other = hz1;
        config3.addListenerConfig(new ListenerConfig(destinationListener));
        HazelcastInstance hz3 = factory.newHazelcastInstance(config3);

        assertClusterSize(3, hz1, hz3);
        assertClusterSizeEventually(3, hz2);

        migrationStartLatch.countDown();

        waitAllForSafeState(hz2, hz3);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertTrue(sourceListener.commit);
            }
        });

        InternalPartition hz2Partition = getOwnedPartition(hz2);
        InternalPartition hz3Partition = getOwnedPartition(hz3);
        assertNotNull(hz2Partition);
        assertNotNull(hz3Partition);
        assertNotEquals(hz2Partition, hz3Partition);
        assertFalse(hz2Partition.isMigrating());
        assertFalse(hz3Partition.isMigrating());
    }

    @Test
    public void shouldCommitMigrationWhenSourceFailsDuringCommit() {
        Config config1 = createConfig();
        config1.setLiteMember(true);
        // hold the migrations until all nodes join so that there will be no retries / failed migrations etc.
        CountDownLatch migrationStartLatch = new CountDownLatch(1);
        TerminateOtherMemberOnMigrationComplete masterListener = new TerminateOtherMemberOnMigrationComplete(migrationStartLatch);
        config1.addListenerConfig(new ListenerConfig(masterListener));

        HazelcastInstance hz1 = factory.newHazelcastInstance(config1);

        HazelcastInstance hz2 = factory.newHazelcastInstance(createConfig());

        warmUpPartitions(hz1, hz2);
        waitAllForSafeState(hz1, hz2);

        Config config3 = createConfig();
        InternalMigrationListenerImpl targetListener = new InternalMigrationListenerImpl();
        config3.addListenerConfig(new ListenerConfig(targetListener));
        HazelcastInstance hz3 = factory.newHazelcastInstance(config3);

        masterListener.other = hz2;

        assertClusterSize(3, hz1, hz3);
        assertClusterSizeEventually(3, hz2);

        migrationStartLatch.countDown();

        waitAllForSafeState(hz1, hz3);

        InternalPartition partition0 = getPartitionService(hz3).getPartition(0);
        InternalPartition partition1 = getPartitionService(hz3).getPartition(1);

        assertEquals(getAddress(hz3), partition0.getOwnerOrNull());
        assertEquals(getAddress(hz3), partition1.getOwnerOrNull());
        assertFalse(partition0.isMigrating());
        assertFalse(partition1.isMigrating());
        assertFalse(masterListener.rollback);
        List<MigrationProgressNotification> notifications = targetListener.getNotifications();
        assertFalse(notifications.isEmpty());
        assertEquals(COMMIT, notifications.get(notifications.size() - 1).event);
    }

    @Test
    public void shouldRollbackMigrationWhenDestinationCrashesDuringCommit() {
        Config config1 = createConfig();
        config1.setLiteMember(true);
        // hold the migrations until all nodes join so that there will be no retries / failed migrations etc.
        CountDownLatch migrationStartLatch = new CountDownLatch(1);
        DelayMigrationStartOnMaster masterListener = new DelayMigrationStartOnMaster(migrationStartLatch);
        config1.addListenerConfig(new ListenerConfig(masterListener));

        HazelcastInstance hz1 = factory.newHazelcastInstance(config1);

        HazelcastInstance hz2 = factory.newHazelcastInstance(createConfig());

        warmUpPartitions(hz1, hz2);
        waitAllForSafeState(hz1, hz2);

        CountDownLatch terminationLatch = new CountDownLatch(1);
        TerminateOnMigrationCommit memberListener = new TerminateOnMigrationCommit(terminationLatch);
        Config config3 = createConfig();
        config3.addListenerConfig(new ListenerConfig(memberListener));
        HazelcastInstance hz3 = factory.newHazelcastInstance(config3);

        warmUpPartitions(hz1, hz2, hz3);

        assertClusterSize(3, hz1, hz3);
        assertClusterSizeEventually(3, hz2);

        migrationStartLatch.countDown();

        waitAllForSafeState(hz1, hz2);

        InternalPartition partition0 = getPartitionService(hz1).getPartition(0);
        InternalPartition partition1 = getPartitionService(hz1).getPartition(1);

        assertEquals(getAddress(hz2), partition0.getOwnerOrNull());
        assertEquals(getAddress(hz2), partition1.getOwnerOrNull());
        assertFalse(partition0.isMigrating());
        assertFalse(partition1.isMigrating());
        assertTrue(masterListener.rollback.get());

        terminationLatch.countDown();
    }

    @Test
    public void shouldRetryMigrationIfParticipantPartitionTableVersionFallsBehind() {
        Config config1 = createConfig();
        config1.setLiteMember(true);
        // hold the migrations until all nodes join so that there will be no retries / failed migrations etc.
        CountDownLatch migrationStartLatch = new CountDownLatch(1);
        final IncrementPartitionTableOnMigrationStart masterListener
                = new IncrementPartitionTableOnMigrationStart(migrationStartLatch);
        config1.addListenerConfig(new ListenerConfig(masterListener));

        HazelcastInstance hz1 = factory.newHazelcastInstance(config1);

        HazelcastInstance hz2 = factory.newHazelcastInstance(createConfig());

        warmUpPartitions(hz1, hz2);
        waitAllForSafeState(hz1, hz2);

        Config config3 = createConfig();
        InternalMigrationListenerImpl targetListener = new InternalMigrationListenerImpl();
        config3.addListenerConfig(new ListenerConfig(targetListener));
        HazelcastInstance hz3 = factory.newHazelcastInstance(config3);

        assertClusterSize(3, hz1, hz3);
        assertClusterSizeEventually(3, hz2);

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
        Config config1 = createConfig();
        config1.setLiteMember(true);
        // hold the migrations until all nodes join so that there will be no retries / failed migrations etc.
        CountDownLatch migrationStartLatch = new CountDownLatch(1);
        final CollectMigrationTaskOnCommit masterListener = new CollectMigrationTaskOnCommit(migrationStartLatch);
        config1.addListenerConfig(new ListenerConfig(masterListener));

        HazelcastInstance hz1 = factory.newHazelcastInstance(config1);

        InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) getPartitionService(hz1);
        final MigrationManager migrationManager = partitionService.getMigrationManager();

        HazelcastInstance hz2 = factory.newHazelcastInstance(createConfig());

        warmUpPartitions(hz1, hz2);
        waitAllForSafeState(hz1, hz2);

        HazelcastInstance hz3 = factory.newHazelcastInstance(createConfig());

        assertClusterSize(3, hz1, hz3);
        assertClusterSizeEventually(3, hz2);

        migrationStartLatch.countDown();

        waitAllForSafeState(hz1, hz2, hz3);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertTrue(masterListener.commit);
                assertTrue(migrationManager.getCompletedMigrationsCopy().isEmpty());
            }
        });
    }

    @Test
    public void shouldNotEvictCompletedMigrationsWhenSomeMembersDoNotAckPublishedPartitionTableAfterSuccessfulMigration() {
        Config config1 = createConfig();
        config1.setLiteMember(true);
        // hold the migrations until all nodes join so that there will be no retries / failed migrations etc.
        CountDownLatch migrationStartLatch = new CountDownLatch(1);
        config1.addListenerConfig(new ListenerConfig(new DelayMigrationStart(migrationStartLatch)));

        final HazelcastInstance hz1 = factory.newHazelcastInstance(config1);

        Config config2 = createConfig();
        CountDownLatch migrationCommitLatch = new CountDownLatch(1);
        config2.addListenerConfig(new ListenerConfig(new DelayMigrationCommit(migrationCommitLatch)));
        HazelcastInstance hz2 = factory.newHazelcastInstance(config2);

        warmUpPartitions(hz1, hz2);
        waitAllForSafeState(hz1, hz2);

        final HazelcastInstance hz3 = factory.newHazelcastInstance(createConfig());

        assertClusterSize(3, hz1, hz3);
        assertClusterSizeEventually(3, hz2);

        migrationStartLatch.countDown();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) getPartitionService(hz1);
                boolean found = false;
                for (MigrationInfo migrationInfo : partitionService.getMigrationManager().getCompletedMigrationsCopy()) {
                    if (migrationInfo.getStatus() == SUCCESS && migrationInfo.getDestination().equals(getAddress(hz3))) {
                        found = true;
                    }
                }

                assertTrue(found);
            }
        });

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) getPartitionService(hz1);
                assertFalse(partitionService.getMigrationManager().getCompletedMigrationsCopy().isEmpty());
            }
        }, 10);

        migrationCommitLatch.countDown();
    }

    private Config createConfig() {
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_MAX_PARALLEL_REPLICATIONS.getName(), "0");
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), String.valueOf(PARTITION_COUNT));
        return config;
    }

    private InternalPartition getOwnedPartition(HazelcastInstance instance) {
        InternalPartitionService partitionService = getPartitionService(instance);
        Address address = getAddress(instance);
        if (address.equals(partitionService.getPartitionOwner(0))) {
            return partitionService.getPartition(0);
        } else if (address.equals(partitionService.getPartitionOwner(1))) {
            return partitionService.getPartition(1);
        }
        return null;
    }

    static void resetInternalMigrationListener(HazelcastInstance instance) {
        InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) getPartitionService(instance);
        partitionService.resetInternalMigrationListener();
    }

    private static class IncrementPartitionTableOnMigrationStart
            extends InternalMigrationListener implements HazelcastInstanceAware {

        private final AtomicReference<MigrationInfo> migrationInfoRef = new AtomicReference<MigrationInfo>();

        private final CountDownLatch migrationStartLatch;

        private HazelcastInstance instance;

        private volatile boolean failed;

        IncrementPartitionTableOnMigrationStart(CountDownLatch migrationStartLatch) {
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
                InternalPartitionServiceImpl partitionService
                        = (InternalPartitionServiceImpl) getPartitionService(instance);
                PartitionStateManager partitionStateManager = partitionService.getPartitionStateManager();
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

    private static class DelayMigrationCommit extends InternalMigrationListener implements HazelcastInstanceAware {

        private final CountDownLatch migrationCommitLatch;

        private volatile HazelcastInstance instance;

        DelayMigrationCommit(CountDownLatch migrationCommitLatch) {
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

    public static class DelayMigrationStart extends InternalMigrationListener implements HazelcastInstanceAware {

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

    private static class DelayMigrationStartOnMaster extends InternalMigrationListener implements HazelcastInstanceAware {

        private final AtomicBoolean rollback = new AtomicBoolean();

        private final CountDownLatch migrationStartLatch;

        private volatile HazelcastInstance instance;

        DelayMigrationStartOnMaster(CountDownLatch migrationStartLatch) {
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
            extends InternalMigrationListener implements HazelcastInstanceAware {

        private final CountDownLatch migrationStartLatch;

        private volatile boolean rollback;
        private volatile HazelcastInstance instance;
        private volatile HazelcastInstance other;

        TerminateOtherMemberOnMigrationComplete(CountDownLatch migrationStartLatch) {
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

            int memberCount = instance.getCluster().getMembers().size();
            spawn(new Runnable() {
                @Override
                public void run() {
                    other.getLifecycleService().terminate();
                }
            });
            assertClusterSizeEventually(memberCount - 1, instance);
        }

        @Override
        public void onMigrationCommit(MigrationParticipant participant, MigrationInfo migrationInfo) {
            System.out.println(getAddress(instance) + " > commit " + migrationInfo + " as " + participant);
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
            extends InternalMigrationListener implements HazelcastInstanceAware {

        private volatile HazelcastInstance instance;
        private volatile HazelcastInstance other;

        @Override
        public void onMigrationCommit(MigrationParticipant participant, MigrationInfo migrationInfo) {
            int memberCount = instance.getCluster().getMembers().size();
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

    private static class TerminateOnMigrationCommit extends InternalMigrationListener implements HazelcastInstanceAware {

        private final CountDownLatch latch;

        private volatile HazelcastInstance instance;

        TerminateOnMigrationCommit(CountDownLatch latch) {
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

    private static class CollectMigrationTaskOnCommit extends InternalMigrationListener implements HazelcastInstanceAware {

        private final CountDownLatch migrationStartLatch;
        private final AtomicReference<MigrationInfo> migrationInfoRef = new AtomicReference<MigrationInfo>();

        private volatile boolean commit;
        private volatile HazelcastInstance instance;

        public CollectMigrationTaskOnCommit(CountDownLatch migrationStartLatch) {
            this.migrationStartLatch = migrationStartLatch;
        }

        @Override
        public void onMigrationStart(MigrationParticipant participant, MigrationInfo migrationInfo) {
            assertOpenEventually(migrationStartLatch);
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
}
