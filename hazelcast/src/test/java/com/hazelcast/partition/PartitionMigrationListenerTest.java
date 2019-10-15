/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.partition;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.partition.MigrationStateImpl;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.MigrationCommitTest.DelayMigrationStart;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.internal.partition.impl.MigrationInterceptor;
import com.hazelcast.internal.partition.impl.MigrationStats;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.UUID;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PartitionMigrationListenerTest extends HazelcastTestSupport {

    @Test
    public void testMigrationStats_whenMigrationProcessCompletes() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hz1 = factory.newHazelcastInstance();
        warmUpPartitions(hz1);

        EventCollectingMigrationListener listener = new EventCollectingMigrationListener();
        hz1.getPartitionService().addMigrationListener(listener);

        factory.newHazelcastInstance();

        MigrationEventsPack eventsPack = listener.ensureAndGetSingleEventPack();
        assertMigrationProcessCompleted(eventsPack);
        assertMigrationProcessEventsConsistent(eventsPack);
        assertMigrationEventsConsistentWithResult(eventsPack);
    }

    @Test
    public void testMigrationStats_whenMigrationProcessRestarts() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        Config config = new Config();
        int partitionCount = 100;
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), String.valueOf(partitionCount));

        HazelcastInstance hz = factory.newHazelcastInstance(config);
        warmUpPartitions(hz);

        InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) getPartitionService(hz);
        AtomicReference<HazelcastInstance> newInstanceRef = new AtomicReference<>();
        partitionService.setMigrationInterceptor(new MigrationInterceptor() {
            @Override
            public void onMigrationComplete(MigrationParticipant participant, MigrationInfo migration, boolean success) {
                MigrationStats stats = partitionService.getMigrationManager().getStats();
                if (stats.getPlannedMigrations() - stats.getCompletedMigrations() < 20) {
                    // start a new member to restart migrations
                    partitionService.resetMigrationInterceptor();
                    HazelcastInstance hz = factory.newHazelcastInstance(config);
                    assertClusterSize(3, hz);
                    newInstanceRef.set(hz);
                }
            }
        });

        EventCollectingMigrationListener listener = new EventCollectingMigrationListener();
        hz.getPartitionService().addMigrationListener(listener);

        // trigger migrations
        factory.newHazelcastInstance(config);

        // await until 3rd member joins
        assertClusterSizeEventually(3, hz);
        assertTrueEventually(() -> assertNotNull(newInstanceRef.get()));

        List<MigrationEventsPack> eventsPackList = listener.ensureAndGetEventPacks(2);

        // 1st migration process, which finishes without completing all migration tasks
        MigrationEventsPack firstEventsPack = eventsPackList.get(0);
        assertMigrationProcessCompleted(firstEventsPack);
        MigrationState migrationResult = firstEventsPack.migrationProcessCompleted;
        assertThat(migrationResult.getCompletedMigrations(), lessThan(migrationResult.getPlannedMigrations()));
        assertThat(migrationResult.getRemainingMigrations(), greaterThan(0));
        assertMigrationEventsConsistentWithResult(firstEventsPack);

        // 2nd migration process finishes by consuming all migration tasks
        MigrationEventsPack secondEventsPack = eventsPackList.get(1);
        assertMigrationProcessCompleted(secondEventsPack);
        assertMigrationProcessEventsConsistent(secondEventsPack);
        assertMigrationEventsConsistentWithResult(secondEventsPack);
    }

    @Test
    public void testMigrationStats_afterPromotions() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hz1 = factory.newHazelcastInstance();
        HazelcastInstance hz2 = factory.newHazelcastInstance();
        HazelcastInstance hz3 = factory.newHazelcastInstance();
        warmUpPartitions(hz1, hz2, hz3);

        EventCollectingMigrationListener listener = new EventCollectingMigrationListener();
        hz1.getPartitionService().addMigrationListener(listener);

        hz3.getLifecycleService().terminate();

        // 2 promotions on each node + 1 repartitioning to create missing backups
        for (MigrationEventsPack eventsPack : listener.ensureAndGetEventPacks(3)) {
            assertMigrationProcessCompleted(eventsPack);
            assertMigrationProcessEventsConsistent(eventsPack);
            assertMigrationEventsConsistentWithResult(eventsPack);
        }
    }

    @Test
    public void testMigrationStats_afterPartitionsLost_when_NO_MIGRATION() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        Config config = new Config().setProperty(GroupProperty.PARTITION_COUNT.getName(), "2000");
        HazelcastInstance[] instances = factory.newInstances(config, 10);
        assertClusterSizeEventually(instances.length, instances);
        warmUpPartitions(instances);

        EventCollectingMigrationListener listener = new EventCollectingMigrationListener();
        instances[0].getPartitionService().addMigrationListener(listener);

        changeClusterStateEventually(instances[0], ClusterState.PASSIVE);

        for (int i = 3; i < instances.length; i++) {
            instances[i].getLifecycleService().terminate();
        }

        changeClusterStateEventually(instances[0], ClusterState.NO_MIGRATION);

        // 3 promotions on each remaining node + 1 to assign owners for lost partitions
        for (MigrationEventsPack eventsPack : listener.ensureAndGetEventPacks(4)) {
            assertMigrationProcessCompleted(eventsPack);
            assertMigrationProcessEventsConsistent(eventsPack);
            assertMigrationEventsConsistentWithResult(eventsPack);
        }
    }

    private void assertMigrationProcessCompleted(MigrationEventsPack eventsPack) {
        assertTrueEventually(() -> assertNotNull(eventsPack.migrationProcessCompleted));
    }

    private void assertMigrationProcessEventsConsistent(MigrationEventsPack eventsPack) {
        MigrationState migrationPlan = eventsPack.migrationProcessStarted;
        assertThat(migrationPlan.getStartTime(), greaterThan(0L));
        assertThat(migrationPlan.getPlannedMigrations(), greaterThan(0));

        MigrationState migrationResult = eventsPack.migrationProcessCompleted;
        assertEquals(migrationPlan.getStartTime(), migrationResult.getStartTime());
        assertThat(migrationResult.getTotalElapsedTime(), greaterThanOrEqualTo(0L));
        assertEquals(migrationPlan.getPlannedMigrations(), migrationResult.getCompletedMigrations());
        assertEquals(0, migrationResult.getRemainingMigrations());
    }

    private void assertMigrationEventsConsistentWithResult(MigrationEventsPack eventsPack) {
        MigrationState migrationResult = eventsPack.migrationProcessCompleted;
        List<ReplicaMigrationEvent> migrationsCompleted = eventsPack.migrationsCompleted;

        assertEquals(migrationResult.getCompletedMigrations(), migrationsCompleted.size());

        MigrationState lastProgress = null;
        for (ReplicaMigrationEvent event : migrationsCompleted) {
            assertTrue(event.toString(), event.isSuccess());
            MigrationState progress = event.getMigrationState();
            assertEquals(migrationResult.getStartTime(), progress.getStartTime());
            assertEquals(migrationResult.getPlannedMigrations(), progress.getPlannedMigrations());

            if (lastProgress != null) {
                assertEquals(lastProgress.getCompletedMigrations(), progress.getCompletedMigrations() - 1);
                assertEquals(lastProgress.getRemainingMigrations(), progress.getRemainingMigrations() + 1);
                assertThat(progress.getTotalElapsedTime(),
                        greaterThanOrEqualTo(lastProgress.getTotalElapsedTime() + event.getElapsedTime()));
            }
            lastProgress = progress;
        }

        assertNotNull(lastProgress);
        assertEquals(migrationResult.getCompletedMigrations(), lastProgress.getCompletedMigrations());
        assertThat(migrationResult.getTotalElapsedTime(), greaterThanOrEqualTo(lastProgress.getTotalElapsedTime()));
    }

    @Test
    public void testMigrationListenerCalledOnlyOnceWhenMigrationHappens() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        Config config = new Config();
        // even partition count to make migration count deterministic
        int partitionCount = 10;
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), String.valueOf(partitionCount));

        // hold the migrations until all nodes join so that there will be no retries / failed migrations etc.
        CountDownLatch migrationStartLatch = new CountDownLatch(1);
        config.addListenerConfig(new ListenerConfig(new DelayMigrationStart(migrationStartLatch)));

        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        warmUpPartitions(instance1);

        CountingMigrationListener migrationListener = new CountingMigrationListener(partitionCount);
        instance1.getPartitionService().addMigrationListener(migrationListener);

        HazelcastInstance instance2 = factory.newHazelcastInstance(config);

        migrationStartLatch.countDown();

        waitAllForSafeState(instance2, instance1);

        assertTrueEventually(() -> {
            assertEquals(1, migrationListener.migrationStarted.get());
            assertEquals(1, migrationListener.migrationCompleted.get());

            int completed = getTotal(migrationListener.replicaMigrationCompleted);
            int failed = getTotal(migrationListener.replicaMigrationFailed);

            assertEquals(partitionCount, completed);
            assertEquals(0, failed);
        });

        assertAllLessThanOrEqual(migrationListener.replicaMigrationCompleted, 1);
    }

    private int getTotal(AtomicInteger[] integers) {
        int total = 0;
        for (AtomicInteger count : integers) {
            total += count.get();
        }
        return total;
    }

    @Test(expected = NullPointerException.class)
    public void testAddMigrationListener_whenNullListener() {
        HazelcastInstance hz = createHazelcastInstance();
        PartitionService partitionService = hz.getPartitionService();

        partitionService.addMigrationListener(null);
    }

    @Test
    public void testAddMigrationListener_whenListenerRegisteredTwice() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance hz1 = factory.newHazelcastInstance();
        PartitionService partitionService = hz1.getPartitionService();

        MigrationListener listener = mock(MigrationListener.class);

        UUID id1 = partitionService.addMigrationListener(listener);
        UUID id2 = partitionService.addMigrationListener(listener);

        // first we check if the registration id's are different
        assertNotEquals(id1, id2);
    }

    @Test(expected = NullPointerException.class)
    public void testRemoveMigrationListener_whenNullListener() {
        HazelcastInstance hz = createHazelcastInstance();
        PartitionService partitionService = hz.getPartitionService();

        partitionService.removeMigrationListener(null);
    }

    @Test
    public void testRemoveMigrationListener_whenNonExistingRegistrationId() {
        HazelcastInstance hz = createHazelcastInstance();
        PartitionService partitionService = hz.getPartitionService();

        boolean result = partitionService.removeMigrationListener(UuidUtil.newUnsecureUUID());

        assertFalse(result);
    }

    @Test
    public void testRemoveMigrationListener() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance hz1 = factory.newHazelcastInstance();
        PartitionService partitionService = hz1.getPartitionService();

        MigrationListener listener = mock(MigrationListener.class);

        UUID id = partitionService.addMigrationListener(listener);
        boolean removed = partitionService.removeMigrationListener(id);

        assertTrue(removed);

        // now we add a member
        HazelcastInstance hz2 = factory.newHazelcastInstance();
        warmUpPartitions(hz1, hz2);

        // and verify that the listener isn't called.
        verify(listener, never()).migrationStarted(any(MigrationStateImpl.class));
        verify(listener, never()).replicaMigrationCompleted(any(ReplicaMigrationEvent.class));
    }


    @SuppressWarnings("SameParameterValue")
    private void assertAllLessThanOrEqual(AtomicInteger[] integers, int expected) {
        for (AtomicInteger integer : integers) {
            assertThat(integer.get(), Matchers.lessThanOrEqualTo(expected));
        }
    }

    private static class CountingMigrationListener implements MigrationListener {

        final AtomicInteger migrationStarted;
        final AtomicInteger migrationCompleted;
        final AtomicInteger[] replicaMigrationCompleted;
        final AtomicInteger[] replicaMigrationFailed;

        CountingMigrationListener(int partitionCount) {
            migrationStarted = new AtomicInteger();
            migrationCompleted = new AtomicInteger();
            replicaMigrationCompleted = new AtomicInteger[partitionCount];
            replicaMigrationFailed = new AtomicInteger[partitionCount];
            for (int i = 0; i < partitionCount; i++) {
                replicaMigrationCompleted[i] = new AtomicInteger();
                replicaMigrationFailed[i] = new AtomicInteger();
            }
        }

        @Override
        public void migrationStarted(MigrationState state) {
            migrationStarted.incrementAndGet();
        }

        @Override
        public void migrationFinished(MigrationState state) {
            migrationCompleted.incrementAndGet();
        }

        @Override
        public void replicaMigrationCompleted(ReplicaMigrationEvent event) {
            assertTrue(event.isSuccess());
            replicaMigrationCompleted[event.getPartitionId()].incrementAndGet();
        }

        @Override
        public void replicaMigrationFailed(ReplicaMigrationEvent event) {
            assertFalse(event.isSuccess());
            replicaMigrationFailed[event.getPartitionId()].incrementAndGet();
        }
    }

    // Migration events are published and processed in order in a single event thread.
    // So we can rely on that here...
    private static class EventCollectingMigrationListener implements MigrationListener {
        final List<MigrationEventsPack> allEventPacks = Collections.synchronizedList(new ArrayList<>());
        volatile MigrationEventsPack currentEvents;

        @Override
        public void migrationStarted(MigrationState state) {
            assertNull(currentEvents);
            currentEvents = new MigrationEventsPack();
            currentEvents.migrationProcessStarted = state;
        }

        @Override
        public void migrationFinished(MigrationState state) {
            assertNotNull(currentEvents);
            currentEvents.migrationProcessCompleted = state;
            allEventPacks.add(currentEvents);
            currentEvents = null;
        }

        @Override
        public void replicaMigrationCompleted(ReplicaMigrationEvent event) {
            assertNotNull(currentEvents);
            currentEvents.migrationsCompleted.add(event);
        }

        @Override
        public void replicaMigrationFailed(ReplicaMigrationEvent event) {
            assertNotNull(currentEvents);
            currentEvents.migrationsCompleted.add(event);
        }

        List<MigrationEventsPack> ensureAndGetEventPacks(int count) {
            awaitEventPacksComplete(count);
            return allEventPacks.subList(0, count);
        }

        MigrationEventsPack ensureAndGetSingleEventPack() {
            return ensureAndGetEventPacks(1).get(0);
        }

        void awaitEventPacksComplete(int count) {
            assertTrueEventually(() -> {
                assertThat(allEventPacks.size(), greaterThanOrEqualTo(count));
                assertNull(currentEvents);
            });
        }

        void reset() {
            allEventPacks.clear();
            currentEvents = null;
        }
    }

    private static class MigrationEventsPack {
        volatile MigrationState migrationProcessStarted;
        volatile MigrationState migrationProcessCompleted;
        final List<ReplicaMigrationEvent> migrationsCompleted = Collections.synchronizedList(new ArrayList<>());
    }
}
