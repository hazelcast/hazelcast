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

package com.hazelcast.partition;

import com.google.common.base.Stopwatch;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.metrics.MetricDescriptorConstants;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.partition.MigrationStateImpl;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.MigrationInterceptor;
import com.hazelcast.internal.partition.impl.MigrationStats;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nullable;
import java.text.MessageFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static com.hazelcast.test.Accessors.getNode;
import static com.hazelcast.test.Accessors.getPartitionService;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PartitionMigrationListenerTest extends HazelcastTestSupport {
    private static final ILogger LOGGER = Logger.getLogger(PartitionMigrationListenerTest.class);

    @Test
    public void testMigrationStats_whenMigrationProcessCompletes() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hz1 = createPausedMigrationCluster(factory, null);

        EventCollectingMigrationListener listener = new EventCollectingMigrationListener(false);
        hz1.getPartitionService().addMigrationListener(listener);

        HazelcastInstance hz2 = factory.newHazelcastInstance();
        // Back to ACTIVE
        changeClusterStateEventually(hz2, ClusterState.ACTIVE);

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
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), String.valueOf(partitionCount));
        config.setProperty(ClusterProperty.PARTITION_MAX_PARALLEL_MIGRATIONS.getName(), String.valueOf(1));

        HazelcastInstance hz1 = createPausedMigrationCluster(factory, config);

        InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) getPartitionService(hz1);
        AtomicReference<HazelcastInstance> newInstanceRef = new AtomicReference<>();
        partitionService.setMigrationInterceptor(new MigrationInterceptor() {
            @Override
            public void onMigrationComplete(MigrationParticipant participant, MigrationInfo migration,
                                            boolean success) {
                MigrationStats stats = partitionService.getMigrationManager().getStats();
                if (stats.getRemainingMigrations() < 50) {
                    // start a new member to restart migrations
                    partitionService.resetMigrationInterceptor();
                    HazelcastInstance hz = factory.newHazelcastInstance(config);
                    assertClusterSize(3, hz);
                    newInstanceRef.set(hz);
                }
            }
        });

        EventCollectingMigrationListener listener = new EventCollectingMigrationListener(true);
        hz1.getPartitionService().addMigrationListener(listener);

        // trigger migrations
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);

        // Back to ACTIVE
        changeClusterStateEventually(hz2, ClusterState.ACTIVE);

        // await until 3rd member joins
        assertClusterSizeEventually(3, hz1);
        assertTrueEventually(() -> assertNotNull(newInstanceRef.get()));

        List<MigrationEventsPack> eventsPackList = listener.ensureAndGetEventPacks(2);

        // 1st migration process, which finishes without completing all migration tasks
        MigrationEventsPack firstEventsPack = eventsPackList.get(0);
        assertMigrationProcessCompleted(firstEventsPack);
        MigrationState migrationResult = firstEventsPack.migrationProcessCompleted;
        assertThat(migrationResult.getCompletedMigrations()).isLessThan(migrationResult.getPlannedMigrations());
        assertThat(migrationResult.getRemainingMigrations()).isGreaterThan(0);
        assertMigrationEventsConsistentWithResult(firstEventsPack);

        // 2nd migration process finishes by consuming all migration tasks
        MigrationEventsPack secondEventsPack = eventsPackList.get(1);

        if (secondEventsPack.migrationProcessCompleted.getCompletedMigrations() == 1
                && !secondEventsPack.migrationsCompleted.get(0).isSuccess()) {
            // There is a failed migration process
            // because migrations restarted before 3rd member is ready.
            // This migration process is failed immediately
            // and we expect a third migration process.
            secondEventsPack = listener.ensureAndGetEventPacks(3).get(2);
        }

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
        waitAllForSafeState(Arrays.asList(hz1, hz2, hz3));

        EventCollectingMigrationListener listener = new EventCollectingMigrationListener(false);
        hz1.getPartitionService().addMigrationListener(listener);

        hz3.getLifecycleService().terminate();

        // 2 promotions on each node + 1 repartitioning to create missing backups
        for (MigrationEventsPack eventsPack : listener.ensureAndGetEventPacks(3)) {
            assertMigrationProcessCompleted(eventsPack);
            assertMigrationProcessEventsConsistent(eventsPack);
            assertMigrationEventsConsistentWithResult(eventsPack);
        }
    }

    public static void assertMigrationProcessCompleted(MigrationEventsPack eventsPack) {
        assertTrueEventually(() -> assertNotNull(eventsPack.migrationProcessCompleted));
    }

    public static void assertMigrationProcessEventsConsistent(MigrationEventsPack eventsPack) {
        MigrationState migrationPlan = eventsPack.migrationProcessStarted;
        assertThat(migrationPlan.getStartTime()).isGreaterThan(0L);
        assertThat(migrationPlan.getPlannedMigrations()).isGreaterThan(0);

        MigrationState migrationResult = eventsPack.migrationProcessCompleted;
        assertEquals(migrationPlan.getStartTime(), migrationResult.getStartTime());
        assertThat(migrationResult.getTotalElapsedTime()).isGreaterThanOrEqualTo(0L);
        assertEquals(migrationPlan.getPlannedMigrations(), migrationResult.getCompletedMigrations());
        assertEquals(0, migrationResult.getRemainingMigrations());
    }

    public static void assertMigrationEventsConsistentWithResult(MigrationEventsPack eventsPack) {
        MigrationState migrationResult = eventsPack.migrationProcessCompleted;
        List<ReplicaMigrationEvent> migrationsCompleted = eventsPack.migrationsCompleted;

        assertEquals(migrationResult.getCompletedMigrations(), migrationsCompleted.size());

        MigrationState completed = null;
        for (ReplicaMigrationEvent event : migrationsCompleted) {
            assertTrue(event.toString(), event.isSuccess());
            MigrationState progress = event.getMigrationState();
            assertEquals(migrationResult.getStartTime(), progress.getStartTime());
            assertEquals(migrationResult.getPlannedMigrations(), progress.getPlannedMigrations());

            assertThat(progress.getCompletedMigrations()).isGreaterThan(0);
            assertThat(progress.getCompletedMigrations()).isLessThanOrEqualTo(migrationResult.getPlannedMigrations());
            assertThat(progress.getCompletedMigrations()).isLessThanOrEqualTo(migrationResult.getCompletedMigrations());
            assertThat(progress.getRemainingMigrations()).isLessThan(migrationResult.getPlannedMigrations());
            assertThat(progress.getRemainingMigrations()).isGreaterThanOrEqualTo(migrationResult.getRemainingMigrations());

            if (progress.getCompletedMigrations() == migrationResult.getCompletedMigrations()) {
                completed = progress;
            }
        }

        assertNotNull(completed);
        assertThat(migrationResult.getTotalElapsedTime()).isGreaterThanOrEqualTo(completed.getTotalElapsedTime());
    }

    @Test
    public void testMigrationListenerCalledOnlyOnceWhenMigrationHappens() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        Config config = new Config();
        // even partition count to make migration count deterministic
        int partitionCount = 10;
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), String.valueOf(partitionCount));

        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        warmUpPartitions(instance1);

        // Change to NO_MIGRATION to prevent repartitioning
        // before 2nd member started and ready.
        instance1.getCluster().changeClusterState(ClusterState.NO_MIGRATION);

        CountingMigrationListener migrationListener = new CountingMigrationListener(partitionCount);
        instance1.getPartitionService().addMigrationListener(migrationListener);

        HazelcastInstance instance2 = factory.newHazelcastInstance(config);

        changeClusterStateEventually(instance2, ClusterState.ACTIVE);

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

    /**
     * @see <a href="https://hazelcast.atlassian.net/browse/HZ-2651">HZ-2651 - MigrationListener: Difference between
     * "wall clock elapsed time" and the totalElasedTime API</a>
     */
    @Test
    public void testMigrationListenerElapsedTime() throws InterruptedException, ExecutionException, TimeoutException {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        // Use an arbitrarily high number of partitions to exacerbate the issue
        final Config config = new Config().setProperty(ClusterProperty.PARTITION_COUNT.getName(), String.valueOf(1000));

        final HazelcastInstance hz1 = createPausedMigrationCluster(factory, config);

        final Stopwatch migrationTimer = Stopwatch.createUnstarted();
        final CompletableFuture<Duration> migrationDurationReference = new CompletableFuture<>();

        hz1.getPartitionService().addMigrationListener(new MigrationListener() {
            @Override
            public void migrationStarted(final MigrationState migrationState) {
                // Don't trust the listener start time because no guarantees as to when its executed
            }

            @Override
            public void migrationFinished(final MigrationState migrationState) {
                migrationTimer.stop();
                migrationDurationReference.complete(Duration.ofMillis(migrationState.getTotalElapsedTime()));
            }

            @Override
            public void replicaMigrationCompleted(final ReplicaMigrationEvent replicaMigrationEvent) {
            }

            @Override
            public void replicaMigrationFailed(final ReplicaMigrationEvent replicaMigrationEvent) {
            }
        });

        LOGGER.fine("Starting second instance...");
        final HazelcastInstance hz2 = factory.newHazelcastInstance(config);

        // Back to ACTIVE
        migrationTimer.start();
        changeClusterStateEventually(hz2, ClusterState.ACTIVE);

        LOGGER.fine("Awaiting migration completion...");
        final Duration reportedMigrationDuration = migrationDurationReference.get(ASSERT_TRUE_EVENTUALLY_TIMEOUT,
                TimeUnit.SECONDS);

        assertFalse(reportedMigrationDuration.isZero());

        final String message = MessageFormat.format("migrationState.getTotalElapsedTime={1}, migrationTimer={0}",
                formatDuration(migrationTimer.elapsed()), formatDuration(reportedMigrationDuration));

        LOGGER.fine(message);
        assertTrue(MessageFormat.format("Reported migrationState.getTotalElapsedTime() was greater than the migration"
                        + " execution time recorded - {0}", message),
                migrationTimer.elapsed().compareTo(reportedMigrationDuration) >= 0);
    }

    /**
     * @see <a href="https://github.com/hazelcast/hazelcast/pull/25028#discussion_r1266664004">Discussion</a>
     */
    @Test
    public void testMigrationListenerTotalElapsedTime() {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        LOGGER.fine("Setting starting up instances...");
        final HazelcastInstance hz1 = factory.newHazelcastInstance();
        warmUpPartitions(hz1);
        HazelcastInstance hz2 = factory.newHazelcastInstance();
        waitAllForSafeState(hz1, hz2);

        final MetricsRegistry metricsRegistry = getNode(hz1).nodeEngine.getMetricsRegistry();

        long totalElapsedMigrationTime = getMetric(metricsRegistry,
                MetricDescriptorConstants.MIGRATION_METRIC_TOTAL_ELAPSED_MIGRATION_TIME);
        long elapsedMigrationTime = getMetric(metricsRegistry,
                MetricDescriptorConstants.MIGRATION_METRIC_ELAPSED_MIGRATION_TIME);

        assertNotEquals(MessageFormat.format("{0} should not be instantaneous",
                        MetricDescriptorConstants.MIGRATION_METRIC_TOTAL_ELAPSED_MIGRATION_TIME), 0,
                totalElapsedMigrationTime);
        assertNotEquals(MessageFormat.format("{0} should not be instantaneous",
                        MetricDescriptorConstants.MIGRATION_METRIC_ELAPSED_MIGRATION_TIME), 0,
                elapsedMigrationTime);

        assertTrue(MessageFormat.format("With only one migration, {0} ({2}) should be greater than or equal to {1} ({3})",
                MetricDescriptorConstants.MIGRATION_METRIC_TOTAL_ELAPSED_MIGRATION_TIME,
                MetricDescriptorConstants.MIGRATION_METRIC_ELAPSED_MIGRATION_TIME, totalElapsedMigrationTime,
                elapsedMigrationTime), totalElapsedMigrationTime >= elapsedMigrationTime);

        LOGGER.fine("Triggering another migration...");
        hz2.shutdown();
        hz2 = factory.newHazelcastInstance();
        waitAllForSafeState(hz1, hz2);

        totalElapsedMigrationTime = getMetric(metricsRegistry,
                MetricDescriptorConstants.MIGRATION_METRIC_TOTAL_ELAPSED_MIGRATION_TIME);
        elapsedMigrationTime = getMetric(metricsRegistry,
                MetricDescriptorConstants.MIGRATION_METRIC_ELAPSED_MIGRATION_TIME);

        assertTrue(MessageFormat.format("After multiple migrations, {0} ({2}) should be greater than {1} ({3})",
                MetricDescriptorConstants.MIGRATION_METRIC_TOTAL_ELAPSED_MIGRATION_TIME,
                MetricDescriptorConstants.MIGRATION_METRIC_ELAPSED_MIGRATION_TIME, totalElapsedMigrationTime,
                elapsedMigrationTime), totalElapsedMigrationTime > elapsedMigrationTime);
    }

    private long getMetric(final MetricsRegistry metricsRegistry, final String metric) {
        return metricsRegistry.newLongGauge(MetricDescriptorConstants.PARTITIONS_PREFIX + '.' + metric).read();
    }

    /**
     * @see <a href="https://github.com/hazelcast/hazelcast/pull/25028#discussion_r1266692838">Discussion</a>
     */
    private static String formatDuration(final Duration duration) {
        return MessageFormat.format("{0} {1}", duration.toMillis(), TimeUnit.MILLISECONDS.name().toLowerCase());
    }

    private HazelcastInstance createPausedMigrationCluster(final TestHazelcastInstanceFactory factory,
                                                           @Nullable final Config config) {
        LOGGER.fine("Starting paused migration instance...");
        final HazelcastInstance hazelcastInstance = factory.newHazelcastInstance(config);
        warmUpPartitions(hazelcastInstance);

        // Change to NO_MIGRATION to prevent repartitioning
        // before 2nd member started and ready.
        hazelcastInstance.getCluster().changeClusterState(ClusterState.NO_MIGRATION);

        return hazelcastInstance;
    }

    @SuppressWarnings("SameParameterValue")
    private void assertAllLessThanOrEqual(AtomicInteger[] integers, int expected) {
        for (AtomicInteger integer : integers) {
            assertThat(integer.get()).isLessThanOrEqualTo(expected);
        }
    }

    private static class CountingMigrationListener implements MigrationListener {

        final int numberOfPartitions;
        final AtomicInteger migrationStarted;
        final AtomicInteger migrationCompleted;
        final AtomicInteger[] replicaMigrationCompleted;
        final AtomicInteger[] replicaMigrationFailed;

        CountingMigrationListener(int partitionCount) {
            numberOfPartitions = partitionCount;
            migrationStarted = new AtomicInteger();
            migrationCompleted = new AtomicInteger();
            replicaMigrationCompleted = new AtomicInteger[numberOfPartitions];
            replicaMigrationFailed = new AtomicInteger[numberOfPartitions];
            for (int i = 0; i < numberOfPartitions; i++) {
                replicaMigrationCompleted[i] = new AtomicInteger();
                replicaMigrationFailed[i] = new AtomicInteger();
            }
        }

        /**
         * Migration can be re-started due to some issues,
         * this reset is used to track whether we have expected
         * number of migration started and completed events.
         * Per each start we expect one matching completion.
         */
        public void reset() {
            migrationStarted.set(0);
            migrationCompleted.set(0);
            for (int i = 0; i < numberOfPartitions; i++) {
                replicaMigrationCompleted[i].set(0);
                replicaMigrationFailed[i].set(0);
            }
        }

        @Override
        public void migrationStarted(MigrationState state) {
            // reset counter in every start
            reset();
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
    public static class EventCollectingMigrationListener implements MigrationListener {
        final List<MigrationEventsPack> allEventPacks = Collections.synchronizedList(new ArrayList<>());
        final ILogger logger = Logger.getLogger(PartitionMigrationListenerTest.class);
        volatile MigrationEventsPack currentEvents;

        final boolean shouldRecordIncompleteEvents;

        public EventCollectingMigrationListener(boolean shouldRecordIncompleteEvents) {
            this.shouldRecordIncompleteEvents = shouldRecordIncompleteEvents;
        }

        @Override
        public void migrationStarted(MigrationState state) {
            assertNull(currentEvents);
            currentEvents = new MigrationEventsPack();
            currentEvents.migrationProcessStarted = state;
            logger.info("Migration started: " + state);
        }

        @Override
        public void migrationFinished(MigrationState state) {
            assertNotNull(currentEvents);
            currentEvents.migrationProcessCompleted = state;
            // As per contract of MigrationListener#migrationFinished:
            //      "Not all of the planned migrations have to be completed.
            //      Some of them can be skipped because of a newly created migration plan."
            // Due to this, we should only record fully completed migrations, otherwise
            //   this test will inconsistently fail when a new migration plan is created
            boolean migrationCompleted = state.getPlannedMigrations() == state.getCompletedMigrations();
            if (shouldRecordIncompleteEvents || migrationCompleted) {
                allEventPacks.add(currentEvents);
            }
            currentEvents = null;
            logger.info(migrationCompleted ? "Migration finished: " + state
                    : "Migration finished but NOT completed, not adding to event tracker: " + state);
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
            logger.info("Replica Migration failed (1 of " + currentEvents.migrationsCompleted.size() + "): " + event);
        }

        List<MigrationEventsPack> ensureAndGetEventPacks(int count) {
            awaitEventPacksComplete(count);
            return allEventPacks.subList(0, count);
        }

        public MigrationEventsPack ensureAndGetSingleEventPack() {
            return ensureAndGetEventPacks(1).get(0);
        }

        void awaitEventPacksComplete(int count) {
            assertTrueEventually(() -> {
                assertThat(allEventPacks.size()).isGreaterThanOrEqualTo(count);
                assertNull(currentEvents);
            });
        }
    }

    public static class MigrationEventsPack {
        public volatile MigrationState migrationProcessStarted;
        public volatile MigrationState migrationProcessCompleted;
        public final List<ReplicaMigrationEvent> migrationsCompleted = Collections.synchronizedList(new ArrayList<>());
    }
}
