/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.impl.migration;

import com.hazelcast.config.Config;
import com.hazelcast.config.vector.Metric;
import com.hazelcast.config.vector.VectorCollectionConfig;
import com.hazelcast.config.vector.VectorIndexConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.partition.MigrationListener;
import com.hazelcast.partition.MigrationState;
import com.hazelcast.partition.ReplicaMigrationEvent;
import com.hazelcast.spi.exception.PartitionMigratingException;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.Accessors;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.vector.impl.service.VectorCollectionServiceImpl;
import com.hazelcast.vector.impl.storage.OnHeapVectorCollectionObjectProvider;
import com.hazelcast.vector.impl.storage.VectorCollectionStorage;
import com.hazelcast.vector.impl.storage.VectorIndexFactory;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.vector.impl.VectorTestUtils.warmupTwoIndexCollection;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

@Category({QuickTest.class, ParallelJVMTest.class})
public class VectorCollectionMigrationAndOptimizationTest extends VectorCollectionMigrationTestBase {

    @Override
    protected Config getConfig() {
        var config = super.getConfig()
                // fail faster when operation is not allowed during migration
                // normally max retry count is aligned with 120s invocation timeout
                .setProperty(ClusterProperty.INVOCATION_MAX_RETRY_COUNT.getName(), "5")
                // need many concurrent optimizes to block index on all partitions
                .setProperty(VectorCollectionServiceImpl.MAX_CONCURRENT_OPTIMIZE.getName(), "1000");

        // Configure OFFLOADABLE_EXECUTOR with partitionCount + 1 threads to avoid deadlock during test execution.
        // In setups with a few vCPUs the pool may be too small. With 11 partitions 4 threads will be blocked by
        // simulated slow optimization and 4 happens to be the default size of the offloadable thread pool for 4 vCPUs.
        // We need more threads in optimizeIsNotAllowed_whenPartitionsAreMigrating for second optimize invocation.
        partitionCount = Integer.parseInt(config.getProperty(ClusterProperty.PARTITION_COUNT.getName()));
        config.getExecutorConfig(ExecutionService.OFFLOADABLE_EXECUTOR).setPoolSize(partitionCount + 1);

        return config;
    }

    @Test
    public void migrationWaitsForOptimizationFinish_whenMemberGracefulShutdownDuringOptimization() {
        assertSize(members[0]);

        HazelcastInstance shuttingDownMember = members[2];
        int partitionId = getPartitionId(shuttingDownMember);

        // start simulated slow optimization - index lock is sufficient to block migration
        VectorCollectionServiceImpl service = Accessors.getService(shuttingDownMember, VectorCollectionServiceImpl.SERVICE_NAME);
        var storage = service.getStorageOrNull(collectionName, partitionId);
        assertThat(storage).as("partition should exists").isNotNull();
        storage.lockIndexMutation(indexName, UUID.randomUUID());

        CountDownLatch migrationStartedLatch = getMigrationStartedLatch();

        // initiate shutdown
        var shutdownFuture = CompletableFuture.runAsync(shuttingDownMember::shutdown);

        // wait for migration to start and give a bit more time to check if it does not finish prematurely
        assertOpenEventually(migrationStartedLatch);
        sleepSeconds(3);

        // shutdown should be still blocked by migration waiting for optimization
        assertThat(shutdownFuture).isNotDone();

        // finish simulated slow optimization
        storage.optimize(indexName);
        storage.unlockIndexMutation(indexName);

        // shutdown should be unblocked now
        assertThat(shutdownFuture).succeedsWithin(ASSERT_TRUE_EVENTUALLY_TIMEOUT_DURATION);

        // no data should be lost
        waitClusterForSafeState(members[0]);
        assertSize(members[0]);
    }

    @Test
    public void optimizeIsNotAllowed_whenPartitionsAreMigrating() throws InterruptedException {
        // scenario:
        // 1. create collection with 2 indexes and data
        // 2. start simulated slow optimization on first index
        // 3. new member joins the cluster triggering repartitioning
        // 4. migrations are not finished until slow optimization finishes
        // 5. optimization on second index fails on partitions that are being migrated

        assumeThat(namedIndex).as("Multi-index test requires named indexes").isTrue();

        collectionName = randomName();
        VectorCollectionConfig vectorCollectionConfig = new VectorCollectionConfig(collectionName)
                // do not use backups to simplify countdownlatches
                .setBackupCount(0)
                .addVectorIndexConfig(new VectorIndexConfig(indexName, Metric.EUCLIDEAN, 2)
                        .setUseDeduplication(useDeduplication))
                .addVectorIndexConfig(new VectorIndexConfig("nonblocking_index", Metric.EUCLIDEAN, 2)
                        .setUseDeduplication(useDeduplication));
        members[0].getConfig().addVectorCollectionConfig(vectorCollectionConfig);
        collection = members[0].getVectorCollection(vectorCollectionConfig.getName());

        CountDownLatch optimizeStartedLatch = new CountDownLatch(partitionCount);
        CountDownLatch optimizeAwait = new CountDownLatch(1);
        // note: replaceStorage replaces previous storage (loses data), so first warmup creates
        // all needed storages and in second warmup adds the data again so they are visible for test
        // note2: storages created later due to migrations will _not_ be blocking
        warmupTwoIndexCollection(members[0], collection);
        replaceStorage(optimizeStartedLatch, optimizeAwait);
        warmupTwoIndexCollection(members[0], collection);

        assertSize(members[0]);

        // ongoing optimizations will block migrations
        CompletionStage<Void> optimizeFuture = collection.optimizeAsync(indexName);
        assertThat(optimizeStartedLatch.await(ASSERT_TRUE_EVENTUALLY_TIMEOUT, TimeUnit.SECONDS)).isTrue();

        try {
            // initiate repartitioning
            factory.newHazelcastInstance(getConfig());

            // Optimization should ultimately throw PartitionMigratingException even if migration starts slowly
            assertTrueEventually(() -> assertThat(collection.optimizeAsync("nonblocking_index"))
                    .as("Optimize should not proceed during migration")
                    .failsWithin(ASSERT_TRUE_EVENTUALLY_TIMEOUT_DURATION)
                    .withThrowableThat().withRootCauseInstanceOf(PartitionMigratingException.class));
        } finally {
            // finish simulated slow optimization also in case of assertion error to allow normal shutdown
            optimizeAwait.countDown();
        }

        assertThat(optimizeFuture).succeedsWithin(ASSERT_TRUE_EVENTUALLY_TIMEOUT_DURATION);

        // migration should be unblocked now
        waitClusterForSafeState(members[0]);

        // no data should be lost
        assertSize(members[0]);
    }


    private void replaceStorage(CountDownLatch optimizeStarted, CountDownLatch optimizeAwait) {
        for (var member : members) {
            VectorCollectionServiceImpl service = Accessors.getService(member, VectorCollectionServiceImpl.SERVICE_NAME);
            for (int partitionId = 0; partitionId < partitionCount; ++partitionId) {
                var storage = service.getStorageOrNull(collectionName, partitionId);
                if (storage != null) {
                    member.getLoggingService().getLogger(this.getClass()).info("forEachStorage found partitionId=" + partitionId);
                    OptimizeBlockingStorage slowStorage = new OptimizeBlockingStorage(storage, Accessors.getNodeEngineImpl(member),
                            optimizeStarted, optimizeAwait);
                    service.attachStorage(slowStorage);
                }
            }
        }
    }

    /**
     * @return latch to wait for actual migration start which happens after the cluster forms but before it is safe to avoid flakiness
     * @apiNote must be invoked before topology change
     */
    private CountDownLatch getMigrationStartedLatch() {
        CountDownLatch migrationStarted = new CountDownLatch(1);
        members[0].getPartitionService().addMigrationListener(new MigrationListener() {
            @Override
            public void migrationStarted(MigrationState state) {
                migrationStarted.countDown();
            }

            @Override
            public void migrationFinished(MigrationState state) {
            }

            @Override
            public void replicaMigrationCompleted(ReplicaMigrationEvent event) {
            }

            @Override
            public void replicaMigrationFailed(ReplicaMigrationEvent event) {
            }
        });
        return migrationStarted;
    }

    public static class OptimizeBlockingStorage extends VectorCollectionStorage {
        private final CountDownLatch optimizeStarted;
        private final CountDownLatch optimizeAwait;

        public OptimizeBlockingStorage(VectorCollectionStorage original, NodeEngine nodeEngine,
                                       CountDownLatch optimizeStarted, CountDownLatch optimizeAwait) {
            super(nodeEngine, original.getName(), original.getPartitionId(), original.getConfig(),
                    OnHeapVectorCollectionObjectProvider.getInstance(), new VectorIndexFactory(nodeEngine));
            this.optimizeStarted = optimizeStarted;
            this.optimizeAwait = optimizeAwait;
        }

        @Override
        public void optimize(String indexName) {
            try {
                if (!indexName.equals("nonblocking_index")) {
                    optimizeStarted.countDown();
                    logger.info("optimize waiting for partitionId=" + getPartitionId() + " for indexName=" + indexName);
                    optimizeAwait.await();
                    logger.info("optimize done waiting for partitionId=" + getPartitionId() + " for indexName=" + indexName);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            super.optimize(indexName);
        }
    }
}
