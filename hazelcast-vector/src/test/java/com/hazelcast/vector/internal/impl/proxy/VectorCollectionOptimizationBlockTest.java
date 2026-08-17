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

package com.hazelcast.vector.internal.impl.proxy;

import com.hazelcast.config.vector.Metric;
import com.hazelcast.config.vector.VectorCollectionConfig;
import com.hazelcast.config.vector.VectorIndexConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.starter.ReflectionUtils;
import com.hazelcast.vector.VectorCollection;
import com.hazelcast.vector.VectorDocument;
import com.hazelcast.vector.internal.impl.VectorCollectionService;
import com.hazelcast.vector.internal.impl.storage.OnHeapVectorCollectionObjectProvider;
import com.hazelcast.vector.internal.impl.storage.VectorCollectionStorage;
import com.hazelcast.vector.internal.impl.storage.VectorIndexFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static com.hazelcast.vector.internal.impl.VectorTestUtils.vec;
import static com.hazelcast.vector.internal.impl.VectorTestUtils.warmupOneIndexCollection;
import static com.hazelcast.vector.internal.impl.service.VectorCollectionServiceImpl.MAX_CONCURRENT_OPTIMIZE;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class VectorCollectionOptimizationBlockTest extends HazelcastTestSupport {

    private static final Duration LATCH_AWAIT_TIMEOUT = Duration.of(5, ChronoUnit.SECONDS);

    private HazelcastInstance instance;
    private int partitionCount;
    private VectorCollection<String, String> vectorCollection;

    private CountDownLatch optimizeStarted;
    private CountDownLatch optimizeAwait;
    private final AtomicBoolean optimizationFails = new AtomicBoolean();
    private final AtomicReference<UUID> optimizationUUID = new AtomicReference<>();

    private static final VectorDocument<String> testDocument = VectorDocument.of("test", vec(0.5f));

    private static final Duration TIMEOUT = ASSERT_TRUE_EVENTUALLY_TIMEOUT_DURATION;

    @Before
    public void setup() {
        // Configure OFFLOADABLE_EXECUTOR with partitionCount + 1 threads to avoid deadlock during test execution.
        var config = smallInstanceConfig();
        partitionCount = Integer.parseInt(config.getProperty(ClusterProperty.PARTITION_COUNT.getName()));
        config.getExecutorConfig(ExecutionService.OFFLOADABLE_EXECUTOR).setPoolSize(partitionCount + 1);
        // need more permits to block index on all partitions
        config.setProperty(MAX_CONCURRENT_OPTIMIZE.getName(), String.valueOf(partitionCount));

        instance = createHazelcastInstance(config);

        String collectionName = randomName();

        vectorCollection = createVectorCollectionOneIndex(collectionName);
        var nodeEngine = getNodeEngineImpl(instance);

        optimizeStarted = new CountDownLatch(partitionCount);
        optimizeAwait = new CountDownLatch(1);
        optimizationFails.set(false);

        replaceVectorStorageWithMocked(collectionName, nodeEngine);
    }

    private void replaceVectorStorageWithMocked(
            String collectionName,
            NodeEngine nodeEngine
    ) {
        var partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        Function<Integer, VectorCollectionStorage> storageFactory = (partition) -> new BlockingTestVectorCollectionStorage(
                nodeEngine,
                collectionName,
                partition,
                instance.getConfig().getVectorCollectionConfigOrNull(collectionName),
                optimizeStarted,
                optimizeAwait,
                optimizationFails,
                optimizationUUID
        );
        ConcurrentHashMap<Integer, VectorCollectionStorage> storageMap = new ConcurrentHashMap<>();
        range(0, partitionCount).forEach(partition -> storageMap.put(partition, storageFactory.apply(partition)));
        ConcurrentHashMap<String, ConcurrentHashMap<Integer, VectorCollectionStorage>> map = new ConcurrentHashMap<>();
        map.put(collectionName, storageMap);
        VectorCollectionService service = getNodeEngineImpl(instance).getService(VectorCollectionService.SERVICE_NAME);
        try {
            ReflectionUtils.setFieldValueReflectively(service, "storage", map);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void optimizationNotAllowSecondOptimization() {
        performTest(() -> vectorCollection.optimizeAsync(), partitionCount);
    }

    @Test
    public void optimizationBlockPutOperation() {
        performTest(() -> vectorCollection.putAsync("1", testDocument));
    }

    @Test
    public void optimizationBlockPutAllOperation() {
        performTest(() -> vectorCollection.putAllAsync(Map.of("1", testDocument)));
    }

    @Test
    public void optimizationBlockPutIfAbsentOperation() {
        performTest(() -> vectorCollection.putIfAbsentAsync("1", testDocument));
    }

    @Test
    public void optimizationBlockSetOperation() {
        performTest(() -> vectorCollection.setAsync("1", testDocument));
    }

    @Test
    public void optimizationBlockDeleteOperation() {
        performTest(() -> vectorCollection.deleteAsync("1"));
    }

    @Test
    public void optimizationBlockRemoveOperation() {
        performTest(() -> vectorCollection.removeAsync("1"));
    }

    @Test
    public void optimizationBlockClearOperation() {
        performTest(() -> vectorCollection.clearAsync(), partitionCount);
    }

    @Test
    public void optimizationFailsThenUnlockIndex() {
        optimizeAwait.countDown();
        optimizationFails.set(true);

        assertThat(vectorCollection.optimizeAsync())
                .failsWithin(TIMEOUT);

        optimizationFails.set(false);

        assertThat(vectorCollection.optimizeAsync())
                .succeedsWithin(TIMEOUT);
    }

    @Test
    public void allowsParallelOptimizationProcessOnDifferentIndexes() {
        var twoIndexCollectionName = randomName();
        var twoIndexVectorCollection = createVectorCollectionTwoIndex(twoIndexCollectionName);

        replaceVectorStorageWithMocked(twoIndexCollectionName, getNodeEngineImpl(instance));
        twoIndexVectorCollection.optimizeAsync("blocked_index");

        await(optimizeStarted);

        assertThat(twoIndexVectorCollection.optimizeAsync("not_blocked_index"))
                .succeedsWithin(TIMEOUT);

        optimizationFails.set(true);
        optimizeAwait.countDown();
        assertThat(twoIndexVectorCollection.optimizeAsync("blocked_index"))
                .failsWithin(TIMEOUT);
    }

    @Test
    public void whenNotEnoughPermitsOptimizationWaitsAndWakesAfterSuccess() {
        whenNotEnoughPermitsOptimizationWaitsAndWakesAfter(true);
    }

    @Test
    public void whenNotEnoughPermitsOptimizationWaitsAndWakesAfterFailure() {
        whenNotEnoughPermitsOptimizationWaitsAndWakesAfter(false);
    }

    private void whenNotEnoughPermitsOptimizationWaitsAndWakesAfter(boolean success) {
        var twoIndexCollectionName = randomName();
        var twoIndexVectorCollection = createVectorCollectionTwoIndex(twoIndexCollectionName);

        replaceVectorStorageWithMocked(twoIndexCollectionName, getNodeEngineImpl(instance));

        var blockedIndexFuture = twoIndexVectorCollection.optimizeAsync("blocked_index");
        await(optimizeStarted);

        var notBlockedIndexFuture = twoIndexVectorCollection.optimizeAsync("not_blocked_index");
        assertWaitingOperationCountEventually(partitionCount, instance);
        assertThat(notBlockedIndexFuture).isNotDone();

        optimizationFails.set(!success);
        optimizeAwait.countDown();

        if (success) {
            assertThat(blockedIndexFuture).succeedsWithin(TIMEOUT);
        } else {
            assertThat(blockedIndexFuture).failsWithin(TIMEOUT);
        }
        assertThat(notBlockedIndexFuture).succeedsWithin(TIMEOUT);
    }

    @Test
    public void whenCollectionDestroyedOptimizationFinishedSuccessfully() {
        whenCollectionDestroyedOptimizationFinishedSuccessfully(false);
    }

    @Test
    public void whenDifferentCollectionDestroyedOptimizationFinishedSuccessfully() {
        whenCollectionDestroyedOptimizationFinishedSuccessfully(true);
    }

    /**
     * This checks special rules of cancelling VectorOptimizeWaitNotifyKey which does not have object name.
     * It does not matter if the blocked optimization executes in such case or not, but it should not fail.
     */
    private void whenCollectionDestroyedOptimizationFinishedSuccessfully(boolean differentCollection) {
        var twoIndexCollectionName = randomName();
        var twoIndexVectorCollection = createVectorCollectionTwoIndex(twoIndexCollectionName);

        VectorCollection<String, String> otherCollection = null;
        if (differentCollection) {
            otherCollection = createVectorCollectionOneIndex(randomName());
            warmupOneIndexCollection(instance, otherCollection);
        }

        replaceVectorStorageWithMocked(twoIndexCollectionName, getNodeEngineImpl(instance));

        var blockedIndexFuture = twoIndexVectorCollection.optimizeAsync("blocked_index");
        await(optimizeStarted);

        var notBlockedIndexFuture = twoIndexVectorCollection.optimizeAsync("not_blocked_index");
        assertWaitingOperationCountEventually(partitionCount, instance);
        assertThat(notBlockedIndexFuture).isNotDone();

        // when
        if (differentCollection) {
            otherCollection.destroy();
        } else {
            twoIndexVectorCollection.destroy();
        }

        // then

        // check that optimization was not cancelled when parked
        // this is not strictly required for functionality, but it validates the current implementation
        assertWaitingOperationCountEventually(partitionCount, instance);

        // optimisation should not fail on destroyed collection
        optimizeAwait.countDown();
        assertThat(blockedIndexFuture).succeedsWithin(TIMEOUT);
        assertThat(notBlockedIndexFuture).succeedsWithin(TIMEOUT);
    }

    public static class BlockingTestVectorCollectionStorage extends VectorCollectionStorage {

        private final CountDownLatch optimizeStarted;
        private final CountDownLatch optimizeAwait;
        private final AtomicBoolean optimizationFails;
        private final AtomicReference<UUID> optimizationUUID;

        public BlockingTestVectorCollectionStorage(
                NodeEngine nodeEngine,
                String name,
                int partitionId,
                VectorCollectionConfig config,
                CountDownLatch optimizeStarted,
                CountDownLatch optimizeAwait,
                AtomicBoolean optimizationFails,
                AtomicReference<UUID> optimizationUUID) {
            super(nodeEngine, name, partitionId, config,
                OnHeapVectorCollectionObjectProvider.getInstance(), new VectorIndexFactory(nodeEngine));
            this.optimizeStarted = optimizeStarted;
            this.optimizeAwait = optimizeAwait;
            this.optimizationFails = optimizationFails;
            this.optimizationUUID = optimizationUUID;
        }

        @Override
        public void lockIndexMutation(String indexName, UUID uuid) {
            optimizationUUID.set(uuid);
            super.lockIndexMutation(indexName, uuid);
        }

        @Override
        public void unlockIndexMutation(String indexName) {
            super.unlockIndexMutation(indexName);
            optimizationUUID.set(null);
        }

        @Override
        public void optimize(String indexName) {
            if ("blocked_index".equals(indexName) || StringUtil.isNullOrEmpty(indexName)) {
                optimizeStarted.countDown();
                await(optimizeAwait);
                if (optimizationFails.get()) {
                    throw new RuntimeException("Optimization failed");
                }
            }
            super.optimize(indexName);
        }
    }

    private void performTest(Supplier<CompletionStage<?>> action) {
        performTest(action, 1);
    }

    private void performTest(Supplier<CompletionStage<?>> action, int expectedOpsCount) {
        var optimization = vectorCollection.optimizeAsync();
        await(optimizeStarted);

        CompletionStage<?> futureDuringOptimization = action.get();
        assertWaitingOperationCountEventually(expectedOpsCount, instance);
        assertThat(futureDuringOptimization).isNotDone();

        optimizeAwait.countDown();
        optimization.toCompletableFuture().join();

        assertThat(futureDuringOptimization).succeedsWithin(TIMEOUT);

        assertThat(action.get()).succeedsWithin(TIMEOUT);
    }

    private <T> VectorCollection<T, T> createVectorCollectionOneIndex(String collectionName) {
        VectorIndexConfig vectorIndexConfig1 = new VectorIndexConfig()
                .setName("index1")
                .setMetric(Metric.EUCLIDEAN)
                .setDimension(1);
        VectorCollectionConfig vectorCollectionConfig = new VectorCollectionConfig(collectionName)
                .addVectorIndexConfig(vectorIndexConfig1);
        instance.getConfig().addVectorCollectionConfig(vectorCollectionConfig);
        return instance.getVectorCollection(vectorCollectionConfig.getName());
    }

    private <T> VectorCollection<T, T> createVectorCollectionTwoIndex(String collectionName) {
        VectorIndexConfig vectorIndexConfig1 = new VectorIndexConfig()
                .setName("blocked_index")
                .setMetric(Metric.EUCLIDEAN)
                .setDimension(1);
        VectorIndexConfig vectorIndexConfig2 = new VectorIndexConfig()
                .setName("not_blocked_index")
                .setMetric(Metric.EUCLIDEAN)
                .setDimension(1);
        VectorCollectionConfig vectorCollectionConfig = new VectorCollectionConfig(collectionName)
                .addVectorIndexConfig(vectorIndexConfig1)
                .addVectorIndexConfig(vectorIndexConfig2);
        instance.getConfig().addVectorCollectionConfig(vectorCollectionConfig);
        return instance.getVectorCollection(vectorCollectionConfig.getName());
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(LATCH_AWAIT_TIMEOUT.getSeconds(), TimeUnit.SECONDS)) {
                throw new RuntimeException("Latch timeout awaits exceeded.");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
