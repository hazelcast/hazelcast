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

package com.hazelcast.map.impl.mapstore;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MapStoreConfig.InitialLoadMode;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.config.MapStoreConfig.InitialLoadMode.EAGER;
import static com.hazelcast.config.MapStoreConfig.InitialLoadMode.LAZY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assume.assumeTrue;

/**
 * Tests that the operations that should be blocked by loading process are blocked and unblocked after loading is finished.
 */
@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class})
@SuppressWarnings("ResultOfMethodCallIgnored")
public class MapStoreBlockedByLoadTest extends HazelcastTestSupport {

    private static final long BLOCK_OPERATION_TIMEOUT_MS = 3000;
    private static final int MAP_SIZE = 22;
    private static final String MAP_NAME = "testMap";

    private HazelcastInstance[] instances;
    private IMap<String, String> map;
    private ControlableMapLoader<String, String> loader;

    @Parameterized.Parameters(name = "mode:{0}")
    public static Object[] mode() {
        return new Object[]{LAZY, EAGER};
    }

    @Parameterized.Parameter
    public InitialLoadMode mode;

    @Before
    public void setup() {
        Config config = smallInstanceConfig();
        var partitionCount = Integer.parseInt(config.getProperty(ClusterProperty.PARTITION_COUNT.getName()));
        loader = new ControlableMapLoader<>(new SimpleStringMapLoader(MAP_SIZE), partitionCount);
        MapStoreConfig mapStoreConfig = new MapStoreConfig()
                .setEnabled(true)
                .setInitialLoadMode(mode)
                .setImplementation(loader);
        config.getMapConfig(MAP_NAME)
                .setMapStoreConfig(mapStoreConfig);
        instances = createHazelcastInstances(config, 1);
        map = instances[0].getMap(MAP_NAME);
    }

    /* == check that methods blocked by full load == */
    @Test
    public void putBlockedByLoadingProcess() throws Exception {
        assertOperationIsBlockedByLoadAll(() -> map.put("1", "value1"));
    }

    @Test
    public void clearBlockedByLoadingProcess() throws Exception {
        assertOperationIsBlockedByLoadAll(() -> map.clear());
    }

    @Test
    public void removeBlockedByLoadingProcess() throws Exception {
        assertOperationIsBlockedByLoadAll(() -> map.remove("1"));
    }

    @Test
    public void deleteBlockedByLoadingProcess() throws Exception {
        assertOperationIsBlockedByLoadAll(() -> map.delete("1"));
    }

    @Test
    public void putIfAbsentBlockedByLoadingProcess() throws Exception {
        assertOperationIsBlockedByLoadAll(() -> map.putIfAbsent("1", "value1"));
    }

    @Test
    public void replaceBlockedByLoadingProcess() throws Exception {
        assertOperationIsBlockedByLoadAll(() -> map.replace("1", "value1"));
    }

    @Test
    public void replaceWithOldValBlockedByLoadingProcess() throws Exception {
        assertOperationIsBlockedByLoadAll(() -> map.replace("1", "oldVal", "newVal"));
    }

    @Test
    public void containsValueBlockedByLoadingProcess() throws Exception {
        assertOperationIsBlockedByLoadAll(() -> map.containsValue("value1"));
    }

    @Test
    public void keySetBlockedByLoadingProcess() throws Exception {
        assertOperationIsBlockedByLoadAll(() -> map.keySet());
    }

    @Test
    public void valuesBlockedByLoadingProcess() throws Exception {
        assertOperationIsBlockedByLoadAll(() -> map.values());
    }

    @Test
    public void entrySetBlockedByLoadingProcess() throws Exception {
        assertOperationIsBlockedByLoadAll(() -> map.entrySet());
    }

    @Test
    public void sizeBlockedByLoadingProcess() throws Exception {
        assertOperationIsBlockedByLoadAll(() -> map.size());
    }

    @Test
    public void isEmptyBlockedByLoadingProcess() throws Exception {
        assertOperationIsBlockedByLoadAll(() -> map.isEmpty());
    }

    @Test
    public void evictAllBlockedByLoadingProcess() throws Exception {
        assertOperationIsBlockedByLoadAll(() -> map.evictAll());
    }

    @Test
    public void putAllBlockedByLoadingProcess() throws Exception {
        assertOperationIsBlockedByLoadAll(() -> map.putAll(Map.of("1", "value1", "2", "value2")));
    }

    @Test
    public void flushIsNotBlockedByLoadingProcess() throws Exception {
        assertOperationIsNotBlockedByLoadAll(() -> map.flush());
    }

    @Test
    @Ignore("containsKey is not blocked on master")
    public void containIstBlockedByLoadingProcess() throws Exception {
        assertOperationIsBlockedByLoadAll(() -> map.containsKey("1"));
    }

    /* == check that methods blocked by load in the corresponded partition == */
    @Test
    public void getBlockedByLoadValuesFromTheSamePartitionLoadingProcess() throws Exception {
        var keyToLoad = generateKeyForPartition(instances[0], 0);
        var key = generateKeyForPartition(instances[0], 0);
        assertThat(map.get(key)).isEqualTo("value" + key);

        assertOperationIsBlockedOnPartitionAndUnblocked(() -> assertThat(map.get(key)).isEqualTo("value" + key), keyToLoad);
        loader.assertLoadedEntriesCount(1);
    }

    @Test
    public void getAllBlockedByPartitionLoad() throws Exception {
        var keyToLoad = generateKeyForPartition(instances[0], 0);
        var key = generateKeyForPartition(instances[0], 0);
        Set<String> keys = Set.of(key);
        assertOperationIsBlockedOnPartitionAndUnblocked(() -> map.getAll(keys), keyToLoad);
    }

    @Test
    public void putBlockedByPartitionLoad() throws Exception {
        // In lazy mode, this test triggers the full initial load, causing the put operation to block.
        // Initial-load blocking behavior in lazy mode is already covered by putBlockedByLoadingProcess test,
        // so running this test in lazy mode is redundant.
        assumeTrue(mode == EAGER);
        var keyToLoad = generateKeyForPartition(instances[0], 0);
        var key = generateKeyForPartition(instances[0], 0);
        assertOperationIsBlockedOnPartitionAndUnblocked(() -> map.put(key, "aaa"), keyToLoad);
        loader.assertLoadedEntriesCount(1);
    }

    @Test
    public void putAllBlockedByPartitionLoad() throws Exception {
        // In lazy mode, this test triggers the full initial load, causing the putAll operation to block.
        // Initial-load blocking behavior in lazy mode is already covered by putAllBlockedByLoadingProcess test,
        // so running this test in lazy mode is redundant.
        assumeTrue(mode == EAGER);
        var keyToLoad = generateKeyForPartition(instances[0], 0);
        var key = generateKeyForPartition(instances[0], 0);
        Map<String, String> values = Map.of(key, "value");
        assertOperationIsBlockedOnPartitionAndUnblocked(() -> map.putAll(values), keyToLoad);
        loader.assertLoadedEntriesCount(1);
    }

    @Test
    @Ignore("containsKey is not blocked on master")
    public void containsKeyBlockedByPartitionLoad() throws Exception {
        var keyToLoad = generateKeyForPartition(instances[0], 0);
        var key = generateKeyForPartition(instances[0], 0);

        assertOperationIsBlockedOnPartitionAndUnblocked(() -> map.containsKey(key), keyToLoad);
    }

    @Test
    @Ignore("evict is not blocked on master")
    public void evictBlockedByPartitionLoad() throws Exception {
        var keyToLoad = generateKeyForPartition(instances[0], 0);
        var key = generateKeyForPartition(instances[0], 0);

        assertOperationIsBlockedOnPartitionAndUnblocked(() -> map.evict(key), keyToLoad);
    }

    @Test
    public void putNotBlockedByLoadValuesFromDifferentPartitionLoadingProcess() throws InterruptedException {
        // In lazy mode, this test triggers the full initial load, causing the put operation to block.
        // Initial-load blocking behavior in lazy mode is already covered by putBlockedByLoadingProcess test,
        // so running this test in lazy mode is redundant.
        assumeTrue(mode == EAGER);
        loader.reset(1);
        loader.pauseValueLoading();
        var coordinatorPartitionId = instances[0].getPartitionService().getPartition(MAP_NAME).getPartitionId();
        int partitionsCount = instances[0].getPartitionService().getPartitions().size();
        var keyToLoad = generateKeyForPartition(instances[0], (coordinatorPartitionId + 1) % partitionsCount);
        var key = generateKeyForPartition(instances[0], (coordinatorPartitionId + 2) % partitionsCount);
        CompletableFuture.runAsync(() -> map.loadAll(Set.of(keyToLoad), true));
        loader.awaitValueLoadPaused();
        map.put(key, "value1");
    }

    @Test
    public void getNotBlockedByLoadValuesFromDifferentPartitionLoadingProcess() throws InterruptedException {
        // In lazy mode, this test triggers the full initial load, causing the get operation to block.
        // Initial-load blocking behavior in lazy mode is already covered by getBlockedByLoadingProcess test,
        // so running this test in lazy mode is redundant.
        assumeTrue(mode == EAGER);
        loader.reset(1);
        loader.pauseValueLoading();
        var coordinatorPartitionId = instances[0].getPartitionService().getPartition(MAP_NAME).getPartitionId();
        int partitionsCount = instances[0].getPartitionService().getPartitions().size();
        var keyToLoad = generateKeyForPartition(instances[0], (coordinatorPartitionId + 1) % partitionsCount);
        var key = generateKeyForPartition(instances[0], (coordinatorPartitionId + 2) % partitionsCount);
        CompletableFuture.runAsync(() -> map.loadAll(Set.of(keyToLoad), true));
        loader.awaitValueLoadPaused();
        map.get(key);
    }

    @Test
    public void containsNotBlockedByLoadValuesFromDifferentPartitionLoadingProcess() throws Exception {
        loader.reset(1);
        loader.pauseValueLoading();
        var coordinatorPartitionId = instances[0].getPartitionService().getPartition(MAP_NAME).getPartitionId();
        int partitionsCount = instances[0].getPartitionService().getPartitions().size();
        var keyToLoad = generateKeyForPartition(instances[0], (coordinatorPartitionId + 1) % partitionsCount);
        var key = generateKeyForPartition(instances[0], (coordinatorPartitionId + 2) % partitionsCount);
        CompletableFuture.runAsync(() -> map.loadAll(Set.of(keyToLoad), true));
        loader.awaitValueLoadPaused();
        map.containsKey(key);
    }

    @Test
    public void evictNotBlockedByLoadValuesFromDifferentPartitionLoadingProcess() throws Exception {
        loader.reset(1);
        loader.pauseValueLoading();
        var coordinatorPartitionId = instances[0].getPartitionService().getPartition(MAP_NAME).getPartitionId();
        int partitionsCount = instances[0].getPartitionService().getPartitions().size();
        var keyToLoad = generateKeyForPartition(instances[0], (coordinatorPartitionId + 1) % partitionsCount);
        var key = generateKeyForPartition(instances[0], (coordinatorPartitionId + 2) % partitionsCount);
        CompletableFuture.runAsync(() -> map.loadAll(Set.of(keyToLoad), true));
        loader.awaitValueLoadPaused();
        map.evict(key);
    }

    private void assertOperationIsBlockedOnPartitionAndUnblocked(Runnable operation, String keyToLoad) throws InterruptedException {
        loader.reset(1);
        loader.pauseValueLoading();

        CompletableFuture.runAsync(() -> map.loadAll(Set.of(keyToLoad), false));
        loader.awaitValueLoadPaused();

        CompletableFuture<Void> operationFuture = CompletableFuture.runAsync(operation);

        assertThatThrownBy(() -> operationFuture.get(BLOCK_OPERATION_TIMEOUT_MS, TimeUnit.MILLISECONDS))
                .as("Expected timeout if operation exceeds " + BLOCK_OPERATION_TIMEOUT_MS + " milliseconds")
                .isInstanceOf(java.util.concurrent.TimeoutException.class);

        loader.resumeValueLoading();
        assertThatNoException().isThrownBy(() -> operationFuture.get(BLOCK_OPERATION_TIMEOUT_MS, TimeUnit.MILLISECONDS));
    }

    private void assertOperationIsBlockedByLoadAll(Runnable operation) throws InterruptedException {
        // lock loading
        loader.pauseValueLoading();

        CompletableFuture.runAsync(() -> map.loadAll(true));
        loader.awaitValueLoadPaused();
        CompletableFuture<Void> future = CompletableFuture.runAsync(operation);

        assertThatThrownBy(() -> future.get(BLOCK_OPERATION_TIMEOUT_MS, TimeUnit.MILLISECONDS))
                .withFailMessage("Expected timeout if operation exceeds " + BLOCK_OPERATION_TIMEOUT_MS + " milliseconds")
                .isInstanceOf(java.util.concurrent.TimeoutException.class);

        // unblock loading
        loader.resumeValueLoading();
        assertThatNoException().isThrownBy(() -> future.get(BLOCK_OPERATION_TIMEOUT_MS, TimeUnit.MILLISECONDS));
    }

    private void assertOperationIsNotBlockedByLoadAll(Runnable operation) throws ExecutionException, InterruptedException, TimeoutException {
        // lock loading
        loader.pauseValueLoading();

        CompletableFuture.runAsync(() -> map.loadAll(true));
        loader.awaitValueLoadPaused();

        CompletableFuture<Void> future = CompletableFuture.runAsync(operation);

        future.get(BLOCK_OPERATION_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }
}
