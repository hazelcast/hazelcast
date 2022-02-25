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

package com.hazelcast.map.impl.mapstore.writebehind;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.OutOfMemoryHandler;
import com.hazelcast.instance.impl.OutOfMemoryErrorDispatcher;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapStoreAdapter;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WriteBehindFailAndRetryTest extends HazelcastTestSupport {

    @Test
    public void failed_store_operations_does_not_change_item_count_in_write_behind_queue_when_batching_enabled() {
        failed_stores_does_not_change_item_count_in_write_behind_queue_without_coalescing(true);
    }

    @Test
    public void failed_store_operations_does_not_change_item_count_in_write_behind_queue_when_batching_disabled() {
        failed_stores_does_not_change_item_count_in_write_behind_queue_without_coalescing(false);
    }

    private void failed_stores_does_not_change_item_count_in_write_behind_queue_without_coalescing(boolean batchingEnabled) {
        ExceptionThrowerMapStore mapStore = new ExceptionThrowerMapStore();
        final IMap<Integer, Integer> map = TestMapUsingMapStoreBuilder.<Integer, Integer>create()
                .withMapStore(mapStore)
                .withNodeCount(1)
                .withNodeFactory(createHazelcastInstanceFactory(1))
                .withWriteDelaySeconds(1)
                .withPartitionCount(1)
                .withWriteCoalescing(false)
                .withWriteBatchSize(batchingEnabled ? 1000 : 1)
                .build();

        int entryCountToStore = 1;
        for (int i = 0; i < entryCountToStore; i++) {
            map.put(i, i);
        }

        sleepAtLeastSeconds(5);

        assertEquals(entryCountToStore, totalItemCountInWriteBehindQueues(map));
        assertEquals(entryCountToStore, sizeOfWriteBehindQueueInPartition(0, map));
    }

    /**
     * Returns total item count in all write behind queues of a node.
     */
    private static long totalItemCountInWriteBehindQueues(IMap map) {
        MapService mapService = (MapService) ((MapProxyImpl) map).getService();
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        return mapServiceContext.getNodeWideUsedCapacityCounter().currentValue();

    }

    private static int sizeOfWriteBehindQueueInPartition(int partitionId, IMap map) {
        MapService mapService = (MapService) ((MapProxyImpl) map).getService();
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        PartitionContainer container = mapServiceContext.getPartitionContainer(partitionId);
        RecordStore recordStore = container.getRecordStore(map.getName());
        return ((WriteBehindStore) recordStore.getMapDataStore()).getWriteBehindQueue().size();
    }

    private class ExceptionThrowerMapStore extends MapStoreAdapter {
        @Override
        public void store(Object key, Object value) {
            throw new RuntimeException("Failed to store DB");
        }
    }

    @Test
    public void testStoreOperationDone_afterTemporaryMapStoreFailure() throws Exception {
        final SelfHealingMapStore<Integer, Integer> mapStore = new SelfHealingMapStore<Integer, Integer>();
        final IMap<Integer, Integer> map = TestMapUsingMapStoreBuilder.<Integer, Integer>create()
                .withMapStore(mapStore)
                .withNodeCount(1)
                .withNodeFactory(createHazelcastInstanceFactory(1))
                .withWriteDelaySeconds(1)
                .withPartitionCount(1)
                .build();

        map.put(1, 2);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(1, mapStore.size());
            }
        });
    }

    @Test
    public void testStoreOperationDone_afterTemporaryMapStoreFailure_whenNonWriteCoalescingModeOn() throws Exception {
        final SelfHealingMapStore<Integer, Integer> mapStore = new SelfHealingMapStore<Integer, Integer>();
        final IMap<Integer, Integer> map = TestMapUsingMapStoreBuilder.<Integer, Integer>create()
                .withMapStore(mapStore)
                .withNodeCount(1)
                .withNodeFactory(createHazelcastInstanceFactory(1))
                .withWriteDelaySeconds(1)
                .withWriteCoalescing(false)
                .withPartitionCount(1)
                .build();

        map.put(1, 2);
        map.put(1, 3);
        map.put(1, 4);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(1, mapStore.size());
            }
        });
    }

    static class SelfHealingMapStore<K, V> extends MapStoreAdapter<K, V> {

        private final ConcurrentMap<K, V> store = new ConcurrentHashMap<K, V>();
        private final TemporarySuccessProducer temporarySuccessProducer = new TemporarySuccessProducer(4);

        @Override
        public void store(K key, V value) {
            temporarySuccessProducer.successOrException();

            store.put(key, value);
        }

        public int size() {
            return store.size();
        }
    }

    /**
     * Produces periodic success, otherwise there will be many exceptions.
     */
    private static class TemporarySuccessProducer {

        private final long successGenerationPeriodInMillis;

        private volatile long startMillis;

        TemporarySuccessProducer(long secondsPeriod) {
            this.successGenerationPeriodInMillis = TimeUnit.SECONDS.toMillis(secondsPeriod);
            this.startMillis = Clock.currentTimeMillis();
        }

        void successOrException() {
            final long now = Clock.currentTimeMillis();
            final long elapsedTime = now - startMillis;

            if (elapsedTime > successGenerationPeriodInMillis) {
                startMillis = Clock.currentTimeMillis();
                return;
            }

            throw new TemporaryMapStoreException();
        }
    }

    private static class TemporaryMapStoreException extends RuntimeException {

        TemporaryMapStoreException() {
            super("Test exception");
        }
    }

    @Test
    public void testOOMHandlerCalled_whenOOMEOccursDuringStoreOperations() throws Exception {
        LeakyMapStore<Integer, Integer> mapStore = new LeakyMapStore<Integer, Integer>();
        IMap<Integer, Integer> map = TestMapUsingMapStoreBuilder.<Integer, Integer>create()
                .withMapStore(mapStore)
                .withNodeCount(1)
                .withNodeFactory(createHazelcastInstanceFactory(1))
                .withWriteDelaySeconds(1)
                .withPartitionCount(1)
                .build();

        final CountDownLatch OOMHandlerCalled = new CountDownLatch(1);
        OutOfMemoryErrorDispatcher.setServerHandler(new OutOfMemoryHandler() {
            @Override
            public void onOutOfMemory(OutOfMemoryError oome, HazelcastInstance[] hazelcastInstances) {
                OOMHandlerCalled.countDown();
            }
        });

        // trigger OOM exception creation in map-store call
        map.put(1, 2);

        assertOpenEventually("OutOfMemoryHandler should be called", OOMHandlerCalled);
    }

    /**
     * This map-store causes {@link OutOfMemoryError}.
     */
    static class LeakyMapStore<K, V> extends MapStoreAdapter<K, V> {

        @Override
        public void store(K key, V value) {
            throw new OutOfMemoryError("Error for testing map-store when OOM exception raised");
        }
    }

    @Test
    public void testPartialStoreOperationDone_afterTemporaryMapStoreFailure() throws Exception {
        final int numEntriesToStore = 6;
        final SequentialMapStore<Integer, Integer> mapStore
                = new SequentialMapStore<Integer, Integer>(5, numEntriesToStore);
        IMap<Integer, Integer> map = TestMapUsingMapStoreBuilder.<Integer, Integer>create()
                .withMapStore(mapStore)
                .withNodeCount(1)
                .withNodeFactory(createHazelcastInstanceFactory(1))
                .withWriteDelaySeconds(2)
                .withPartitionCount(1)
                .build();

        for (int i = 0; i < numEntriesToStore; i++) {
            map.put(i, i);
        }
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(numEntriesToStore, mapStore.storeCount());
            }
        });
    }

    /**
     * This map-store stores only one entry at storeAll call, then throws an exception;
     * Used to test partial failure processing.
     */
    static class SequentialMapStore<K, V> extends MapStoreAdapter<K, V> {

        boolean failed;
        final int failAfterStoreNum;
        final int numEntriesToStore;
        final AtomicInteger storeCount = new AtomicInteger(0);

        SequentialMapStore(int failAfterStoreNum, int numEntriesToStore) {
            this.failAfterStoreNum = failAfterStoreNum;
            this.numEntriesToStore = numEntriesToStore;
        }


        @Override
        public void storeAll(Map<K, V> map) {
            for (Map.Entry<K, V> entry : map.entrySet()) {
                K key = entry.getKey();
                store(key, entry.getValue());
                map.remove(key);
            }
        }

        @Override
        public void store(K key, V value) {
            int succeededStoreCount = storeCount.get();

            if (!failed && succeededStoreCount == failAfterStoreNum) {
                failed = true;
                throw new TemporaryMapStoreException();
            }

            storeCount.incrementAndGet();
        }

        public int storeCount() {
            return storeCount.get();
        }
    }

}
