/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapStoreAdapter;
import com.hazelcast.core.OutOfMemoryHandler;
import com.hazelcast.instance.OutOfMemoryErrorDispatcher;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.Clock;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class WriteBehindFailAndRetryTest extends HazelcastTestSupport {

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

        public TemporaryMapStoreException() {
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
        final SequentialMapStore<Integer, Integer> mapStore = new SequentialMapStore<Integer, Integer>();
        final IMap<Integer, Integer> map = TestMapUsingMapStoreBuilder.<Integer, Integer>create()
                .withMapStore(mapStore)
                .withNodeCount(1)
                .withNodeFactory(createHazelcastInstanceFactory(1))
                .withWriteDelaySeconds(1)
                .withPartitionCount(1)
                .build();

        map.put(1, 2);
        map.put(2, 3);
        map.put(3, 4);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(3, mapStore.storeCount());
            }
        });
    }

    /**
     * This map-store stores only one entry at storeAll call, then throws an exception;
     * Used to test partial failure processing.
     */
    static class SequentialMapStore<K, V> extends MapStoreAdapter<K, V> {

        private int cnt = 0;

        @Override
        public void storeAll(Map<K, V> entries) {
            entries.remove(entries.keySet().iterator().next());
            cnt++;
            if (entries.size() > 0) {
                throw new TemporaryMapStoreException();
            }
        }

        public int storeCount() {
            return cnt;
        }
    }

}
