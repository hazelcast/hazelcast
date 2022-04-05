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

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapStore;
import com.hazelcast.map.MapStoreAdapter;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(NightlyTest.class)
public class WriteBehindWriteDelaySecondsTest extends HazelcastTestSupport {

    @Test
    public void testUpdatesInWriteDelayWindowDone_withStoreAllMethod() throws Exception {
        final TestMapStore store = new TestMapStore();
        Config config = newMapStoredConfig(store, 20);
        HazelcastInstance instance = createHazelcastInstance(config);

        IMap<Integer, Integer> map = instance.getMap("default");
        int numberOfPuts = 2;
        sleepSeconds(2);
        for (int i = 0; i < numberOfPuts; i++) {
            map.put(i, i);
            sleepSeconds(4);
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(store.toString(), 1, store.getStoreAllMethodCallCount());
            }
        });
    }

    private Config newMapStoredConfig(MapStore store, int writeDelaySeconds) {
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setWriteDelaySeconds(writeDelaySeconds);
        mapStoreConfig.setImplementation(store);

        Config config = getConfig();
        MapConfig mapConfig = config.getMapConfig("default");
        mapConfig.setMapStoreConfig(mapStoreConfig);

        return config;
    }

    private static class TestMapStore extends MapStoreAdapter {

        private final AtomicInteger storeMethodCallCount = new AtomicInteger();
        private final AtomicInteger storeAllMethodCallCount = new AtomicInteger();

        @Override
        public void store(Object key, Object value) {
            storeMethodCallCount.incrementAndGet();
        }

        @Override
        public void storeAll(Map map) {
            storeAllMethodCallCount.incrementAndGet();
        }

        int getStoreAllMethodCallCount() {
            return storeAllMethodCallCount.get();
        }

        int getStoreMethodCallCount() {
            return storeMethodCallCount.get();
        }

        @Override
        public String toString() {
            return "TestMapStore{"
                    + "storeMethodCallCount=" + getStoreMethodCallCount()
                    + ", storeAllMethodCallCount=" + getStoreAllMethodCallCount()
                    + '}';
        }
    }

    /**
     * Updates on same key should not shift store time.
     */
    @Test
    public void continuouslyUpdatedKey_shouldBeStored_inEveryWriteDelayTimeWindow() throws Exception {
        final MapStoreWithCounter<Integer, Integer> mapStore = new MapStoreWithCounter<Integer, Integer>();
        final IMap<Integer, Integer> map = TestMapUsingMapStoreBuilder.<Integer, Integer>create()
                .withMapStore(mapStore)
                .withNodeCount(1)
                .withNodeFactory(createHazelcastInstanceFactory(1))
                .withWriteDelaySeconds(6)
                .withPartitionCount(1)
                .build();

        for (int i = 1; i <= 60; i++) {
            map.put(1, i);
            sleepMillis(500);
        }

        // min expected 2 --> Expect to observe at least 2 store operations in every write-delay-seconds window.
        // Current write-delay-seconds is 6 seconds. We don't want to see 1 store operation in the course of this 60 * 500 millis test period.
        // It should be bigger than 1.

        assertMinMaxStoreOperationsCount(2, mapStore);
        mapStore.countStore.get();
    }

    private void assertMinMaxStoreOperationsCount(final int minimumExpectedStoreOperationCount,
                                                  final MapStoreWithCounter mapStore) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                final Object value = mapStore.store.get(1);
                final int countStore = mapStore.countStore.get();

                assertEquals(60, value);
                assertTrue("Minimum store operation count should be bigger than "
                                + minimumExpectedStoreOperationCount + " but found = " + countStore,
                        countStore >= minimumExpectedStoreOperationCount);
            }
        });
    }
}
