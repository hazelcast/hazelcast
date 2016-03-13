/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapStore;
import com.hazelcast.core.MapStoreAdapter;
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

        IMap map = instance.getMap("default");

        int numberOfPuts = 2;
        for (int i = 0; i < numberOfPuts; i++) {
            map.put(i, i);
            sleepSeconds(4);
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(1, store.getStoreAllCount());
            }
        });
    }

    protected Config newMapStoredConfig(MapStore store, int writeDelaySeconds) {
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

        private final AtomicInteger storeAll = new AtomicInteger();

        @Override
        public void storeAll(Map map) {
            storeAll.incrementAndGet();
        }

        public int getStoreAllCount() {
            return storeAll.get();
        }
    }


    /**
     * Updates on same key should not shift store time.
     */
    @Test
    public void continuouslyUpdatedKey_shouldBeStored_inEveryWriteDelayTimeWindow() throws Exception {
        final MapStoreWithCounter mapStore = new MapStoreWithCounter<Integer, String>();
        final IMap<Object, Object> map = TestMapUsingMapStoreBuilder.create()
                .withMapStore(mapStore)
                .withNodeCount(1)
                .withNodeFactory(createHazelcastInstanceFactory(1))
                .withWriteDelaySeconds(2)
                .withPartitionCount(1)
                .build();


        for (int i = 1; i <= 60; i++) {
            map.put(1, i);
            sleepMillis(100);
        }

        // min expected 2 --> Expect to observe at least 2 store operations in every write-delay-seconds window.
        // Currently it is 2 seconds. We don't want to see 1 store operation in the course of this 60 * 100 millis test period.
        // It should be bigger than 1.
        //
        // max expected 3 --> 60 * 100 millis / 2 * 1000 millis
        assertMinMaxStoreOperationsCount(2, 3, mapStore);
    }

    private void assertMinMaxStoreOperationsCount(final int minimumExpectedStoreOperationCount,
                                                  final int maximumExpectedStoreOperationCount,
                                                  final MapStoreWithCounter mapStore) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                final Object value = mapStore.store.get(1);
                final int countStore = mapStore.countStore.get();

                assertEquals(60, value);
                assertTrue("Minimum store operation count should be bigger than 1 but found = " + countStore,
                        countStore >= minimumExpectedStoreOperationCount);
                assertTrue("Maximum store operation count should be smaller than " + maximumExpectedStoreOperationCount
                                + " but found = " + countStore,
                        countStore <= maximumExpectedStoreOperationCount);
            }
        });
    }
}
