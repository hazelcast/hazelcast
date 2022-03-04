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
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapInterceptor;
import com.hazelcast.map.MapStore;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class StoreWorkerConcurrentUpdateTest extends HazelcastTestSupport {

    /**
     * Reproducer for https://github.com/hazelcast/hazelcast/issues/15060
     */
    @Test
    public void testCoalescedWBQ_noUpdatesLost_whenEqualEntryAddedConcurrently() {
        doTestAddEqualEntryConcurrently(true);
    }

    @Test
    public void testBoundedWBQ_noUpdatesLost_whenEqualEntryAddedConcurrently() {
        doTestAddEqualEntryConcurrently(false);
    }

    private void doTestAddEqualEntryConcurrently(boolean coalesced) {
        String mapName = randomName();

        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        final NewVersionConcurrentInsertMapStore mapStore = new NewVersionConcurrentInsertMapStore();
        mapStoreConfig.setImplementation(mapStore).setWriteDelaySeconds(1).setWriteCoalescing(coalesced);

        Config config = getConfig();
        config.getMapConfig(mapName).setBackupCount(0).setMapStoreConfig(mapStoreConfig);
        HazelcastInstance instance = createHazelcastInstance(config);

        IMap<Integer, Entry> map = instance.getMap(mapName);
        map.addInterceptor(new DummyMapInterceptor());
        mapStore.setMap(map);

        final int entryCount = 100;
        for (int i = 0; i < entryCount; i++) {
            map.put(i, new Entry(i, 1));
        }

        assertTrueEventually(() -> {
            assertEquals(entryCount, mapStore.versionOne.size());
            assertEquals(entryCount, mapStore.versionTwo.size());
        });
    }

    private static final class Entry implements Serializable {

        private int id;
        private int version;

        Entry(int id, int version) {
            this.id = id;
            this.version = version;
        }

        public Entry newVersion() {
            return new Entry(id, version + 1);
        }

        @Override
        public int hashCode() {
            return id;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Entry)) {
                return false;
            }
            // different versions are considered equal
            return id == ((Entry) o).id;
        }
    }

    private static final class DummyMapInterceptor implements MapInterceptor {

        @Override
        public Object interceptGet(Object value) {
            return null;
        }

        @Override
        public void afterGet(Object value) {
        }

        @Override
        public Object interceptPut(Object oldValue, Object newValue) {
            /* Return non-null to make sure a plain object (rather than the
               serialized version) will be stored in the write behind queue. */
            return newValue;
        }

        @Override
        public void afterPut(Object value) {
        }

        @Override
        public Object interceptRemove(Object removedValue) {
            return null;
        }

        @Override
        public void afterRemove(Object oldValue) {
        }
    }

    /**
     * Simulates a concurrent addition of a new Entry version to the write
     * behind queue right after {@link StoreWorker} selects entries to store.
     */
    private static final class NewVersionConcurrentInsertMapStore implements MapStore<Integer, Entry> {

        private IMap<Integer, Entry> map;
        private Set<Entry> versionOne = new HashSet<>();
        private Set<Entry> versionTwo = new HashSet<>();

        private void setMap(IMap<Integer, Entry> map) {
            this.map = map;
        }

        @Override
        public void store(Integer key, Entry value) {
            if (value.version == 1) {
                versionOne.add(value);
                // add version 2 of the entry to the write behind queue
                map.put(key, value.newVersion());
            } else {
                versionTwo.add(value);
            }
        }

        @Override
        public void storeAll(Map<Integer, Entry> map) {
            for (Map.Entry<Integer, Entry> e : map.entrySet()) {
                store(e.getKey(), e.getValue());
            }
        }

        @Override
        public void delete(Integer key) {
        }

        @Override
        public void deleteAll(Collection<Integer> keys) {
        }

        @Override
        public Entry load(Integer key) {
            return null;
        }

        @Override
        public Map<Integer, Entry> loadAll(Collection<Integer> keys) {
            return null;
        }

        @Override
        public Iterable<Integer> loadAllKeys() {
            return null;
        }
    }

}
