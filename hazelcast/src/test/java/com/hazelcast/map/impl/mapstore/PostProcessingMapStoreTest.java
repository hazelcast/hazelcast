/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapStore;
import com.hazelcast.core.PostProcessingMapStore;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryUpdatedListener;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class PostProcessingMapStoreTest extends HazelcastTestSupport {

    private TestHazelcastInstanceFactory factory;

    @Before
    public void init() {
        factory = createHazelcastInstanceFactory(2);
    }

    @After
    public void cleanup() {
        factory.terminateAll();
    }

    @Test
    public void testProcessedValueCarriedToTheBackup() {
        String name = randomString();
        Config config = new Config();
        MapConfig mapConfig = config.getMapConfig(name);
        mapConfig.setReadBackupData(true);
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true).setClassName(IncrementerPostProcessingMapStore.class.getName());
        mapConfig.setMapStoreConfig(mapStoreConfig);

        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        HazelcastInstance instance2 = factory.newHazelcastInstance(config);

        IMap<Integer, SampleObject> map1 = instance1.getMap(name);
        IMap<Integer, SampleObject> map2 = instance2.getMap(name);

        for (int i = 0; i < 100; i++) {
            map1.put(i, new SampleObject(i));
        }

        for (int i = 0; i < 100; i++) {
            SampleObject o = map1.get(i);
            assertEquals(i + 1, o.version);
        }

        for (int i = 0; i < 100; i++) {
            SampleObject o = map2.get(i);
            assertEquals(i + 1, o.version);
        }
    }

    @Test
    public void testEntryListenerIncludesTheProcessedValue_onPut() {
        IMap<Integer, SampleObject> map = createInstanceAndGetMap();
        int count = 10;
        final CountDownLatch latch = new CountDownLatch(count);
        map.addEntryListener(new EntryAddedListener<Integer, SampleObject>() {
            @Override
            public void entryAdded(EntryEvent<Integer, SampleObject> event) {
                assertEquals(event.getKey() + 1, event.getValue().version);
                latch.countDown();
            }
        }, true);
        for (int i = 0; i < count; i++) {
            map.put(i, new SampleObject(i));
        }
        assertOpenEventually(latch);
    }

    @Test
    public void testEntryListenerIncludesTheProcessedValue_onPutAll() {
        IMap<Integer, SampleObject> map = createInstanceAndGetMap();
        int count = 10;
        final CountDownLatch latch = new CountDownLatch(count);
        map.addEntryListener(new EntryAddedListener<Integer, SampleObject>() {
            @Override
            public void entryAdded(EntryEvent<Integer, SampleObject> event) {
                assertEquals(event.getKey() + 1, event.getValue().version);
                latch.countDown();
            }
        }, true);
        Map<Integer, SampleObject> localMap = new HashMap<Integer, SampleObject>();
        for (int i = 0; i < count; i++) {
            localMap.put(i, new SampleObject(i));
        }
        map.putAll(localMap);
        assertOpenEventually(latch);
    }

    @Test
    public void testEntryListenerIncludesTheProcessedValue_onEntryProcessor() {
        IMap<Integer, SampleObject> map = createInstanceAndGetMap();
        int count = 10;
        final CountDownLatch latch = new CountDownLatch(count);
        map.addEntryListener(new EntryUpdatedListener<Integer, SampleObject>() {
            @Override
            public void entryUpdated(EntryEvent<Integer, SampleObject> event) {
                // value is incremented three times :
                // +1 -> post processing map store
                // +1 -> entry processor
                // +1 -> post processing map store
                assertEquals(event.getKey() + 3, event.getValue().version);
                latch.countDown();
            }
        }, true);
        for (int i = 0; i < count; i++) {
            map.put(i, new SampleObject(i));
            map.executeOnKey(i, new EntryProcessor<Integer, SampleObject>() {
                @Override
                public Object process(Map.Entry<Integer, SampleObject> entry) {
                    SampleObject value = entry.getValue();
                    value.version++;
                    entry.setValue(value);
                    return null;
                }

                @Override
                public EntryBackupProcessor<Integer, SampleObject> getBackupProcessor() {
                    return null;
                }
            });
        }
        assertOpenEventually(latch);
    }

    private IMap<Integer, SampleObject> createInstanceAndGetMap() {
        String name = randomString();
        Config config = new Config();
        MapConfig mapConfig = config.getMapConfig(name);
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true).setClassName(IncrementerPostProcessingMapStore.class.getName());
        mapConfig.setMapStoreConfig(mapStoreConfig);
        HazelcastInstance instance = factory.newHazelcastInstance(config);
        warmUpPartitions(instance);
        return instance.getMap(name);
    }

    public static class IncrementerPostProcessingMapStore implements MapStore<Integer, SampleObject>, PostProcessingMapStore {

        Map<Integer, SampleObject> map = new ConcurrentHashMap<Integer, SampleObject>();

        @Override
        public void store(Integer key, SampleObject value) {
            value.version++;
            map.put(key, value);
        }

        @Override
        public void storeAll(Map<Integer, SampleObject> map) {
            for (Map.Entry<Integer, SampleObject> entry : map.entrySet()) {
                store(entry.getKey(), entry.getValue());
            }
        }

        @Override
        public void delete(Integer key) {
            map.remove(key);
        }

        @Override
        public void deleteAll(Collection<Integer> keys) {
            for (Integer key : keys) {
                map.remove(key);
            }
        }

        @Override
        public SampleObject load(Integer key) {
            return map.get(key);
        }

        @Override
        public Map<Integer, SampleObject> loadAll(Collection<Integer> keys) {
            HashMap<Integer, SampleObject> temp = new HashMap<Integer, SampleObject>();
            for (Integer key : keys) {
                temp.put(key, map.get(key));
            }
            return temp;
        }

        @Override
        public Set<Integer> loadAllKeys() {
            return map.keySet();
        }
    }

    public static class SampleObject implements Serializable {

        public int version;

        SampleObject(int version) {
            this.version = version;
        }
    }
}
