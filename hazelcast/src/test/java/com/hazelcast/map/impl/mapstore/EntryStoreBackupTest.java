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

package com.hazelcast.map.impl.mapstore;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.EntryLoader;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static junit.framework.TestCase.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EntryStoreBackupTest extends HazelcastTestSupport {

    private static final long TTL_MILLIS = 2000;

    @Test
    public void testPromotedEntriesAreRemovedAfterExpiration_loadAll() {
        final int entryCount = 1000;
        OnOffIdentityEntryLoader entryLoader = new OnOffIdentityEntryLoader();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        Config config = new Config();
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setImplementation(entryLoader).setEnabled(true);
        config.getMapConfig("default").setMapStoreConfig(mapStoreConfig);
        HazelcastInstance[] instances = factory.newInstances(config);

        IMap<Integer, Integer> map = instances[0].getMap(randomMapName());
        Set<Integer> keys = new HashSet<>();
        for (int i = 0; i < entryCount; i++) {
            keys.add(i);
        }
        map.loadAll(keys, false);
        factory.terminate(instances[1]);
        entryLoader.disable();

        sleepAtLeastMillis(TTL_MILLIS);
        for (int i = 0; i < entryCount; i++) {
            assertNull(map.get(i));
        }
    }

    @Test
    public void testPromotedEntriesAreRemovedAfterExpiration_getAll() {
        final int entryCount = 1000;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        OnOffIdentityEntryLoader entryLoader = new OnOffIdentityEntryLoader();
        Config config = new Config();
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setImplementation(entryLoader).setEnabled(true);
        config.getMapConfig("default").setMapStoreConfig(mapStoreConfig);
        HazelcastInstance[] instances = factory.newInstances(config);

        IMap<Integer, Integer> map = instances[0].getMap(randomMapName());
        Set<Integer> keys = new HashSet<>();
        for (int i = 0; i < entryCount; i++) {
            keys.add(i);
        }
        map.getAll(keys);
        factory.terminate(instances[1]);
        entryLoader.disable();

        sleepAtLeastMillis(TTL_MILLIS);
        for (int i = 0; i < entryCount; i++) {
            assertNull(map.get(i));
        }
    }

    @Test
    public void testPromotedEntryIsRemovedAfterExpiration_get() {
        final int entryCount = 1000;
        OnOffIdentityEntryLoader entryLoader = new OnOffIdentityEntryLoader();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        Config config = new Config();
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setImplementation(entryLoader).setEnabled(true);
        config.getMapConfig("default").setMapStoreConfig(mapStoreConfig);
        HazelcastInstance[] instances = factory.newInstances(config);

        IMap<Integer, Integer> map = instances[0].getMap(randomMapName());
        for (int i = 0; i < entryCount; i++) {
            map.get(i);
        }
        factory.terminate(instances[1]);
        entryLoader.disable();

        sleepAtLeastMillis(TTL_MILLIS);
        for (int i = 0; i < entryCount; i++) {
            assertNull(map.get(i));
        }
    }

    private static class OnOffIdentityEntryLoader implements EntryLoader<Integer, Integer> {

        private volatile boolean enabled = true;

        @Override
        public MetadataAwareValue<Integer> load(Integer key) {
            if (!enabled) {
                return null;
            }
            return new MetadataAwareValue<>(key, System.currentTimeMillis() + TTL_MILLIS);
        }

        @Override
        public Map<Integer, MetadataAwareValue<Integer>> loadAll(Collection<Integer> keys) {
            if (!enabled) {
                return null;
            }
            Map<Integer, MetadataAwareValue<Integer>> map = new HashMap<>();
            long expirationTime = System.currentTimeMillis() + TTL_MILLIS;
            for (Integer key : keys) {
                map.put(key, new MetadataAwareValue<>(key, expirationTime));
            }
            return map;
        }

        @Override
        public Iterable<Integer> loadAllKeys() {
            return null;
        }

        public void enable() {
            enabled = true;
        }

        public void disable() {
            enabled = false;
        }
    }
}
