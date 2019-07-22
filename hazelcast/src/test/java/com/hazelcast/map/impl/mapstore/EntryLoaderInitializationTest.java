/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.config.MapStoreConfig.InitialLoadMode.EAGER;
import static com.hazelcast.config.MapStoreConfig.InitialLoadMode.LAZY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EntryLoaderInitializationTest extends HazelcastTestSupport {

    @Test
    public void testEagerEntryLoaderInitializesValues() {
        final int entryCount = 100;
        TestEntryLoader entryLoader = createAndInitializeEntryLoader(entryCount);
        HazelcastInstance instance = createHazelcastInstance(createConfig(EAGER, entryLoader));
        IMap<String, String> map = instance.getMap(randomMapName());
        assertEquals(1, entryLoader.getLoadAllKeysCallCount());
        assertTrue(0 < entryLoader.getLoadAllCallCount());

        for (int i = 0; i < entryCount; i++) {
            assertEquals("val" + i, map.get("key" + i));
        }
        assertEquals(0, entryLoader.getLoadCallCount());
    }

    @Test
    public void testEagerEntryLoaderInitializesValues_withExpirationTime() {
        final int entryCount = 100;
        TestEntryLoader entryLoader = createAndInitializeEntryLoader(entryCount, System.currentTimeMillis() + 5000);
        HazelcastInstance instance = createHazelcastInstance(createConfig(EAGER, entryLoader));
        IMap<String, String> map = instance.getMap(randomMapName());
        assertEquals(1, entryLoader.getLoadAllKeysCallCount());
        assertTrue(0 < entryLoader.getLoadAllCallCount());

        for (int i = 0; i < entryCount; i++) {
            assertEquals("val" + i, map.get("key" + i));
        }
        assertEquals(0, entryLoader.getLoadCallCount());
        sleepAtLeastSeconds(6);
        for (int i = 0; i < entryCount; i++) {
            assertNull(map.get("key" + i));
        }

        assertEquals(entryCount, entryLoader.getLoadCallCount());
    }

    @Test
    public void testLazyEntryLoaderInitializesAfterAccess() {
        final int entryCount = 100;
        TestEntryLoader entryLoader = createAndInitializeEntryLoader(entryCount);
        HazelcastInstance instance = createHazelcastInstance(createConfig(LAZY, entryLoader));
        IMap<String, String> map = instance.getMap(randomMapName());
        assertEquals(0, entryLoader.getLoadAllKeysCallCount());
        assertEquals(0, entryLoader.getLoadAllCallCount());

        for (int i = 0; i < entryCount; i++) {
            assertEquals("val" + i, map.get("key" + i));
        }
        assertEquals(0, entryLoader.getLoadCallCount());
        assertTrue(0 < entryLoader.getLoadAllCallCount());
    }

    @Test
    public void testLazyEntryLoaderInitializesAfterAccess_withExpirationTime() {
        final int entryCount = 100;
        TestEntryLoader entryLoader = createAndInitializeEntryLoader(entryCount, System.currentTimeMillis() + 5000);
        HazelcastInstance instance = createHazelcastInstance(createConfig(LAZY, entryLoader));
        IMap<String, String> map = instance.getMap(randomMapName());
        assertEquals(0, entryLoader.getLoadAllKeysCallCount());
        assertEquals(0, entryLoader.getLoadAllCallCount());

        for (int i = 0; i < entryCount; i++) {
            assertEquals("val" + i, map.get("key" + i));
        }
        sleepAtLeastSeconds(6);

        for (int i = 0; i < entryCount; i++) {
            assertNull(map.get("key" + i));
        }
        assertEquals(entryCount, entryLoader.getLoadCallCount());
    }

    private Config createConfig(MapStoreConfig.InitialLoadMode loadMode, EntryLoader entryLoader) {
        Config config = new Config();
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setInitialLoadMode(loadMode);
        mapStoreConfig.setEnabled(true).setImplementation(entryLoader);
        config.getMapConfig("default").setMapStoreConfig(mapStoreConfig);
        return config;
    }

    private TestEntryLoader createAndInitializeEntryLoader(int entryCount, long expirationTime) {
        TestEntryLoader entryLoader = new TestEntryLoader();
        for (int i = 0; i < entryCount; i++) {
            entryLoader.putExternally("key" + i, "val" + i, expirationTime);
        }
        return entryLoader;
    }

    private TestEntryLoader createAndInitializeEntryLoader(int entryCount) {
        TestEntryLoader entryLoader = new TestEntryLoader();
        for (int i = 0; i < entryCount; i++) {
            entryLoader.putExternally("key" + i, "val" + i);
        }
        return entryLoader;
    }
}
