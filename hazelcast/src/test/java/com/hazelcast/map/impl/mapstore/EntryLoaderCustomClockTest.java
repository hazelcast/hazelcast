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
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.internal.util.AbstractClockTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class EntryLoaderCustomClockTest extends AbstractClockTest {

    private HazelcastInstance instance;
    private IMap<String, String> map;
    private TestEntryStore<String, String> testEntryStore = new TestEntryStore<>();

    @Before
    public void setup() {
        setClockOffset(100000);
        instance = startNode();
        map = instance.getMap(randomMapName());
    }

    @After
    public void teardown() {
        instance.getLifecycleService().terminate();
        resetClock();
    }

    @Test
    public void testEntryLoader() {
        testEntryStore.putExternally("key", "val", System.currentTimeMillis() + 2000);
        assertEquals("val", map.get("key"));
        sleepAtLeastSeconds(2);
        assertNull(map.get("key"));
    }

    @Test
    public void testEntryStore() {
        map.put("key", "val", 1, TimeUnit.DAYS);
        long expectedExpirationTime = System.currentTimeMillis() + TimeUnit.DAYS.toMillis(1);
        testEntryStore.assertRecordStored("key", "val", expectedExpirationTime, 5000);
    }

    @Override
    protected Config getConfig() {
        Config config = smallInstanceConfig();
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setImplementation(testEntryStore).setEnabled(true);
        config.getMapConfig("default").setMapStoreConfig(mapStoreConfig);
        return config;
    }
}
