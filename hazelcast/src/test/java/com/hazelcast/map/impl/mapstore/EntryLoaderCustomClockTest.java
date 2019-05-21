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
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.AbstractClockTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class EntryLoaderCustomClockTest extends AbstractClockTest {

    private HazelcastInstance instance;
    private Map<String, String> map;
    private TestEntryLoader testEntryLoader = new TestEntryLoader();

    @Before
    public void setup() {
        setClockOffset(10000);
        instance = startNode();
        map = instance.getMap(randomMapName());
    }

    @After
    public void teardown() {
        instance.getLifecycleService().terminate();
        resetClock();
    }

    @Test
    public void testWithClockSkew() {
        testEntryLoader.putExternally("key", "val", System.currentTimeMillis() + 2000);
        assertEquals("val", map.get("key"));
        sleepAtLeastSeconds(2);
        assertNull(map.get("key"));
    }

    @Override
    protected Config getConfig() {
        Config config = super.getConfig();
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setImplementation(testEntryLoader).setEnabled(true);
        config.getMapConfig("default").setMapStoreConfig(mapStoreConfig);
        return config;
    }
}
