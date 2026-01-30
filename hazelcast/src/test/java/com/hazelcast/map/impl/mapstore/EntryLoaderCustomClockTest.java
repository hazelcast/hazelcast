/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.util.AbstractClockTest;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class EntryLoaderCustomClockTest extends AbstractClockTest {

    private IMap<String, String> map;

    @Before
    public void setup() {
        setClockOffset(Duration.ofHours(3).toMillis());
        startIsolatedNode(getConfig());
        map = isolatedNode.getMap(randomMapName());
    }

    @After
    public void teardown() {
        shutdownIsolatedNode();
        resetClock();
    }

    // If the Hazelcast server clock differs from the system clock,
    // then when retrieving from the external system, the time is NOT converted to the server clock.
    @Test
    public void testEntryLoader() {
        assertNull(map.get("key"));

        invokeIsolatedInstanceMethod(
            SingletonTestEntryStoreFactory.class.getName(),
            "putExternally",
            new Class[]{String.class, String.class, long.class},
            "key",
            "val",
            System.currentTimeMillis() + 2000
        );
        assertNull(map.get("key"));
    }


    // If the Hazelcast server clock differs from the system clock,
    // then when storing a value to the external system, the server time is converted to system time.
    @Test
    public void testEntryStore() {
        map.put("key", "val", 1, TimeUnit.DAYS);
        long expectedExpirationTime = System.currentTimeMillis() + TimeUnit.DAYS.toMillis(1);

        invokeIsolatedInstanceMethod(
            SingletonTestEntryStoreFactory.class.getName(),
            "assertRecordStored",
            new Class[]{String.class, String.class, long.class, long.class},
            "key",
            "val",
            expectedExpirationTime,
            5000
        );

    }

    @Override
    protected Config getConfig() {
        Config config = smallInstanceConfig();
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setFactoryClassName(SingletonTestEntryStoreFactory.class.getName()).setEnabled(true);
        config.getMapConfig("default").setMapStoreConfig(mapStoreConfig);
        return config;
    }
}
