/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.IMap;
import com.hazelcast.map.impl.mapstore.MapStoreTest;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class WriteBehindUponMigrationTest extends HazelcastTestSupport {

    @Test
    public void testRemovedEntry_shouldNotBeReached_afterMigration() throws Exception {
        String mapName = randomMapName();
        TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(2);
        MapStoreTest.SimpleMapStore<Integer, Integer> store
                = new MapStoreTest.SimpleMapStore<Integer, Integer>();
        store.store.put(1, 0);

        Config config = createConfig(mapName, store);
        HazelcastInstance node1 = factory.newHazelcastInstance(config);

        IMap<Integer, Integer> map = node1.getMap(mapName);

        map.put(1, 1);
        map.delete(1);

        HazelcastInstance node2 = factory.newHazelcastInstance(config);

        map = node2.getMap(mapName);

        Integer value = map.get(1);
        factory.shutdownAll();

        assertNull(value);
    }


    private static Config createConfig(String mapName, MapStoreTest.SimpleMapStore store) {
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig
                .setImplementation(store)
                .setWriteDelaySeconds(100)
                .setWriteBatchSize(1)
                .setWriteCoalescing(false);

        Config config = new Config();
        config.getMapConfig(mapName)
                .setBackupCount(1)
                .setMapStoreConfig(mapStoreConfig);

        return config;
    }
}
