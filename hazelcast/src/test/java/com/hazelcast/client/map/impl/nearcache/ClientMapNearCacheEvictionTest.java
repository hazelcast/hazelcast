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

package com.hazelcast.client.map.impl.nearcache;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientMapNearCacheEvictionTest extends HazelcastTestSupport {

    private final TestHazelcastFactory factory = new TestHazelcastFactory();

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Test
    public void near_cache_size_equals_map_size_when_eviction_policy_is_none() {
        String mapName = "test";
        NearCacheConfig nearCacheConfig = new NearCacheConfig(mapName);
        nearCacheConfig.getEvictionConfig().setEvictionPolicy(EvictionPolicy.NONE);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.addNearCacheConfig(nearCacheConfig);

        HazelcastInstance server = factory.newHazelcastInstance();
        HazelcastInstance client = factory.newHazelcastClient(clientConfig);
        IMap<Integer, Integer> map = client.getMap(mapName);

        // populate map
        int mapSize = 99_999;
        for (int i = 0; i < mapSize; i++) {
            map.set(i, i);
        }

        // populate near-cache
        for (int i = 0; i < mapSize; i++) {
            map.get(i);
        }

        assertEquals(mapSize, map.getLocalMapStats()
                .getNearCacheStats().getOwnedEntryCount());
    }

    @Test
    public void no_more_entries_than_max_near_cache_size_when_eviction_policy_is_not_none() {
        String mapName = "mapName";
        NearCacheConfig nearCacheConfig = new NearCacheConfig(mapName);
        int maxNearCacheSize = 9_999;
        nearCacheConfig.getEvictionConfig()
                .setEvictionPolicy(EvictionPolicy.LRU)
                .setSize(maxNearCacheSize);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.addNearCacheConfig(nearCacheConfig);

        HazelcastInstance server = factory.newHazelcastInstance();
        HazelcastInstance client = factory.newHazelcastClient(clientConfig);
        IMap<Integer, Integer> map = client.getMap(mapName);

        // populate map
        int mapSize = 99_999;
        for (int i = 0; i < mapSize; i++) {
            map.set(i, i);
        }

        // populate near-cache
        for (int i = 0; i < mapSize; i++) {
            map.get(i);
        }

        assertEquals(maxNearCacheSize, map.getLocalMapStats()
                .getNearCacheStats().getOwnedEntryCount());
    }
}
