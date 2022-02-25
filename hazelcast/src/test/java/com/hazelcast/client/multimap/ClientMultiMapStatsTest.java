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

package com.hazelcast.client.multimap;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.LocalMapStats;
import com.hazelcast.multimap.LocalMultiMapStats;
import com.hazelcast.multimap.LocalMultiMapStatsTest;
import com.hazelcast.multimap.MultiMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientMultiMapStatsTest extends LocalMultiMapStatsTest {
    private TestHazelcastFactory factory = new TestHazelcastFactory();

    private String mapName = "mapName";
    private String mapNameSet = "mapNameSet";
    private HazelcastInstance client;
    private HazelcastInstance member;

    @Before
    public void setUp() {
        member = factory.newHazelcastInstance();
        client = factory.newHazelcastClient();
        MultiMapConfig multiMapConfig1 = new MultiMapConfig()
                .setName(mapName)
                .setValueCollectionType(MultiMapConfig.ValueCollectionType.LIST);
        MultiMapConfig multiMapConfig2 = new MultiMapConfig()
                .setName(mapNameSet)
                .setValueCollectionType(MultiMapConfig.ValueCollectionType.SET);
        client.getConfig()
                .addMultiMapConfig(multiMapConfig1)
                .addMultiMapConfig(multiMapConfig2);
    }

    @After
    public void cleanup() {
        factory.terminateAll();
    }

    @Override
    public void testPutAllAndHitsGeneratedTemplateVerify() {
        LocalMapStats localMapStats1 = getMultiMapStats();
        LocalMapStats localMapStats2 = getMultiMapStats(mapNameSet);

        assertEquals(300, localMapStats1.getOwnedEntryCount());
        assertEquals(100, localMapStats1.getHits());
        assertEquals(100, localMapStats2.getOwnedEntryCount());
        assertEquals(100, localMapStats2.getHits());
    }

    @Override
    protected LocalMultiMapStats getMultiMapStats() {
        return getMultiMapStats(mapName);
    }

    @Override
    protected LocalMultiMapStats getMultiMapStats(String multiMapName) {
        return member.getMultiMap(multiMapName).getLocalMultiMapStats();
    }

    @Override
    public void testOtherOperationCount_localKeySet() {
        // localKeySet is not supported on client
    }

    @Override
    protected <K, V> MultiMap<K, V> getMultiMap() {
        return getMultiMap(mapName);
    }

    @Override
    protected <K, V> MultiMap<K, V> getMultiMap(String multiMapName) {
        return client.getMultiMap(multiMapName);
    }

}
