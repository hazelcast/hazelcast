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

package com.hazelcast.client.map;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.LocalMapStatsTest;
import com.hazelcast.map.LocalMapStats;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;


@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientMapStatsTest extends LocalMapStatsTest {
    private TestHazelcastFactory factory = new TestHazelcastFactory();

    private String mapName = "mapName";
    private HazelcastInstance client;
    private HazelcastInstance member;

    @Before
    public void setUp() {
        member = factory.newHazelcastInstance(createMemberConfig());
        client = factory.newHazelcastClient();
    }

    @After
    public void cleanup() {
        factory.terminateAll();
    }

    @Override
    protected LocalMapStats getMapStats(String mapName) {
        return member.getMap(mapName).getLocalMapStats();
    }

    @Override
    public void testOtherOperationCount_localKeySet() {
        // localKeySet is not supported on client
    }

    @Override
    protected <K, V> IMap<K, V> getMap(String mapName) {
        return client.getMap(mapName);
    }
}
