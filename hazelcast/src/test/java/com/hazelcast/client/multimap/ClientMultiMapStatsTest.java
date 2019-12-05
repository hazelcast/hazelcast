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

package com.hazelcast.client.multimap;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.multimap.LocalMultiMapStats;
import com.hazelcast.multimap.LocalMultiMapStatsTest;
import com.hazelcast.multimap.MultiMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientMultiMapStatsTest extends LocalMultiMapStatsTest {
    private TestHazelcastFactory factory = new TestHazelcastFactory();

    private String mapName = "mapName";
    private HazelcastInstance client;
    private HazelcastInstance member;

    @Before
    public void setUp() {
        member = factory.newHazelcastInstance();
        client = factory.newHazelcastClient();
    }

    @After
    public void cleanup() {
        factory.terminateAll();
    }

    @Override
    protected LocalMultiMapStats getMultiMapStats() {
        return member.getMultiMap(mapName).getLocalMultiMapStats();
    }

    @Override
    public void testOtherOperationCount_localKeySet() {
        // localKeySet is not supported on client
    }

    @Override
    protected <K, V> MultiMap<K, V> getMultiMap() {
        return client.getMultiMap(mapName);
    }

    @Override
    @Test
    @Ignore("GH issue 15307")
    public void testDelete() {
    }

    @Override
    @Test
    @Ignore("GH issue 15307")
    public void testGetAndHitsGenerated() {
    }

    @Override
    @Test
    @Ignore("GH issue 15307")
    public void testPutAndHitsGenerated() {
    }

    @Override
    @Test
    @Ignore("GH issue 15307")
    public void testRemove() {
    }
}
