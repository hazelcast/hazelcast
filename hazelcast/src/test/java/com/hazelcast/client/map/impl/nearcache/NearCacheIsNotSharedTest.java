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
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.replicatedmap.ReplicatedMap;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class NearCacheIsNotSharedTest {

    private TestHazelcastFactory factory;

    @Before
    public void setUp() {
        factory = new TestHazelcastFactory();
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void nearCacheShouldNotBeUsedByDifferentDataStructure() {
        HazelcastInstance hz = factory.newHazelcastInstance();

        ClientConfig config = new ClientConfig();
        config.addNearCacheConfig(new NearCacheConfig("test"));
        HazelcastInstance client = factory.newHazelcastClient(config);

        IMap<String, String> map = client.getMap("test");
        ReplicatedMap<String, String> replicatedMap = client.getReplicatedMap("test");

        replicatedMap.put("key", "replicated-map-value");
        map.put("key", "map-value");
        map.get("key");
        assertEquals(replicatedMap.get("key"), "replicated-map-value");
    }
}
