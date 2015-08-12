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

package com.hazelcast.client.map;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.HazelcastTestSupport.randomMapName;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class ClientMapNearCacheSpeedTest {

    private static final String NEAR_CACHE_WITH_INVALIDATION = "NEAR_CACHE_WITH_INVALIDATION";

    private HazelcastInstance client;
    private HazelcastInstance instance1;
    private HazelcastInstance instance2;

    @Before
    public void setup() {
        ClientConfig clientConfig = new ClientConfig();
        NearCacheConfig invalidateConfig = new NearCacheConfig();
        invalidateConfig.setName(NEAR_CACHE_WITH_INVALIDATION + "*");
        invalidateConfig.setInvalidateOnChange(true);
        clientConfig.addNearCacheConfig(invalidateConfig);

        instance1 = Hazelcast.newHazelcastInstance();
        instance2 = Hazelcast.newHazelcastInstance();
        client = HazelcastClient.newHazelcastClient(clientConfig);

    }

    @After
    public void tearDown() {
        client.getLifecycleService().terminate();
        instance1.getLifecycleService().terminate();
        instance2.getLifecycleService().terminate();
    }

    @Test
    public void testNearCacheFasterThanGoingToTheCluster() {
        final IMap map = client.getMap(randomMapName(NEAR_CACHE_WITH_INVALIDATION));

        final int size = 2007;
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }

        long begin = System.currentTimeMillis();
        for (int i = 0; i < size; i++) {
            map.get(i);
        }
        long readFromClusterTime = System.currentTimeMillis() - begin;

        begin = System.currentTimeMillis();
        for (int i = 0; i < size; i++) {
            map.get(i);
        }
        long readFromCacheTime = System.currentTimeMillis() - begin;

        assertTrue("readFromCacheTime > readFromClusterTime", readFromCacheTime < readFromClusterTime);
    }
}
