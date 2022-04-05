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

import com.hazelcast.client.HazelcastClientOfflineException;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientConnectionStrategyConfig;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientNonStopNearCacheTest extends ClientTestSupport {

    private final TestHazelcastFactory factory = new TestHazelcastFactory();
    private IMap<Object, Object> map;

    @Before
    public void init() {
        HazelcastInstance member = factory.newHazelcastInstance();
        ClientConfig config = new ClientConfig();
        config.getConnectionStrategyConfig().setReconnectMode(ClientConnectionStrategyConfig.ReconnectMode.ASYNC);
        config.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Integer.MAX_VALUE);

        NearCacheConfig nearCacheConfig = new NearCacheConfig("map");
        config.addNearCacheConfig(nearCacheConfig);

        CountDownLatch disconnectedLatch = new CountDownLatch(1);
        config.addListenerConfig(new ListenerConfig().setImplementation((LifecycleListener) event -> {
            if (LifecycleEvent.LifecycleState.CLIENT_DISCONNECTED.equals(event.getState())) {
                disconnectedLatch.countDown();
            }
        }));
        HazelcastInstance client = factory.newHazelcastClient(config);

        map = client.getMap("map");


        for (int i = 0; i < 100; i++) {
            map.put(i, i);
        }

        //populate near cache
        for (int i = 0; i < 100; i++) {
            map.get(i);
        }

        member.shutdown();

        assertOpenEventually(disconnectedLatch);
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void testClusterShutdown_getExistingKeysFromCache() {
        //verify the near cache data available while client is disconnected
        for (int i = 0; i < 100; i++) {
            assertEquals(map.get(i), i);
        }
    }

    @Test(expected = HazelcastClientOfflineException.class)
    public void testClusterShutdown_getNonExistingKeyFromCache() {
        //verify that if client ask for non available key, we get offline exception immediately
        map.get(200);
    }

    @Test(expected = HazelcastClientOfflineException.class)
    public void testClusterShutdown_putToMap() {
        //verify that if client tries to put a map with cache when disconnected, we get offline exception immediately
        map.put(1, 2);
    }

}
