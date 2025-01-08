/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.spi;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapStoreAdapter;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientHazelcastRunningInForkJoinPoolTest extends HazelcastTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    private HazelcastInstance client;
    private HazelcastInstance server;
    private IMap serverMap;
    private IMap clientMap;
    private String slowLoadingMapName = "slowLoadingMap";
    private String defaultMapName = "default";
    private CountDownLatch loadCompletedLatch = new CountDownLatch(1);

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Before
    public void setup() {
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(new MapStoreAdapter() {
            @Override
            public Object load(Object key) {
                sleepSeconds(1000);
                loadCompletedLatch.countDown();
                return super.load(key);
            }
        });

        MapConfig mapConfig = new MapConfig(slowLoadingMapName);
        mapConfig.setMapStoreConfig(mapStoreConfig);

        Config config = smallInstanceConfigWithoutJetAndMetrics().addMapConfig(mapConfig);

        server = hazelcastFactory.newHazelcastInstance(config);
        client = hazelcastFactory.newHazelcastClient();

        serverMap = server.getMap(defaultMapName);
        clientMap = client.getMap(slowLoadingMapName);
    }

    @Test
    public void slow_data_loading_does_not_block_entry_listener_addition() throws ExecutionException, InterruptedException {
        // 1. Simulate some parallel tasks contending on loading 1 item from a database.
        int count = 2 * Runtime.getRuntime().availableProcessors();
        CountDownLatch tasksSubmitted = new CountDownLatch(count);
        for (int i = 0; i < count; i++) {
            ForkJoinPool.commonPool().submit(() -> {
                tasksSubmitted.countDown();
                clientMap.get(1);
            });
        }

        // Wait until all our tasks have been submitted
        tasksSubmitted.await();

        // 2. In parallel, outside FJP, adding a listener to a different map must not be affected by loading at step 1.
        CountDownLatch listenerLatch = new CountDownLatch(1);
        EntryAdapter<Object, Object> adapter = new EntryAdapter<>() {
            @Override
            public void entryAdded(EntryEvent<Object, Object> event) {
                listenerLatch.countDown();
            }
        };
        spawn(() -> serverMap.addEntryListener(adapter, true)).get();

        // Validate that client map loading is still blocked
        assertTrue(loadCompletedLatch.getCount() > 0);

        // Validate that our entry listener was registered successfully
        serverMap.put("key", "value");
        listenerLatch.await();
    }
}
