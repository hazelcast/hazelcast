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

package com.hazelcast.client.impl.spi;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapStoreAdapter;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientHazelcastRunningInForkJoinPoolTest extends HazelcastTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    private HazelcastInstance client;
    private HazelcastInstance server;
    private IMap serverMap;
    private IMap clientMap;
    private String slowLoadingMapName = "slowLoadingMap";
    private String defaultMapName = "default";

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
                return super.load(key);
            }
        });

        MapConfig mapConfig = new MapConfig(slowLoadingMapName);
        mapConfig.setMapStoreConfig(mapStoreConfig);

        Config config = getConfig().addMapConfig(mapConfig);

        server = hazelcastFactory.newHazelcastInstance(config);
        client = hazelcastFactory.newHazelcastClient();

        serverMap = server.getMap(defaultMapName);
        clientMap = client.getMap(slowLoadingMapName);
    }

    @Test
    public void slow_data_loading_does_not_block_entry_listener_addition() {
        // 1. Let's simulate 1000 parallel tasks
        // contend on loading 1 item from a database.
        Collection<Future> tasks = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            tasks.add(ForkJoinPool.commonPool().submit(() -> clientMap.get(1)));
        }

        // 2. In parallel, adding a listener to a different
        // map must not affected by loading phase at step 1.
        serverMap.addEntryListener(new EntryAdapter<>(), true);
    }
}
