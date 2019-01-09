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

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.spi.properties.GroupProperty.MAP_LOAD_ALL_PUBLISHES_ADDED_EVENT;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("deprecation")
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class EntryLoadedListenerCompatibleModeTest extends HazelcastTestSupport {

    private static final TestHazelcastInstanceFactory FACTORY = new TestHazelcastInstanceFactory();

    private static HazelcastInstance node;

    @BeforeClass
    public static void setUp() {
        Config config = new Config();

        config.setProperty(MAP_LOAD_ALL_PUBLISHES_ADDED_EVENT.getName(), "true");
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER);
        mapStoreConfig.setClassName(EntryLoadedListenerTest.TestMapLoader.class.getName());
        config.getMapConfig("default").setMapStoreConfig(mapStoreConfig);

        FACTORY.newHazelcastInstance(config);
        FACTORY.newHazelcastInstance(config);
        node = FACTORY.newHazelcastInstance(config);
    }

    @AfterClass
    public static void tearDown() {
        FACTORY.shutdownAll();
    }

    @Test
    public void add_listener_notified_after_loadAll_when_backward_compatibility_mode_on() {
        final AtomicInteger addEventCount = new AtomicInteger();
        IMap<Integer, Integer> map = node.getMap("add_listener_notified_after_loadAll");
        map.clear();

        MapListener listener = new AddListener(addEventCount);
        map.addEntryListener(listener, true);

        map.loadAll(true);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(5, addEventCount.get());
            }
        }, 10);
    }

    static class AddListener implements EntryAddedListener<Integer, Integer> {

        private final AtomicInteger addEventCount;

        public AddListener(AtomicInteger addEventCount) {
            this.addEventCount = addEventCount;
        }

        @Override
        public void entryAdded(EntryEvent<Integer, Integer> event) {
            addEventCount.incrementAndGet();
        }
    }
}
