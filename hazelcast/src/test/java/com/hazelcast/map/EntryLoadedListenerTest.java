/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.MapLoader;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryLoadedListener;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.core.EntryEventType.LOADED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@SuppressWarnings("deprecation")
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class EntryLoadedListenerTest extends HazelcastTestSupport {

    @Test
    public void get() {
        Config config = getConfig();
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(new ML());
        config.getMapConfig("default").setMapStoreConfig(mapStoreConfig);
        HazelcastInstance node = createHazelcastInstance(config);

        final AtomicReference<EntryEvent> eventReceived = new AtomicReference<EntryEvent>();
        IMap<Integer, Integer> map = node.getMap("test");
        map.addEntryListener(new EntryLoadedListener<Integer, Integer>() {
            @Override
            public void entryLoaded(EntryEvent<Integer, Integer> event) {
                eventReceived.set(event);
            }
        }, true);

        map.get(1);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                EntryEvent entryEvent = eventReceived.get();

                assertEquals(1, entryEvent.getKey());
                assertEquals(LOADED, entryEvent.getEventType());
            }
        });
    }

    @Test
    public void getAll() {
        Config config = getConfig();
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(new ML());
        config.getMapConfig("default").setMapStoreConfig(mapStoreConfig);
        HazelcastInstance node = createHazelcastInstance(config);


        final Queue<EntryEvent> entryEvents = new ConcurrentLinkedQueue<EntryEvent>();

        IMap<Integer, Integer> map = node.getMap("test");
        map.addEntryListener(new EntryLoadedListener<Integer, Integer>() {
            @Override
            public void entryLoaded(EntryEvent<Integer, Integer> event) {
                entryEvents.add(event);
            }
        }, true);


        final List<Integer> keyList = Arrays.asList(1, 2, 3, 4, 5);
        map.getAll(new HashSet<Integer>(keyList));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(keyList.size(), entryEvents.size());
                for (EntryEvent entryEvent : entryEvents) {
                    assertEquals(LOADED, entryEvent.getEventType());
                }
            }
        });
    }

    @Test
    public void ep_LOADED() {
        Config config = getConfig();
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(new ML());
        config.getMapConfig("default").setMapStoreConfig(mapStoreConfig);
        HazelcastInstance node = createHazelcastInstance(config);

        final AtomicReference<EntryEvent> eventReceived = new AtomicReference<EntryEvent>();
        IMap<Integer, Integer> map = node.getMap("test");
        map.addEntryListener(new EntryLoadedListener<Integer, Integer>() {
            @Override
            public void entryLoaded(EntryEvent<Integer, Integer> event) {
                eventReceived.set(event);
            }
        }, true);

        Object result = map.executeOnKey(1, new EntryProcessor() {
            @Override
            public Object process(Map.Entry entry) {
                entry.setValue(-1);
                return entry.getValue();
            }

            @Override
            public EntryBackupProcessor getBackupProcessor() {
                return null;
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                EntryEvent entryEvent = eventReceived.get();

                assertEquals(1, entryEvent.getKey());
                assertEquals(LOADED, entryEvent.getEventType());
            }
        });
    }

    @Test
    public void ep_ADDED() {
        Config config = getConfig();
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(new ML());
        config.getMapConfig("default").setMapStoreConfig(mapStoreConfig);
        HazelcastInstance node = createHazelcastInstance(config);

        final AtomicReference<EntryEvent> eventReceived = new AtomicReference<EntryEvent>();
        IMap<Integer, Integer> map = node.getMap("test");
        map.addEntryListener(new EntryAddedListener<Integer, Integer>() {
            @Override
            public void entryAdded(EntryEvent<Integer, Integer> event) {
                eventReceived.set(event);
            }
        }, true);

        Object result = map.executeOnKey(1, new EntryProcessor() {
            @Override
            public Object process(Map.Entry entry) {
                return entry.getValue();
            }

            @Override
            public EntryBackupProcessor getBackupProcessor() {
                return null;
            }
        });

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                assertNull(eventReceived.get());
            }
        }, 3);
    }

    class ML implements MapLoader<Integer, Integer> {

        AtomicInteger sequence = new AtomicInteger();

        @Override
        public Integer load(Integer key) {
            return sequence.incrementAndGet();
        }

        @Override
        public Map<Integer, Integer> loadAll(Collection<Integer> keys) {
            HashMap<Integer, Integer> map = new HashMap<Integer, Integer>();
            for (Integer key : keys) {
                map.put(key, sequence.incrementAndGet());
            }
            return map;
        }

        @Override
        public Iterable<Integer> loadAllKeys() {
            return Collections.emptyList();
        }
    }
}
