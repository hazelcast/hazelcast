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

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryLoadedListener;
import com.hazelcast.map.listener.EntryUpdatedListener;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
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

import static com.hazelcast.core.EntryEventType.LOADED;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EntryLoadedListenerTest extends HazelcastTestSupport {

    private static final TestHazelcastInstanceFactory FACTORY = new TestHazelcastInstanceFactory();

    private static HazelcastInstance node;

    @BeforeClass
    public static void setUp() {
        Config config = new Config();

        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER);
        mapStoreConfig.setClassName(TestMapLoader.class.getName());
        config.getMapConfig("default").setMapStoreConfig(mapStoreConfig);

        MapStoreConfig noInitialLoading = new MapStoreConfig();
        noInitialLoading.setEnabled(true);
        noInitialLoading.setClassName(TestMapLoaderWithoutInitialLoad.class.getName());
        config.getMapConfig("noInitialLoading*").setMapStoreConfig(noInitialLoading);

        FACTORY.newHazelcastInstance(config);
        FACTORY.newHazelcastInstance(config);
        node = FACTORY.newHazelcastInstance(config);
    }

    @AfterClass
    public static void tearDown() {
        FACTORY.shutdownAll();
    }

    @Test
    public void load_listener_notified_when_containsKey_loads_from_map_loader() {
        final AtomicInteger loadEventCount = new AtomicInteger();
        IMap<Integer, Integer> map = node.getMap("noInitialLoading_test_containsKey");
        map.addEntryListener(new EntryLoadedListener<Integer, Integer>() {
            @Override
            public void entryLoaded(EntryEvent<Integer, Integer> event) {
                loadEventCount.incrementAndGet();
            }
        }, true);

        map.containsKey(1);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(1, loadEventCount.get());
            }
        });
    }

    @Test
    public void load_listener_notified_when_putIfAbsent_loads_from_map_loader() {
        final AtomicInteger loadEventCount = new AtomicInteger();
        IMap<Integer, Integer> map = node.getMap("noInitialLoading_test_putIfAbsent");
        map.addEntryListener(new EntryLoadedListener<Integer, Integer>() {
            @Override
            public void entryLoaded(EntryEvent<Integer, Integer> event) {
                loadEventCount.incrementAndGet();
            }
        }, true);

        map.putIfAbsent(1, 100);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(1, loadEventCount.get());
            }
        });
    }

    @Test
    public void load_listener_notified_when_get_loads_from_map_loader() {
        final AtomicInteger loadEventCount = new AtomicInteger();
        IMap<Integer, Integer> map = node.getMap("noInitialLoading_test_get");
        map.addEntryListener(new EntryLoadedListener<Integer, Integer>() {
            @Override
            public void entryLoaded(EntryEvent<Integer, Integer> event) {
                loadEventCount.incrementAndGet();
            }
        }, true);

        map.get(1);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(1, loadEventCount.get());
            }
        });
    }

    @Test
    public void load_listener_notified_when_get_after_evict() {
        final AtomicInteger loadEventCount = new AtomicInteger();
        IMap<Integer, Integer> map = node.getMap("noInitialLoading_load_listener_notified_when_get_after_evict");
        map.addEntryListener(new EntryLoadedListener<Integer, Integer>() {
            @Override
            public void entryLoaded(EntryEvent<Integer, Integer> event) {
                loadEventCount.incrementAndGet();
            }
        }, true);

        map.put(1, 1);
        map.evict(1);
        map.get(1);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(1, loadEventCount.get());
            }
        }, 5);
    }

    @Test
    public void load_listener_notified_when_getAll_loads_from_map_loader() {
        final Queue<EntryEvent> entryEvents = new ConcurrentLinkedQueue<EntryEvent>();

        IMap<Integer, Integer> map = node.getMap("noInitialLoading_test_getAll");
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
            public void run() {
                assertEquals(keyList.size(), entryEvents.size());
                for (EntryEvent entryEvent : entryEvents) {
                    assertEquals(LOADED, entryEvent.getEventType());
                }
            }
        });
    }

    @Test
    public void load_listener_notified_when_read_only_entry_processor_loads_from_map_loader() {
        final AtomicInteger loadEventCount = new AtomicInteger();
        IMap<Integer, Integer> map = node.getMap("noInitialLoading_test_read_only_ep");

        map.addEntryListener(new EntryLoadedListener<Integer, Integer>() {
            @Override
            public void entryLoaded(EntryEvent<Integer, Integer> event) {
                loadEventCount.incrementAndGet();
            }
        }, true);

        map.executeOnKey(1, new Reader());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(1, loadEventCount.get());
            }
        });
    }

    @Test
    public void add_listener_not_notified_when_read_only_entry_processor_loads_from_map_loader() {
        final AtomicInteger addEventCount = new AtomicInteger();
        IMap<Integer, Integer> map = node.getMap("noInitialLoading_test_read_only_ep_not_notified");
        map.addEntryListener(new EntryAddedListener<Integer, Integer>() {
            @Override
            public void entryAdded(EntryEvent<Integer, Integer> event) {
                addEventCount.incrementAndGet();
            }
        }, true);

        map.executeOnKey(1, new Reader());

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                assertEquals(0, addEventCount.get());
            }
        }, 3);
    }

    @Test
    public void load_and_update_listener_notified_when_updater_entry_processor_loads_from_map_loader() {
        final AtomicInteger loadEventCount = new AtomicInteger();
        final AtomicInteger updateEventCount = new AtomicInteger();
        IMap<Integer, Integer> map = node.getMap("noInitialLoading_test_updater_ep");
        map.addEntryListener(new LoadAndUpdateListener(loadEventCount, updateEventCount), true);

        for (int i = 0; i < 10; i++) {
            map.executeOnKey(i, new Updater());
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(10, loadEventCount.get());
                assertEquals(10, updateEventCount.get());
            }
        });
    }

    @Test
    public void load_listener_notified_but_add_listener_not_notified_after_loadAll() {
        final AtomicInteger loadEventCount = new AtomicInteger();
        final AtomicInteger addEventCount = new AtomicInteger();

        IMap<Integer, Integer> map = node.getMap("load_listener_notified_but_add_listener_not_notified_after_loadAll");
        map.clear();

        MapListener listener = new LoadAndAddListener(loadEventCount, addEventCount);
        map.addEntryListener(listener, true);

        map.loadAll(true);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(5, loadEventCount.get());
            }
        });
        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                assertEquals(0, addEventCount.get());
            }
        }, 3);
    }

    @Test
    public void add_listener_not_notified_after_loadAll() {
        final AtomicInteger addEventCount = new AtomicInteger();

        IMap<Integer, Integer> map = node.getMap("add_listener_notified_after_loadAll");
        map.clear();

        MapListener listener = new AddListener(addEventCount);
        map.addEntryListener(listener, true);

        map.loadAll(true);

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                assertEquals(0, addEventCount.get());
            }
        }, 5);
    }

    static class LoadAndAddListener implements EntryLoadedListener<Integer, Integer>,
            EntryAddedListener<Integer, Integer> {

        private final AtomicInteger loadEventCount;
        private final AtomicInteger addEventCount;

        LoadAndAddListener(AtomicInteger loadEventCount, AtomicInteger addEventCount) {
            this.loadEventCount = loadEventCount;
            this.addEventCount = addEventCount;
        }

        @Override
        public void entryLoaded(EntryEvent<Integer, Integer> event) {
            loadEventCount.incrementAndGet();
        }

        @Override
        public void entryAdded(EntryEvent<Integer, Integer> event) {
            addEventCount.incrementAndGet();
        }
    }

    static class LoadAndUpdateListener implements EntryLoadedListener<Integer, Integer>,
            EntryUpdatedListener<Integer, Integer> {

        private final AtomicInteger loadEventCount;
        private final AtomicInteger updateEventCount;

        LoadAndUpdateListener(AtomicInteger loadEventCount, AtomicInteger updateEventCount) {
            this.loadEventCount = loadEventCount;
            this.updateEventCount = updateEventCount;
        }

        @Override
        public void entryLoaded(EntryEvent<Integer, Integer> event) {
            loadEventCount.incrementAndGet();
        }

        @Override
        public void entryUpdated(EntryEvent<Integer, Integer> event) {
            updateEventCount.incrementAndGet();
        }
    }

    static class AddListener implements EntryAddedListener<Integer, Integer> {

        private final AtomicInteger addEventCount;

        AddListener(AtomicInteger addEventCount) {
            this.addEventCount = addEventCount;
        }

        @Override
        public void entryAdded(EntryEvent<Integer, Integer> event) {
            addEventCount.incrementAndGet();
        }
    }

    public static class TestMapLoader implements MapLoader<Integer, Integer> {

        AtomicInteger sequence = new AtomicInteger();

        public TestMapLoader() {
        }

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
            return Arrays.asList(1, 2, 3, 4, 5);
        }
    }

    public static class TestMapLoaderWithoutInitialLoad extends TestMapLoader {

        public TestMapLoaderWithoutInitialLoad() {
        }

        @Override
        public Iterable<Integer> loadAllKeys() {
            return Collections.emptyList();
        }
    }

    public static class Updater implements EntryProcessor<Integer, Integer, Integer> {
        @Override
        public Integer process(Map.Entry<Integer, Integer> entry) {
            entry.setValue(entry.getValue() + 1);
            return entry.getValue();
        }
    }

    public static class Reader implements EntryProcessor<Integer, Integer, Integer> {
        @Override
        public Integer process(Map.Entry<Integer, Integer> entry) {
            return entry.getValue();
        }
    }
}
