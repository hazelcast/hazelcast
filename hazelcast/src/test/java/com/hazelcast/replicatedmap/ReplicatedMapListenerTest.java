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

package com.hazelcast.replicatedmap;

import com.hazelcast.config.Config;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MapEvent;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.query.impl.FalsePredicate;
import com.hazelcast.query.impl.predicates.InstanceOfPredicate;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class ReplicatedMapListenerTest extends HazelcastTestSupport {

    @Test
    public void testRegisterListenerViaConfiguration() throws Exception {
        String mapName = randomMapName();
        Config config = new Config();
        ReplicatedMapConfig replicatedMapConfig = config.getReplicatedMapConfig(mapName);
        EntryListenerConfig listenerConfig = new EntryListenerConfig();
        final EventCountingListener listener = new EventCountingListener();
        listenerConfig.setImplementation(listener);
        replicatedMapConfig.addEntryListenerConfig(listenerConfig);
        HazelcastInstance instance = createHazelcastInstance(config);
        ReplicatedMap<Object, Object> replicatedMap = instance.getReplicatedMap(mapName);
        replicatedMap.put(3, 3);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(1, listener.addCount.get());
                assertEquals(3, listener.keys.peek());
            }
        }, 10);
    }

    @Test
    public void testEntryAdded() throws Exception {
        ReplicatedMap<Object, Object> replicatedMap = createClusterAndGetRandomReplicatedMap();
        final EventCountingListener listener = new EventCountingListener();
        replicatedMap.addEntryListener(listener);
        replicatedMap.put(1, 1);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(1, listener.addCount.get());
            }
        });
    }

    @Test
    public void testEntryUpdated() throws Exception {
        ReplicatedMap<Object, Object> replicatedMap = createClusterAndGetRandomReplicatedMap();
        final EventCountingListener listener = new EventCountingListener();
        replicatedMap.addEntryListener(listener);
        replicatedMap.put(1, 1);
        replicatedMap.put(1, 2);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(1, listener.updateCount.get());
            }
        });
    }

    @Test
    public void testEntryEvicted() throws Exception {
        ReplicatedMap<Object, Object> replicatedMap = createClusterAndGetRandomReplicatedMap();
        final EventCountingListener listener = new EventCountingListener();
        replicatedMap.addEntryListener(listener);
        replicatedMap.put(1, 1, 1, TimeUnit.SECONDS);
        sleepAtLeastSeconds(2);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(1, listener.evictCount.get());
            }
        });
    }

    @Test
    public void testEntryRemoved() throws Exception {
        ReplicatedMap<Object, Object> replicatedMap = createClusterAndGetRandomReplicatedMap();
        final EventCountingListener listener = new EventCountingListener();
        replicatedMap.addEntryListener(listener);
        replicatedMap.put(1, 1);
        replicatedMap.remove(1);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(1, listener.removeCount.get());
            }
        });
    }

    @Test
    public void testMapClear() throws Exception {
        ReplicatedMap<Object, Object> replicatedMap = createClusterAndGetRandomReplicatedMap();
        final EventCountingListener listener = new EventCountingListener();
        replicatedMap.addEntryListener(listener);
        replicatedMap.put(1, 1);
        replicatedMap.clear();
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(1, listener.mapClearCount.get());
            }
        });
    }


    @Test
    public void testListenToKeyForEntryAdded() throws Exception {
        ReplicatedMap<Object, Object> replicatedMap = createClusterAndGetRandomReplicatedMap();
        final EventCountingListener listener = new EventCountingListener();
        replicatedMap.addEntryListener(listener, 1);
        replicatedMap.put(1, 1);
        replicatedMap.put(2, 2);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(1, listener.keys.size());
                assertEquals(1, listener.keys.peek());
                assertEquals(1, listener.addCount.get());
            }
        });
    }

    @Test
    public void testListenWithPredicate() throws Exception {
        ReplicatedMap<Object, Object> replicatedMap = createClusterAndGetRandomReplicatedMap();
        final EventCountingListener listener = new EventCountingListener();
        replicatedMap.addEntryListener(listener, FalsePredicate.INSTANCE);
        replicatedMap.put(2, 2);
        assertTrueFiveSeconds(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(0, listener.addCount.get());
            }
        });
    }

    @Test
    public void testListenToKeyWithPredicate() throws Exception {
        ReplicatedMap<Object, Object> replicatedMap = createClusterAndGetRandomReplicatedMap();
        final EventCountingListener listener = new EventCountingListener();
        replicatedMap.addEntryListener(listener, new InstanceOfPredicate(Integer.class), 2);
        replicatedMap.put(1, 1);
        replicatedMap.put(2, 2);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(1, listener.keys.size());
                assertEquals(2, listener.keys.peek());
                assertEquals(1, listener.addCount.get());
            }
        });
    }

    private ReplicatedMap<Object, Object> createClusterAndGetRandomReplicatedMap() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance h1 = factory.newHazelcastInstance();
        HazelcastInstance h2 = factory.newHazelcastInstance();
        String mapName = randomMapName();
        return h1.getReplicatedMap(mapName);
    }


    public class EventCountingListener implements EntryListener<Object, Object> {

        public final ConcurrentLinkedQueue<Object> keys = new ConcurrentLinkedQueue<Object>();
        public final AtomicLong addCount = new AtomicLong();
        public final AtomicLong removeCount = new AtomicLong();
        public final AtomicLong updateCount = new AtomicLong();
        public final AtomicLong evictCount = new AtomicLong();
        public final AtomicLong mapClearCount = new AtomicLong();
        public final AtomicLong mapEvictCount = new AtomicLong();

        public EventCountingListener() {
        }

        @Override
        public void entryAdded(EntryEvent<Object, Object> event) {
            keys.add(event.getKey());
            addCount.incrementAndGet();
        }

        @Override
        public void entryRemoved(EntryEvent<Object, Object> event) {
            keys.add(event.getKey());
            removeCount.incrementAndGet();
        }

        @Override
        public void entryUpdated(EntryEvent<Object, Object> event) {
            keys.add(event.getKey());
            updateCount.incrementAndGet();
        }

        @Override
        public void entryEvicted(EntryEvent<Object, Object> event) {
            keys.add(event.getKey());
            evictCount.incrementAndGet();
        }

        @Override
        public void mapEvicted(MapEvent event) {
            mapEvictCount.incrementAndGet();
        }

        @Override
        public void mapCleared(MapEvent event) {
            mapClearCount.incrementAndGet();
        }

        @Override
        public String toString() {
            return "EventCountingListener{" +
                    "addCount=" + addCount +
                    ", removeCount=" + removeCount +
                    ", updateCount=" + updateCount +
                    ", evictCount=" + evictCount +
                    ", mapClearCount=" + mapClearCount +
                    ", mapEvictCount=" + mapEvictCount +
                    '}';
        }
    }

}
