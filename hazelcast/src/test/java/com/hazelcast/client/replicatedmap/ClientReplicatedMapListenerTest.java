/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.replicatedmap;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.MapEvent;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.Predicates;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class ClientReplicatedMapListenerTest extends HazelcastTestSupport {

    private TestHazelcastFactory factory = new TestHazelcastFactory();

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Test
    public void testEntryAdded() {
        ReplicatedMap<Object, Object> replicatedMap = createClusterAndGetRandomReplicatedMap();
        final EventCountingListener listener = new EventCountingListener();
        replicatedMap.addEntryListener(listener);
        replicatedMap.put(1, 1);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(1, listener.addCount.get());
            }
        });
    }

    @Test
    public void testEntryUpdated() {
        ReplicatedMap<Object, Object> replicatedMap = createClusterAndGetRandomReplicatedMap();
        final EventCountingListener listener = new EventCountingListener();
        replicatedMap.addEntryListener(listener);
        replicatedMap.put(1, 1);
        replicatedMap.put(1, 2);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(1, listener.updateCount.get());
            }
        });
    }

    @Test
    public void testEntryRemoved() {
        ReplicatedMap<Object, Object> replicatedMap = createClusterAndGetRandomReplicatedMap();
        final EventCountingListener listener = new EventCountingListener();
        replicatedMap.addEntryListener(listener);
        replicatedMap.put(1, 1);
        replicatedMap.remove(1);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(1, listener.removeCount.get());
            }
        });
    }

    @Test
    public void testMapClear() {
        ReplicatedMap<Object, Object> replicatedMap = createClusterAndGetRandomReplicatedMap();
        final EventCountingListener listener = new EventCountingListener();
        replicatedMap.addEntryListener(listener);
        replicatedMap.put(1, 1);
        replicatedMap.clear();
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(1, listener.mapClearCount.get());
            }
        });
    }

    @Test
    public void testListenToKeyForEntryAdded() {
        ReplicatedMap<Object, Object> replicatedMap = createClusterAndGetRandomReplicatedMap();
        final EventCountingListener listener = new EventCountingListener();
        replicatedMap.addEntryListener(listener, 1);
        replicatedMap.put(1, 1);
        replicatedMap.put(2, 2);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(1, listener.keys.size());
                assertEquals(1, listener.keys.peek());
                assertEquals(1, listener.addCount.get());
            }
        });
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testListenWithPredicate() {
        ReplicatedMap<Object, Object> replicatedMap = createClusterAndGetRandomReplicatedMap();
        final EventCountingListener listener = new EventCountingListener();
        replicatedMap.addEntryListener(listener, Predicates.alwaysFalse());
        replicatedMap.put(2, 2);
        assertTrueFiveSeconds(new AssertTask() {
            @Override
            public void run() {
                assertEquals(0, listener.addCount.get());
            }
        });
    }

    @Test
    public void no_key_value_deserialization_on_server_when_in_memory_format_is_binary() {
        final CountDownLatch eventReceivedLatch = new CountDownLatch(1);

        Config config = new Config();
        config.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.BINARY);

        HazelcastInstance server = factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient();

        ReplicatedMap<DeserializationCounter, DeserializationCounter> replicatedMap = client.getReplicatedMap("test");
        replicatedMap.addEntryListener(new EntryAdapter<DeserializationCounter, DeserializationCounter>() {
            @Override
            public void onEntryEvent(EntryEvent<DeserializationCounter, DeserializationCounter> event) {
                eventReceivedLatch.countDown();
            }
        }, Predicates.alwaysTrue());

        DeserializationCounter key = new DeserializationCounter();
        DeserializationCounter value = new DeserializationCounter();

        replicatedMap.put(key, value);

        // wait to get event on client side
        assertOpenEventually(eventReceivedLatch);

        assertEquals(0, key.DESERIALIZATION_COUNT.get());
        assertEquals(0, value.DESERIALIZATION_COUNT.get());
    }

    public static class DeserializationCounter implements DataSerializable {

        protected static final AtomicInteger DESERIALIZATION_COUNT = new AtomicInteger();

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {

        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            DESERIALIZATION_COUNT.incrementAndGet();
        }
    }

    private <K, V> ReplicatedMap<K, V> createClusterAndGetRandomReplicatedMap() {
        factory.newHazelcastInstance();
        HazelcastInstance client = factory.newHazelcastClient();
        String mapName = randomMapName();
        return client.getReplicatedMap(mapName);
    }

    public class EventCountingListener implements EntryListener<Object, Object> {

        final ConcurrentLinkedQueue<Object> keys = new ConcurrentLinkedQueue<Object>();
        final AtomicLong addCount = new AtomicLong();
        final AtomicLong removeCount = new AtomicLong();
        final AtomicLong updateCount = new AtomicLong();
        final AtomicLong evictCount = new AtomicLong();
        final AtomicLong expiryCount = new AtomicLong();
        final AtomicLong mapClearCount = new AtomicLong();
        final AtomicLong mapEvictCount = new AtomicLong();

        EventCountingListener() {
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
        public void entryExpired(EntryEvent<Object, Object> event) {
            throw new UnsupportedOperationException("Expired event is not published by replicated map");
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
            return "EventCountingListener{"
                    + "addCount=" + addCount
                    + ", removeCount=" + removeCount
                    + ", updateCount=" + updateCount
                    + ", evictCount=" + evictCount
                    + ", mapClearCount=" + mapClearCount
                    + ", mapEvictCount=" + mapEvictCount
                    + '}';
        }
    }
}
