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

package com.hazelcast.client.replicatedmap;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MapEvent;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class ClientReplicatedMapListenerTest extends HazelcastTestSupport {

    TestHazelcastFactory factory = new TestHazelcastFactory();

    @After
    public void tearDown() throws Exception {
        factory.terminateAll();
    }

    @Test
    public void testEntryAdded() throws Exception {
        HazelcastInstance instance = factory.newHazelcastInstance();
        HazelcastInstance client = factory.newHazelcastClient();
        String mapName = randomMapName();
        ReplicatedMap<Object, Object> replicatedMap = client.getReplicatedMap(mapName);
        final EventCountingListener listener = new EventCountingListener();
        replicatedMap.addEntryListener(listener);
        replicatedMap.put(1, 1);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                Assert.assertEquals(1, listener.addCount.get());
            }
        });
    }

    @Test
    public void testEntryUpdated() throws Exception {
        HazelcastInstance instance = factory.newHazelcastInstance();
        HazelcastInstance client = factory.newHazelcastClient();
        String mapName = randomMapName();
        ReplicatedMap<Object, Object> replicatedMap = client.getReplicatedMap(mapName);
        final EventCountingListener listener = new EventCountingListener();
        replicatedMap.addEntryListener(listener);
        replicatedMap.put(1, 1);
        replicatedMap.put(1, 2);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                Assert.assertEquals(1, listener.updateCount.get());
            }
        });
    }

    @Test
    public void testEntryRemoved() throws Exception {
        HazelcastInstance instance = factory.newHazelcastInstance();
        HazelcastInstance client = factory.newHazelcastClient();
        String mapName = randomMapName();
        ReplicatedMap<Object, Object> replicatedMap = client.getReplicatedMap(mapName);
        final EventCountingListener listener = new EventCountingListener();
        replicatedMap.addEntryListener(listener);
        replicatedMap.put(1, 1);
        replicatedMap.remove(1);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                Assert.assertEquals(1, listener.removeCount.get());
            }
        });
    }

    @Test
    public void testMapClear() throws Exception {
        HazelcastInstance instance = factory.newHazelcastInstance();
        HazelcastInstance client = factory.newHazelcastClient();
        String mapName = randomMapName();
        ReplicatedMap<Object, Object> replicatedMap = client.getReplicatedMap(mapName);
        final EventCountingListener listener = new EventCountingListener();
        replicatedMap.addEntryListener(listener);
        replicatedMap.put(1, 1);
        replicatedMap.clear();
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                Assert.assertEquals(1, listener.mapClearCount.get());
            }
        });
    }

    public class EventCountingListener implements EntryListener<Object, Object> {

        public final AtomicLong addCount = new AtomicLong();
        public final AtomicLong removeCount = new AtomicLong();
        public final AtomicLong updateCount = new AtomicLong();
        public final AtomicLong evictCount = new AtomicLong();
        public final AtomicLong mapClearCount = new AtomicLong();
        public final AtomicLong mapEvictCount = new AtomicLong();

        public EventCountingListener() {
        }

        @Override
        public void entryAdded(EntryEvent<Object, Object> objectObjectEntryEvent) {
            addCount.incrementAndGet();
        }

        @Override
        public void entryRemoved(EntryEvent<Object, Object> objectObjectEntryEvent) {
            removeCount.incrementAndGet();
        }

        @Override
        public void entryUpdated(EntryEvent<Object, Object> objectObjectEntryEvent) {
            updateCount.incrementAndGet();
        }

        @Override
        public void entryEvicted(EntryEvent<Object, Object> objectObjectEntryEvent) {
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
