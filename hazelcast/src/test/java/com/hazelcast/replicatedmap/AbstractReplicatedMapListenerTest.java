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

package com.hazelcast.replicatedmap;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.map.MapEvent;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;

public abstract class AbstractReplicatedMapListenerTest extends HazelcastTestSupport {

    protected abstract <K, V> ReplicatedMap<K, V> createClusterAndGetRandomReplicatedMap();

    @Test
    public void testEntryAdded() {
        ReplicatedMap<Integer, Integer> replicatedMap = createClusterAndGetRandomReplicatedMap();
        final EventCountingListener<Integer, Integer> listener = new EventCountingListener<>();
        replicatedMap.addEntryListener(listener);
        replicatedMap.put(1, 1);
        assertTrueEventually(() -> assertEquals(1, listener.addCount.get()));
    }

    @Test
    public void testEntryUpdated() {
        ReplicatedMap<Integer, Integer> replicatedMap = createClusterAndGetRandomReplicatedMap();
        final EventCountingListener<Integer, Integer> listener = new EventCountingListener<>();
        replicatedMap.addEntryListener(listener);
        replicatedMap.put(1, 1);
        replicatedMap.put(1, 2);
        assertTrueEventually(() -> assertEquals(1, listener.updateCount.get()));
    }

    @Test
    public void testEntryEvicted() {
        ReplicatedMap<Integer, Integer> replicatedMap = createClusterAndGetRandomReplicatedMap();
        final EventCountingListener<Integer, Integer> listener = new EventCountingListener<>();
        replicatedMap.addEntryListener(listener);
        replicatedMap.put(1, 1, 1, TimeUnit.SECONDS);
        sleepAtLeastSeconds(2);
        assertTrueEventually(() -> assertEquals(1, listener.evictCount.get()));
    }

    @Test
    public void testEntryRemoved() {
        ReplicatedMap<Integer, Integer> replicatedMap = createClusterAndGetRandomReplicatedMap();
        final EventCountingListener<Integer, Integer> listener = new EventCountingListener<>();
        replicatedMap.addEntryListener(listener);
        replicatedMap.put(1, 1);
        replicatedMap.remove(1);
        assertTrueEventually(() -> assertEquals(1, listener.removeCount.get()));
    }

    @Test
    public void testMapClear() {
        ReplicatedMap<Integer, Integer> replicatedMap = createClusterAndGetRandomReplicatedMap();
        final EventCountingListener<Integer, Integer> listener = new EventCountingListener<>();
        replicatedMap.addEntryListener(listener);
        replicatedMap.put(1, 1);
        replicatedMap.clear();
        assertTrueEventually(() -> assertEquals(1, listener.mapClearCount.get()));
    }

    @Test
    public void testListenToKeyForEntryAdded() {
        ReplicatedMap<Integer, Integer> replicatedMap = createClusterAndGetRandomReplicatedMap();
        final EventCountingListener<Integer, Integer> listener = new EventCountingListener<>();
        replicatedMap.addEntryListener(listener, 1);
        replicatedMap.put(1, 1);
        replicatedMap.put(2, 2);

        assertTrueEventually(() -> {
            assertEquals(1, listener.keys.size());
            assertEquals(Integer.valueOf(1), listener.keys.peek());
            assertEquals(1, listener.addCount.get());
        });
    }

    @Test
    public void testListenWithPredicate() {
        ReplicatedMap<Integer, Integer> replicatedMap = createClusterAndGetRandomReplicatedMap();
        final EventCountingListener<Integer, Integer> listener = new EventCountingListener<>();
        replicatedMap.addEntryListener(listener, Predicates.alwaysFalse());
        replicatedMap.put(2, 2);
        assertTrueFiveSeconds(() -> assertEquals(0, listener.addCount.get()));
    }

    @Test
    public void testListenToKeyWithPredicate() {
        ReplicatedMap<Integer, Integer> replicatedMap = createClusterAndGetRandomReplicatedMap();
        final EventCountingListener<Integer, Integer> listener = new EventCountingListener<>();
        replicatedMap.addEntryListener(listener, Predicates.instanceOf(Integer.class), 2);
        replicatedMap.put(1, 1);
        replicatedMap.put(2, 2);
        assertTrueEventually(() -> {
            assertEquals(1, listener.keys.size());
            assertEquals(Integer.valueOf(2), listener.keys.peek());
            assertEquals(1, listener.addCount.get());
        });
    }

    @Test
    public void testListenWithPredicateWithAttributePath() {
        ReplicatedMap<Integer, HazelcastJsonValue> replicatedMap = createClusterAndGetRandomReplicatedMap();
        EventCountingListener<Integer, HazelcastJsonValue> listener = new EventCountingListener<>();
        replicatedMap.addEntryListener(listener, Predicates.equal("a", "foo"));
        replicatedMap.put(1, new HazelcastJsonValue("{\"a\": \"notFoo\"}"));
        replicatedMap.put(2, new HazelcastJsonValue("{\"a\": \"foo\"}"));

        assertTrueEventually(() -> assertEquals(1, listener.addCount.get()));
    }

    public static class EventCountingListener<K, V> implements EntryListener<K, V> {

        protected final ConcurrentLinkedQueue<K> keys = new ConcurrentLinkedQueue<>();
        protected final AtomicLong addCount = new AtomicLong();
        protected final AtomicLong removeCount = new AtomicLong();
        protected final AtomicLong updateCount = new AtomicLong();
        protected final AtomicLong evictCount = new AtomicLong();
        protected final AtomicLong mapClearCount = new AtomicLong();
        protected final AtomicLong mapEvictCount = new AtomicLong();

        public EventCountingListener() {
        }

        @Override
        public void entryAdded(EntryEvent<K, V> event) {
            keys.add(event.getKey());
            addCount.incrementAndGet();
        }

        @Override
        public void entryRemoved(EntryEvent<K, V> event) {
            keys.add(event.getKey());
            removeCount.incrementAndGet();
        }

        @Override
        public void entryUpdated(EntryEvent<K, V> event) {
            keys.add(event.getKey());
            updateCount.incrementAndGet();
        }

        @Override
        public void entryEvicted(EntryEvent<K, V> event) {
            keys.add(event.getKey());
            evictCount.incrementAndGet();
        }

        @Override
        public void entryExpired(EntryEvent<K, V> event) {
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
