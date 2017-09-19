/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.journal;

import com.hazelcast.config.Config;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.Node;
import com.hazelcast.journal.EventJournalInitialSubscriberState;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.map.journal.EventJournalMapEvent;
import com.hazelcast.projection.Projection;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.ringbuffer.impl.RingbufferContainer;
import com.hazelcast.ringbuffer.impl.RingbufferService;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.function.BiConsumer;
import com.hazelcast.util.function.Predicate;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class BasicMapJournalTest extends HazelcastTestSupport {

    private static final Random RANDOM = new Random();
    private static final TruePredicate<EventJournalMapEvent<String, Integer>> TRUE_PREDICATE = truePredicate();
    private static final IdentityProjection<EventJournalMapEvent<String, Integer>> IDENTITY_PROJECTION = identityProjection();

    protected HazelcastInstance[] instances;

    private int partitionId;

    @Before
    public void init() {
        instances = createInstances();
        partitionId = 1;
        warmUpPartitions(instances);
    }

    protected HazelcastInstance getRandomInstance() {
        return instances[RANDOM.nextInt(instances.length)];
    }

    protected HazelcastInstance[] createInstances() {
        return createHazelcastInstanceFactory(2).newInstances(getConfig());
    }

    protected <K, V> IMap<K, V> getMap(String mapName) {
        return getRandomInstance().getMap(mapName);
    }

    protected <K, V> EventJournalInitialSubscriberState subscribeToEventJournal(IMap<K, V> map, int partitionId) throws Exception {
        return ((MapProxyImpl<K, V>) map).subscribeToEventJournal(partitionId).get();
    }

    protected <K, V, T> ICompletableFuture<ReadResultSet<T>> readFromEventJournal(
            IMap<K, V> map,
            long startSequence,
            int maxSize,
            int partitionId,
            Predicate<? super EventJournalMapEvent<K, V>> predicate,
            Projection<? super EventJournalMapEvent<K, V>, T> projection) {
        return ((MapProxyImpl<K, V>) map).readFromEventJournal(startSequence, 1, maxSize, partitionId, predicate, projection);
    }

    @Override
    protected Config getConfig() {
        int defaultPartitionCount = Integer.parseInt(GroupProperty.PARTITION_COUNT.getDefaultValue());
        EventJournalConfig eventJournalConfig = new EventJournalConfig()
                .setEnabled(true)
                .setMapName("default")
                .setCapacity(500 * defaultPartitionCount);

        MapConfig mapConfig = new MapConfig("mappy");

        MapConfig expiringMap = new MapConfig("expiring")
                .setTimeToLiveSeconds(1);

        return super.getConfig()
                .addEventJournalConfig(eventJournalConfig)
                .addMapConfig(mapConfig)
                .addMapConfig(expiringMap);
    }

    @Test
    public void unparkReadOperation() {
        final IMap<String, Integer> m = getMap();
        assertJournalSize(m, 0);

        final String key = randomPartitionKey();
        final Integer value = RANDOM.nextInt();
        final CountDownLatch latch = new CountDownLatch(1);

        final ExecutionCallback<ReadResultSet<EventJournalMapEvent<String, Integer>>> ec
                = new ExecutionCallback<ReadResultSet<EventJournalMapEvent<String, Integer>>>() {
            @Override
            public void onResponse(ReadResultSet<EventJournalMapEvent<String, Integer>> response) {
                assertEquals(1, response.size());
                final EventJournalMapEvent<String, Integer> e = response.get(0);

                assertEquals(EntryEventType.ADDED, e.getType());
                assertEquals(e.getKey(), key);
                assertEquals(e.getNewValue(), value);
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                t.printStackTrace();
            }
        };
        readFromEventJournal(m, 0, 100, partitionId, TRUE_PREDICATE, IDENTITY_PROJECTION).andThen(ec);
        readFromEventJournal(m, 0, 100, partitionId + 1, TRUE_PREDICATE, IDENTITY_PROJECTION).andThen(ec);
        readFromEventJournal(m, 0, 100, partitionId + 2, TRUE_PREDICATE, IDENTITY_PROJECTION).andThen(ec);
        readFromEventJournal(m, 0, 100, partitionId + 3, TRUE_PREDICATE, IDENTITY_PROJECTION).andThen(ec);
        readFromEventJournal(m, 0, 100, partitionId + 4, TRUE_PREDICATE, IDENTITY_PROJECTION).andThen(ec);

        m.put(key, value);
        assertOpenEventually(latch, 30);
        assertJournalSize(m, 1);
    }

    @Test
    public void receiveAddedEventsWhenPut() throws Exception {
        final IMap<String, Integer> m = getMap();

        final int count = 100;
        assertJournalSize(m, 0);

        for (int i = 0; i < count; i++) {
            m.put(randomPartitionKey(), i);
        }

        assertJournalSize(m, count);
        final ReadResultSet<EventJournalMapEvent<String, Integer>> events = getAllEvents(m, null, null);
        assertEquals(count, events.size());

        final HashMap<String, Integer> received = new HashMap<String, Integer>();
        for (EventJournalMapEvent<String, Integer> e : events) {
            assertEquals(EntryEventType.ADDED, e.getType());
            assertNull(e.getOldValue());
            received.put(e.getKey(), e.getNewValue());
        }

        assertEquals(m.entrySet(), received.entrySet());
    }

    @Test
    public void receiveExpirationEventsWhenPutWithTtl() {
        final String mapName = "mappy";
        final IMap<String, Integer> m = getMap(mapName);
        testExpiration(mapName, new BiConsumer<String, Integer>() {
            @Override
            public void accept(String k, Integer i) {
                m.put(k, i, 1, TimeUnit.SECONDS);
            }
        });
    }

    @Test
    public void receiveExpirationEventsWhenPutOnExpiringMap() {
        final String mapName = "expiring";
        final IMap<String, Integer> m = getMap(mapName);
        testExpiration(mapName, new BiConsumer<String, Integer>() {
            @Override
            public void accept(String k, Integer i) {
                m.put(k, i);
            }
        });
    }

    @Test
    public void receiveRemoveEventsWhenRemove() throws Exception {
        final IMap<String, Integer> m = getMap();

        final int count = 100;
        assertJournalSize(m, 0);
        for (int i = 0; i < count; i++) {
            m.put(randomPartitionKey(), i);
        }
        assertJournalSize(m, count);

        final HashMap<String, Integer> initialMap = new HashMap<String, Integer>(m);
        for (String key : m.keySet()) {
            m.remove(key);
        }

        final HashMap<String, Integer> added = new HashMap<String, Integer>(count);
        final HashMap<String, Integer> removed = new HashMap<String, Integer>(count);

        for (EventJournalMapEvent<String, Integer> e : getAllEvents(m, TRUE_PREDICATE, IDENTITY_PROJECTION)) {
            switch (e.getType()) {
                case ADDED:
                    added.put(e.getKey(), e.getNewValue());
                    break;
                case REMOVED:
                    removed.put(e.getKey(), e.getOldValue());
                    break;
            }
        }

        assertEquals(0, m.size());
        assertJournalSize(m, count * 2);
        assertEquals(initialMap, added);
        assertEquals(initialMap, removed);
    }

    @Test
    public void receiveUpdateEventsOnMapPut() throws Exception {
        final IMap<String, Integer> m = getMap();
        final int count = 100;
        assertJournalSize(m, 0);
        for (int i = 0; i < count; i++) {
            m.put(randomPartitionKey(), i);
        }
        final HashMap<String, Integer> initialMap = new HashMap<String, Integer>(m);
        assertJournalSize(m, count);

        for (String key : m.keySet()) {
            final Integer newVal = initialMap.get(key) + 100;
            m.put(key, newVal);
        }
        assertJournalSize(m, count * 2);

        final HashMap<String, Integer> updatedFrom = new HashMap<String, Integer>(count);
        final HashMap<String, Integer> updatedTo = new HashMap<String, Integer>(count);

        for (EventJournalMapEvent<String, Integer> e : getAllEvents(m, TRUE_PREDICATE, IDENTITY_PROJECTION)) {
            switch (e.getType()) {
                case UPDATED:
                    updatedFrom.put(e.getKey(), e.getOldValue());
                    updatedTo.put(e.getKey(), e.getNewValue());
                    break;
            }
        }

        assertEquals(initialMap, updatedFrom);
        assertEquals(m.entrySet(), updatedTo.entrySet());
    }

    @Test
    public void testPredicates() throws Exception {
        final IMap<String, Integer> m = getMap();
        final int count = 50;
        assertJournalSize(m, 0);

        for (int i = 0; i < count; i++) {
            m.put(randomPartitionKey(), i);
        }
        assertJournalSize(m, count);

        final HashMap<String, Integer> evenMap = new HashMap<String, Integer>();
        final HashMap<String, Integer> oddMap = new HashMap<String, Integer>();

        for (EventJournalMapEvent<String, Integer> e : getAllEvents(m, new NewValueParityPredicate(0), IDENTITY_PROJECTION)) {
            assertEquals(EntryEventType.ADDED, e.getType());
            evenMap.put(e.getKey(), e.getNewValue());
        }

        for (EventJournalMapEvent<String, Integer> e : getAllEvents(m, new NewValueParityPredicate(1), IDENTITY_PROJECTION)) {
            assertEquals(EntryEventType.ADDED, e.getType());
            oddMap.put(e.getKey(), e.getNewValue());
        }

        assertEquals(count / 2, evenMap.size());
        assertEquals(count / 2, oddMap.size());

        for (Entry<String, Integer> e : evenMap.entrySet()) {
            final Integer v = e.getValue();
            assertTrue(v % 2 == 0);
            assertEquals(m.get(e.getKey()), v);
        }
        for (Entry<String, Integer> e : oddMap.entrySet()) {
            final Integer v = e.getValue();
            assertTrue(v % 2 == 1);
            assertEquals(m.get(e.getKey()), v);
        }
    }

    @Test
    public void testProjection() throws Exception {
        final IMap<String, Integer> m = getMap();
        final int count = 50;
        assertJournalSize(m, 0);
        for (int i = 0; i < count; i++) {
            m.put(randomPartitionKey(), i);
        }
        assertJournalSize(m, count);


        final ReadResultSet<Integer> ints = getAllEvents(m, TRUE_PREDICATE, new NewValueIncrementingProjection(100));
        assertEquals(count, ints.size());
        final Collection<Integer> values = m.values();
        for (Integer v : ints) {
            assertTrue(values.contains(v - 100));
        }
    }

    private void testExpiration(String mapName, BiConsumer<String, Integer> mutationFn) {
        final IMap<String, Integer> m = getMap(mapName);
        final int count = 2;
        assertJournalSize(m, 0);

        for (int i = 0; i < count; i++) {
            final String k = randomPartitionKey();
            mutationFn.accept(k, i);
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertJournalSize(partitionId, m, count * 2);
                final ReadResultSet<EventJournalMapEvent<String, Integer>> set = getAllEvents(m, null, null);
                assertEquals(count * 2, set.size());
                final HashMap<String, Integer> added = new HashMap<String, Integer>();
                final HashMap<String, Integer> evicted = new HashMap<String, Integer>();
                for (EventJournalMapEvent<String, Integer> e : set) {
                    if (EntryEventType.ADDED.equals(e.getType())) {
                        added.put(e.getKey(), e.getNewValue());
                    } else if (EntryEventType.EVICTED.equals(e.getType())) {
                        evicted.put(e.getKey(), e.getOldValue());
                    }
                }
                assertEquals(added, evicted);
            }
        });
    }

    private <T> ReadResultSet<T> getAllEvents(IMap<String, Integer> m,
                                              Predicate<? super EventJournalMapEvent<String, Integer>> predicate,
                                              Projection<? super EventJournalMapEvent<String, Integer>, T> projection)
            throws Exception {
        final EventJournalInitialSubscriberState state = subscribeToEventJournal(m, partitionId);
        return readFromEventJournal(
                m, state.getOldestSequence(), (int) (state.getNewestSequence() - state.getOldestSequence() + 1),
                partitionId, predicate, projection).get();
    }

    private String randomPartitionKey() {
        return generateKeyForPartition(instances[0], partitionId);
    }

    private void assertJournalSize(DistributedObject object, int size) {
        assertJournalSize(partitionId, MapService.getObjectNamespace(object.getName()), size);
    }

    private void assertJournalSize(int partitionId, DistributedObject object, int size) {
        assertJournalSize(partitionId, MapService.getObjectNamespace(object.getName()), size);
    }

    private void assertJournalSize(int partitionId, ObjectNamespace namespace, int size) {
        HazelcastInstance partitionOwner = null;
        for (HazelcastInstance instance : instances) {
            if (getNode(instance).partitionService.getPartition(partitionId).isLocal()) {
                partitionOwner = instance;
                break;
            }
        }

        final Node node = getNode(partitionOwner);
        final NodeEngineImpl nodeEngine = node.nodeEngine;
        final RingbufferService rbService = nodeEngine.getService(RingbufferService.SERVICE_NAME);
        final ConcurrentMap<Integer, Map<ObjectNamespace, RingbufferContainer>> containers = rbService.getContainers();
        final Map<ObjectNamespace, RingbufferContainer> partitionContainers = containers.get(partitionId);
        if (size == 0 && partitionContainers == null) {
            return;
        }
        assertNotNull(partitionContainers);
        final RingbufferContainer container = partitionContainers.get(namespace);
        if (size == 0 && container == null) {
            return;
        }
        assertNotNull(container);
        assertEquals(size, container.size());
    }

    private <K, V> IMap<K, V> getMap() {
        return getMap("mappy");
    }

    private static <T> TruePredicate<T> truePredicate() {
        return new TruePredicate<T>();
    }

    private static <T> IdentityProjection<T> identityProjection() {
        return new IdentityProjection<T>();
    }

    private static class NewValueIncrementingProjection extends Projection<EventJournalMapEvent<String, Integer>, Integer> {
        private final int delta;

        NewValueIncrementingProjection(int delta) {
            this.delta = delta;
        }

        @Override
        public Integer transform(EventJournalMapEvent<String, Integer> input) {
            return input.getNewValue() + delta;
        }
    }

    public static class NewValueParityPredicate implements Predicate<EventJournalMapEvent<String, Integer>>, Serializable {
        private final int remainder;

        NewValueParityPredicate(int remainder) {
            this.remainder = remainder;
        }

        @Override
        public boolean test(EventJournalMapEvent<String, Integer> e) {
            return e.getNewValue() % 2 == remainder;
        }
    }

    private static class TruePredicate<T> implements Predicate<T>, Serializable {
        @Override
        public boolean test(T t) {
            return true;
        }
    }

    private static class IdentityProjection<I> extends Projection<I, I> implements Serializable {
        @Override
        public I transform(I input) {
            return input;
        }
    }
}
