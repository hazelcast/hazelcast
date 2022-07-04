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

package com.hazelcast.journal;

import com.hazelcast.config.Config;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.journal.EventJournalInitialSubscriberState;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.internal.util.SetUtil;
import com.hazelcast.journal.EventJournalEventAdapter.EventType;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.ringbuffer.impl.RingbufferContainer;
import com.hazelcast.ringbuffer.impl.RingbufferService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.MapUtil.createHashMap;
import static com.hazelcast.journal.EventJournalEventAdapter.EventType.ADDED;
import static com.hazelcast.journal.EventJournalEventAdapter.EventType.EVICTED;
import static com.hazelcast.journal.EventJournalEventAdapter.EventType.LOADED;
import static com.hazelcast.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Base class for implementing data-structure specific basic event journal test.
 *
 * @param <EJ_TYPE> the type of the event journal event
 */
public abstract class AbstractEventJournalBasicTest<EJ_TYPE> extends HazelcastTestSupport {
    private static final Random RANDOM = new Random();

    protected HazelcastInstance[] instances;

    private int partitionId;
    private TruePredicate<EJ_TYPE> TRUE_PREDICATE = new TruePredicate<>();
    private Function<EJ_TYPE, EJ_TYPE> IDENTITY_FUNCTION = new IdentityFunction<>();

    @Before
    public void setUp() throws Exception {
        instances = createInstances();
        partitionId = 1;
        warmUpPartitions(instances);
    }

    @Override
    protected Config getConfig() {
        int defaultPartitionCount = Integer.parseInt(ClusterProperty.PARTITION_COUNT.getDefaultValue());
        EventJournalConfig eventJournalConfig = new EventJournalConfig()
                .setEnabled(true)
                .setCapacity(500 * defaultPartitionCount);
        Config config = super.getConfig();
        config.getMapConfig("default").setEventJournalConfig(eventJournalConfig);
        config.getCacheConfig("default").setEventJournalConfig(eventJournalConfig);

        return config;
    }

    /**
     * Tests that event journal read operations parked on different partitions
     * can be woken up independently.
     */
    @Test
    public void unparkReadOperation() {
        final EventJournalTestContext<String, Integer, EJ_TYPE> context = createContext();
        assertEventJournalSize(context.dataAdapter, 0);

        final String key = randomPartitionKey();
        final Integer value = RANDOM.nextInt();
        final CountDownLatch latch = new CountDownLatch(1);

        final BiConsumer<ReadResultSet<EJ_TYPE>, Throwable> ec = addEventExecutionCallback(context, key, value, latch);
        readFromEventJournal(context.dataAdapter, 0, 100, partitionId, TRUE_PREDICATE, IDENTITY_FUNCTION).whenCompleteAsync(ec);
        readFromEventJournal(context.dataAdapter, 0, 100, partitionId + 1, TRUE_PREDICATE, IDENTITY_FUNCTION)
                .whenCompleteAsync(ec);
        readFromEventJournal(context.dataAdapter, 0, 100, partitionId + 2, TRUE_PREDICATE, IDENTITY_FUNCTION)
                .whenCompleteAsync(ec);
        readFromEventJournal(context.dataAdapter, 0, 100, partitionId + 3, TRUE_PREDICATE, IDENTITY_FUNCTION)
                .whenCompleteAsync(ec);
        readFromEventJournal(context.dataAdapter, 0, 100, partitionId + 4, TRUE_PREDICATE, IDENTITY_FUNCTION)
                .whenCompleteAsync(ec);

        context.dataAdapter.put(key, value);
        assertOpenEventually(latch, 30);
        assertEventJournalSize(context.dataAdapter, 1);
    }

    @Test
    public void readManyFromEventJournalShouldNotBlock_whenHitsStale() {
        final EventJournalTestContext<String, Integer, EJ_TYPE> context = createContext();
        assertEventJournalSize(context.dataAdapter, 0);

        final CountDownLatch latch = new CountDownLatch(1);

        final Runnable ec = () -> {
            latch.countDown();
        };

        Thread consumer = new Thread(
                () -> readFromEventJournal(context.dataAdapter, 0, 10, partitionId, TRUE_PREDICATE, IDENTITY_FUNCTION)
                        .thenRun(ec));

        consumer.start();

        Map<String, Integer> addMap = new HashMap<String, Integer>();
        for (int i = 0; i < 501; i++) {
            addMap.put(randomPartitionKey(), RANDOM.nextInt());
        }

        context.dataAdapter.putAll(addMap);
        assertOpenEventually(latch, 30);
    }

    @Test
    public void receiveAddedEventsWhenPut() throws Exception {
        final EventJournalTestContext<String, Integer, EJ_TYPE> context = createContext();

        final int count = 100;
        assertEventJournalSize(context.dataAdapter, 0);

        for (int i = 0; i < count; i++) {
            context.dataAdapter.put(randomPartitionKey(), i);
        }

        assertEventJournalSize(context.dataAdapter, count);
        final ReadResultSet<EJ_TYPE> events = getAllEvents(context.dataAdapter, null, null);
        assertEquals(count, events.size());

        final HashMap<String, Integer> received = new HashMap<String, Integer>();
        final EventJournalEventAdapter<String, Integer, EJ_TYPE> journalAdapter = context.eventJournalAdapter;
        for (EJ_TYPE e : events) {
            assertEquals(ADDED, journalAdapter.getType(e));
            assertNull(journalAdapter.getOldValue(e));
            received.put(journalAdapter.getKey(e), journalAdapter.getNewValue(e));
        }

        assertEquals(context.dataAdapter.entrySet(), received.entrySet());
    }

    @Test
    public void receiveLoadedEventsWhenLoad() throws Exception {
        final EventJournalTestContext<String, String, EJ_TYPE> context = createContext();

        final int count = 100;
        assertEventJournalSize(context.dataAdapter, 0);

        for (int i = 0; i < count; i++) {
            context.dataAdapter.load(randomPartitionKey());
        }

        assertEventJournalSize(context.dataAdapter, count);
        final ReadResultSet<EJ_TYPE> events = getAllEvents(context.dataAdapter, null, null);
        assertEquals(count, events.size());

        final HashMap<String, String> received = new HashMap<String, String>();
        final EventJournalEventAdapter<String, String, EJ_TYPE> journalAdapter = context.eventJournalAdapter;
        for (EJ_TYPE e : events) {
            assertEquals(LOADED, journalAdapter.getType(e));
            assertNull(journalAdapter.getOldValue(e));
            received.put(journalAdapter.getKey(e), journalAdapter.getNewValue(e));
        }

        assertEquals(context.dataAdapter.entrySet(), received.entrySet());
    }

    @Test
    public void receiveLoadedEventsWhenLoadAll() throws Exception {
        final EventJournalTestContext<String, String, EJ_TYPE> context = createContext();

        final int count = 100;
        assertEventJournalSize(context.dataAdapter, 0);

        Set<String> keys = SetUtil.createHashSet(count);
        for (int i = 0; i < count; i++) {
            keys.add(randomPartitionKey());
        }
        context.dataAdapter.loadAll(keys);

        assertEventJournalSizeEventually(context, count);
        final ReadResultSet<EJ_TYPE> events = getAllEvents(context.dataAdapter, null, null);
        assertEquals(count, events.size());

        final HashMap<String, String> received = new HashMap<String, String>();
        final EventJournalEventAdapter<String, String, EJ_TYPE> journalAdapter = context.eventJournalAdapter;
        for (EJ_TYPE e : events) {
            assertEquals(LOADED, journalAdapter.getType(e));
            assertNull(journalAdapter.getOldValue(e));
            received.put(journalAdapter.getKey(e), journalAdapter.getNewValue(e));
        }

        assertEquals(context.dataAdapter.entrySet(), received.entrySet());
    }

    private void assertEventJournalSizeEventually(final EventJournalTestContext<String, String, EJ_TYPE> context,
                                                  final int count) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEventJournalSize(context.dataAdapter, count);
            }
        });
    }

    @Test
    public void receiveExpirationEventsWhenPutWithTtl() {
        final EventJournalTestContext<String, Integer, EJ_TYPE> context = createContext();
        final EventJournalDataStructureAdapter<String, Integer, EJ_TYPE> adapter = context.dataAdapter;
        testExpiration(context, adapter, new BiConsumer<String, Integer>() {
            @Override
            public void accept(String k, Integer v) {
                adapter.put(k, v, 1, TimeUnit.SECONDS);
            }
        });
    }

    @Test
    public void receiveExpirationEventsWhenPutOnExpiringStructure() {
        final EventJournalTestContext<String, Integer, EJ_TYPE> context = createContext();
        final EventJournalDataStructureAdapter<String, Integer, EJ_TYPE> adapter = context.dataAdapterWithExpiration;
        testExpiration(context, adapter, new BiConsumer<String, Integer>() {
            @Override
            public void accept(String k, Integer i) {
                adapter.put(k, i);
            }
        });
    }

    @Test
    public void receiveRemoveEventsWhenRemove() throws Exception {
        final EventJournalTestContext<String, Integer, EJ_TYPE> context = createContext();

        final int count = 100;
        assertEventJournalSize(context.dataAdapter, 0);
        final Map<String, Integer> initialMap = createHashMap(count);

        for (int v = 0; v < count; v++) {
            final String k = randomPartitionKey();
            context.dataAdapter.put(k, v);
            initialMap.put(k, v);
        }
        assertEventJournalSize(context.dataAdapter, count);

        for (Entry<String, Integer> e : context.dataAdapter.entrySet()) {
            context.dataAdapter.remove(e.getKey());
        }

        final HashMap<String, Integer> added = new HashMap<String, Integer>(count);
        final HashMap<String, Integer> removed = new HashMap<String, Integer>(count);

        final EventJournalEventAdapter<String, Integer, EJ_TYPE> journalAdapter = context.eventJournalAdapter;
        for (EJ_TYPE e : getAllEvents(context.dataAdapter, TRUE_PREDICATE, IDENTITY_FUNCTION)) {
            switch (journalAdapter.getType(e)) {
                case ADDED:
                    added.put(journalAdapter.getKey(e), journalAdapter.getNewValue(e));
                    break;
                case REMOVED:
                    removed.put(journalAdapter.getKey(e), journalAdapter.getOldValue(e));
                    break;
            }
        }

        assertEquals(0, context.dataAdapter.size());
        assertEventJournalSize(context.dataAdapter, count * 2);
        assertEquals(initialMap, added);
        assertEquals(initialMap, removed);
    }

    @Test
    public void receiveUpdateEventsOnMapPut() throws Exception {
        final EventJournalTestContext<String, Integer, EJ_TYPE> context = createContext();

        final int count = 100;
        assertEventJournalSize(context.dataAdapter, 0);
        final Map<String, Integer> initialMap = createHashMap(count);

        for (int v = 0; v < count; v++) {
            final String k = randomPartitionKey();
            context.dataAdapter.put(k, v);
            initialMap.put(k, v);
        }

        assertEventJournalSize(context.dataAdapter, count);

        for (Entry<String, Integer> e : context.dataAdapter.entrySet()) {
            final String key = e.getKey();
            final Integer newVal = initialMap.get(key) + 100;
            context.dataAdapter.put(key, newVal);
        }
        assertEventJournalSize(context.dataAdapter, count * 2);

        final Map<String, Integer> updatedFrom = createHashMap(count);
        final Map<String, Integer> updatedTo = createHashMap(count);

        final EventJournalEventAdapter<String, Integer, EJ_TYPE> journalAdapter = context.eventJournalAdapter;
        for (EJ_TYPE e : getAllEvents(context.dataAdapter, TRUE_PREDICATE, IDENTITY_FUNCTION)) {
            switch (journalAdapter.getType(e)) {
                case UPDATED:
                    updatedFrom.put(journalAdapter.getKey(e), journalAdapter.getOldValue(e));
                    updatedTo.put(journalAdapter.getKey(e), journalAdapter.getNewValue(e));
                    break;
            }
        }

        assertEquals(initialMap, updatedFrom);
        assertEquals(context.dataAdapter.entrySet(), updatedTo.entrySet());
    }

    @Test
    public void testPredicates() throws Exception {
        final EventJournalTestContext<String, Integer, EJ_TYPE> context = createContext();
        final int count = 50;
        assertEventJournalSize(context.dataAdapter, 0);

        for (int i = 0; i < count; i++) {
            context.dataAdapter.put(randomPartitionKey(), i);
        }
        assertEventJournalSize(context.dataAdapter, count);

        final Map<String, Integer> evenMap = createHashMap(count / 2);
        final Map<String, Integer> oddMap = createHashMap(count / 2);
        final EventJournalEventAdapter<String, Integer, EJ_TYPE> journalAdapter = context.eventJournalAdapter;
        final NewValueParityPredicate<EJ_TYPE> evenPredicate = new NewValueParityPredicate<EJ_TYPE>(0, journalAdapter);
        final NewValueParityPredicate<EJ_TYPE> oddPredicate = new NewValueParityPredicate<EJ_TYPE>(1, journalAdapter);

        for (EJ_TYPE e : getAllEvents(context.dataAdapter, evenPredicate, IDENTITY_FUNCTION)) {
            assertEquals(ADDED, journalAdapter.getType(e));
            evenMap.put(journalAdapter.getKey(e), journalAdapter.getNewValue(e));
        }

        for (EJ_TYPE e : getAllEvents(context.dataAdapter, oddPredicate, IDENTITY_FUNCTION)) {
            assertEquals(ADDED, journalAdapter.getType(e));
            oddMap.put(journalAdapter.getKey(e), journalAdapter.getNewValue(e));
        }

        assertEquals(count / 2, evenMap.size());
        assertEquals(count / 2, oddMap.size());

        for (Entry<String, Integer> e : evenMap.entrySet()) {
            final Integer v = e.getValue();
            assertTrue(v % 2 == 0);
            assertEquals(context.dataAdapter.get(e.getKey()), v);
        }
        for (Entry<String, Integer> e : oddMap.entrySet()) {
            final Integer v = e.getValue();
            assertTrue(v % 2 == 1);
            assertEquals(context.dataAdapter.get(e.getKey()), v);
        }
    }

    @Test
    public void testProjection() throws Exception {
        final EventJournalTestContext<String, Integer, EJ_TYPE> context = createContext();
        final int count = 50;
        assertEventJournalSize(context.dataAdapter, 0);
        for (int i = 0; i < count; i++) {
            context.dataAdapter.put(randomPartitionKey(), i);
        }
        assertEventJournalSize(context.dataAdapter, count);


        final ReadResultSet<Integer> resultSet = getAllEvents(context.dataAdapter, TRUE_PREDICATE,
                new NewValueIncrementingFunction<EJ_TYPE>(100, context.eventJournalAdapter));
        final ArrayList<Integer> ints = new ArrayList<Integer>(count);
        for (Integer i : resultSet) {
            ints.add(i);
        }

        assertEquals(count, ints.size());


        final Set<Entry<String, Integer>> entries = context.dataAdapter.entrySet();
        for (Entry<String, Integer> e : entries) {
            assertTrue(ints.contains(e.getValue() + 100));
        }
    }

    @Test
    public void skipEventsWhenFallenBehind() throws Exception {
        final EventJournalTestContext<String, Integer, EJ_TYPE> context = createContext();

        final int count = 1000;
        assertEventJournalSize(context.dataAdapter, 0);

        for (int i = 0; i < count; i++) {
            context.dataAdapter.put(randomPartitionKey(), i);
        }

        final EventJournalInitialSubscriberState state = subscribeToEventJournal(context.dataAdapter, partitionId);

        assertEquals(500, state.getOldestSequence());
        assertEquals(999, state.getNewestSequence());
        assertEventJournalSize(context.dataAdapter, 500);

        final int startSequence = 0;
        final ReadResultSet<EJ_TYPE> resultSet = readFromEventJournal(
                context.dataAdapter, startSequence, 1, partitionId, TRUE_PREDICATE, IDENTITY_FUNCTION)
                .toCompletableFuture()
                .get();

        assertEquals(1, resultSet.size());
        assertEquals(1, resultSet.readCount());
        assertNotEquals(startSequence + resultSet.readCount(), resultSet.getNextSequenceToReadFrom());
        assertEquals(501, resultSet.getNextSequenceToReadFrom());
        final long lostCount = resultSet.getNextSequenceToReadFrom() - resultSet.readCount() - startSequence;
        assertEquals(500, lostCount);
    }


    @Test
    public void nextSequenceProceedsWhenReadFromEventJournalWhileMinSizeIsZero() throws Exception {
        final EventJournalTestContext<String, Integer, EJ_TYPE> context = createContext();

        final int count = 1000;
        assertEventJournalSize(context.dataAdapter, 0);

        for (int i = 0; i < count; i++) {
            context.dataAdapter.put(randomPartitionKey(), i);
        }

        final EventJournalInitialSubscriberState state = subscribeToEventJournal(context.dataAdapter, partitionId);

        assertEquals(500, state.getOldestSequence());
        assertEquals(999, state.getNewestSequence());
        assertEventJournalSize(context.dataAdapter, 500);

        final int startSequence = 0;
        final ReadResultSet<EJ_TYPE> resultSet = readFromEventJournal(
                context.dataAdapter, startSequence, 1, 0, partitionId, TRUE_PREDICATE, IDENTITY_FUNCTION)
                .toCompletableFuture()
                .get();

        assertEquals(1, resultSet.size());
        assertEquals(1, resultSet.readCount());
        assertNotEquals(startSequence + resultSet.readCount(), resultSet.getNextSequenceToReadFrom());
        assertEquals(501, resultSet.getNextSequenceToReadFrom());
        final long lostCount = resultSet.getNextSequenceToReadFrom() - resultSet.readCount() - startSequence;
        assertEquals(500, lostCount);
    }

    @Test
    public void allowReadingWithFutureSeq() throws Exception {
        final EventJournalTestContext<String, Integer, EJ_TYPE> context = createContext();

        final EventJournalInitialSubscriberState state = subscribeToEventJournal(context.dataAdapter, partitionId);
        assertEquals(0, state.getOldestSequence());
        assertEquals(-1, state.getNewestSequence());
        assertEventJournalSize(context.dataAdapter, 0);

        final Integer value = RANDOM.nextInt();
        final CountDownLatch latch = new CountDownLatch(1);
        final int startSequence = 1;

        final BiConsumer<ReadResultSet<EJ_TYPE>, Throwable> callback = (response, t) -> {
            if (t == null) {
                latch.countDown();
                assertEquals(1, response.size());
                final EventJournalEventAdapter<String, Integer, EJ_TYPE> journalAdapter = context.eventJournalAdapter;
                final EJ_TYPE e = response.get(0);

                assertEquals(ADDED, journalAdapter.getType(e));
                assertEquals(value, journalAdapter.getNewValue(e));
            } else {
                rethrow(t);
            }
        };
        CompletionStage<ReadResultSet<EJ_TYPE>> callbackStage =
                readFromEventJournal(context.dataAdapter, startSequence, 1, partitionId, TRUE_PREDICATE, IDENTITY_FUNCTION)
                    .whenCompleteAsync(callback);

        assertTrueEventually(() -> {
            context.dataAdapter.put(randomPartitionKey(), value);
            assertTrue(latch.await(200, TimeUnit.MILLISECONDS));
        }, 30);

        // ensure no exception thrown from callback
        callbackStage.toCompletableFuture().join();
    }

    /**
     * Returns an execution callback for an event journal read operation. The
     * callback expects a single
     * {@link EventType#ADDED}
     * event for a provided {@code expectedKey} and with the provided
     * {@code expectedValue} as the new entry expectedValue.
     *
     * @param context       the data-structure specific context for the running test
     * @param expectedKey   the expected key
     * @param expectedValue the expected value
     * @param latch         the latch to open when the event has been received
     * @return an execution callback
     */
    private BiConsumer<ReadResultSet<EJ_TYPE>, Throwable> addEventExecutionCallback(
            final EventJournalTestContext<String, Integer, EJ_TYPE> context,
            final String expectedKey,
            final Integer expectedValue,
            final CountDownLatch latch) {
        return (response, throwable) -> {
            if (throwable == null) {
                assertEquals(1, response.size());
                final EventJournalEventAdapter<String, Integer, EJ_TYPE> journalAdapter = context.eventJournalAdapter;
                final EJ_TYPE e = response.get(0);

                assertEquals(ADDED, journalAdapter.getType(e));
                assertEquals(expectedKey, journalAdapter.getKey(e));
                assertEquals(expectedValue, journalAdapter.getNewValue(e));
                latch.countDown();
            } else {
                throwable.printStackTrace();
            }
        };
    }

    /**
     * Tests behaviour of event journal on entry expiration
     *
     * @param context    the data-structure specific context for the running test
     * @param adapter    the adapter for a specific data structure currently
     *                   being tested
     * @param mutationFn the function to mutate the data structure
     */
    private void testExpiration(EventJournalTestContext<String, Integer, EJ_TYPE> context,
                                final EventJournalDataStructureAdapter<String, Integer, EJ_TYPE> adapter,
                                BiConsumer<String, Integer> mutationFn) {
        final EventJournalEventAdapter<String, Integer, EJ_TYPE> journalAdapter = context.eventJournalAdapter;
        final int count = 2;
        assertEventJournalSize(adapter, 0);

        for (int i = 0; i < count; i++) {
            final String k = randomPartitionKey();
            mutationFn.accept(k, i);
        }

        assertTrueEventually(() -> {
            assertEventJournalSize(partitionId, adapter, count * 2);
            final ReadResultSet<EJ_TYPE> set = getAllEvents(adapter, null, null);
            assertEquals(count * 2, set.size());
            final HashMap<String, Integer> added = new HashMap<>();
            final HashMap<String, Integer> evicted = new HashMap<>();
            for (EJ_TYPE e : set) {
                if (ADDED.equals(journalAdapter.getType(e))) {
                    added.put(journalAdapter.getKey(e), journalAdapter.getNewValue(e));
                } else if (EVICTED.equals(journalAdapter.getType(e))) {
                    evicted.put(journalAdapter.getKey(e), journalAdapter.getOldValue(e));
                }
            }
            assertEquals(added, evicted);
        });
    }

    /**
     * Reads from the event journal a set of events.
     *
     * @param adapter     the adapter for a specific data structure
     * @param predicate   the predicate which the events must pass to be included in the response.
     *                    May be {@code null} in which case all events pass the predicate
     * @param projection  the projection which is applied to the events before returning.
     *                    May be {@code null} in which case the event is returned without being
     *                    projected
     * @param <PROJ_TYPE> the return type of the projection. It is equal to the journal event type
     *                    if the projection is {@code null} or it is the identity projection
     * @return the filtered and projected journal items
     * @throws Exception if any exception occurred while reading the events
     */
    private <PROJ_TYPE> ReadResultSet<PROJ_TYPE> getAllEvents(EventJournalDataStructureAdapter<?, ?, EJ_TYPE> adapter,
                                                              Predicate<EJ_TYPE> predicate,
                                                              Function<EJ_TYPE, PROJ_TYPE> projection)
            throws Exception {
        final EventJournalInitialSubscriberState state = subscribeToEventJournal(adapter, partitionId);
        return readFromEventJournal(
                adapter, state.getOldestSequence(),
                (int) (state.getNewestSequence() - state.getOldestSequence() + 1),
                partitionId, predicate, projection).toCompletableFuture().get();
    }

    /**
     * Returns a random key belonging to the partition with ID {@link #partitionId}.
     */
    private String randomPartitionKey() {
        return generateKeyForPartition(instances[0], partitionId);
    }

    /**
     * Asserts that the number of event journal entries for the partition
     * {@link #partitionId} is equal to the given {@code size}.
     *
     * @param adapter the adapter for a specific data structure
     * @param size    the expected count for the partition event journal events
     */
    private void assertEventJournalSize(EventJournalDataStructureAdapter<?, ?, EJ_TYPE> adapter, int size) {
        assertEventJournalSize(partitionId, adapter, size);
    }

    /**
     * Asserts that the number of event journal entries for the partition
     * {@code partitionId} is equal to the given {@code size}.
     *
     * @param partitionId the partition ID for the event journal entries
     * @param adapter     the adapter for a specific data structure
     * @param size        the expected count for the partition event journal events
     */
    private void assertEventJournalSize(
            int partitionId,
            EventJournalDataStructureAdapter<?, ?, EJ_TYPE> adapter, int size) {
        final ObjectNamespace namespace = adapter.getNamespace();
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

    /**
     * Returns a random hazelcast instance
     */
    protected HazelcastInstance getRandomInstance() {
        return instances[RANDOM.nextInt(instances.length)];
    }

    /**
     * Creates hazelcast instances used to run the tests.
     *
     * @return the array of hazelcast instances
     */
    protected HazelcastInstance[] createInstances() {
        return createHazelcastInstanceFactory(2).newInstances(getConfig());
    }

    /**
     * Subscribe to the event journal for this reader and a specific partition ID.
     *
     * @param adapter     the adapter for a specific data structure
     * @param partitionId the partition to read events from
     * @return the state for the event journal subscription
     * @throws Exception if any error occurred while subscribing to the event journal
     */
    private EventJournalInitialSubscriberState subscribeToEventJournal(
            EventJournalDataStructureAdapter<?, ?, EJ_TYPE> adapter,
            int partitionId) throws Exception {
        return adapter.subscribeToEventJournal(partitionId).toCompletableFuture().get();
    }

    /**
     * Reads from the event journal a set of events.
     *
     * @param adapter       the adapter for a specific data structure
     * @param startSequence the sequence of the first item to read
     * @param maxSize       the maximum number of items to read
     * @param partitionId   the partition ID of the entries in the journal
     * @param predicate     the predicate which the events must pass to be included in the response.
     *                      May be {@code null} in which case all events pass the predicate
     * @param projection    the projection which is applied to the events before returning.
     *                      May be {@code null} in which case the event is returned without being
     *                      projected
     * @param <K>           the data structure entry key type
     * @param <V>the        data structure entry value type
     * @param <PROJ_TYPE>   the return type of the projection. It is equal to the journal event type
     *                      if the projection is {@code null} or it is the identity projection
     * @return the future with the filtered and projected journal items
     */
    private <K, V, PROJ_TYPE> CompletionStage<ReadResultSet<PROJ_TYPE>> readFromEventJournal(
            EventJournalDataStructureAdapter<K, V, EJ_TYPE> adapter,
            long startSequence,
            int maxSize,
            int partitionId,
            Predicate<EJ_TYPE> predicate,
            Function<EJ_TYPE, PROJ_TYPE> projection) {
        return readFromEventJournal(adapter, startSequence, maxSize, 1, partitionId, predicate, projection);
    }

    /**
     * Reads from the event journal a set of events.
     *
     * @param adapter       the adapter for a specific data structure
     * @param startSequence the sequence of the first item to read
     * @param maxSize       the maximum number of items to read
     * @param minSize       the minimum number of items to read
     * @param partitionId   the partition ID of the entries in the journal
     * @param predicate     the predicate which the events must pass to be included in the response.
     *                      May be {@code null} in which case all events pass the predicate
     * @param projection    the projection which is applied to the events before returning.
     *                      May be {@code null} in which case the event is returned without being
     *                      projected
     * @param <K>           the data structure entry key type
     * @param <V>the        data structure entry value type
     * @param <PROJ_TYPE>   the return type of the projection. It is equal to the journal event type
     *                      if the projection is {@code null} or it is the identity projection
     * @return the future with the filtered and projected journal items
     */
    private <K, V, PROJ_TYPE> CompletionStage<ReadResultSet<PROJ_TYPE>> readFromEventJournal(
            EventJournalDataStructureAdapter<K, V, EJ_TYPE> adapter,
            long startSequence,
            int maxSize,
            int minSize,
            int partitionId,
            Predicate<EJ_TYPE> predicate,
            Function<EJ_TYPE, PROJ_TYPE> projection) {
        return adapter.readFromEventJournal(startSequence, minSize, maxSize, partitionId, predicate, projection);
    }

    /**
     * Creates the data structure specific {@link EventJournalTestContext} used
     * by the event journal tests.
     *
     * @param <K> key type of the created {@link EventJournalTestContext}
     * @param <V> value type of the created {@link EventJournalTestContext}
     * @return a {@link EventJournalTestContext} used by the event journal tests
     */
    protected abstract <K, V> EventJournalTestContext<K, V, EJ_TYPE> createContext();
}
