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

package com.hazelcast.cache.impl.journal;

import com.hazelcast.cache.CacheEventType;
import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.CacheProxy;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.cache.journal.EventJournalCacheEvent;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.journal.EventJournalInitialSubscriberState;
import com.hazelcast.projection.Projection;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.ringbuffer.impl.RingbufferContainer;
import com.hazelcast.ringbuffer.impl.RingbufferService;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.function.Predicate;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;
import java.io.Serializable;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class BasicCacheJournalTest extends HazelcastTestSupport {

    private static final Random RANDOM = new Random();
    private static final TruePredicate<EventJournalCacheEvent<String, Integer>> TRUE_PREDICATE = truePredicate();
    private static final IdentityProjection<EventJournalCacheEvent<String, Integer>> IDENTITY_PROJECTION = identityProjection();

    protected HazelcastInstance[] instances;
    protected CacheManager cacheManager;

    private int partitionId;

    @Before
    public void init() {
        instances = createInstances();
        partitionId = 1;
        cacheManager = createCacheManager();
        warmUpPartitions(instances);
    }

    protected CacheManager createCacheManager() {
        CachingProvider cachingProvider = HazelcastServerCachingProvider.createCachingProvider(getRandomInstance());
        return cachingProvider.getCacheManager();
    }

    protected HazelcastInstance getRandomInstance() {
        return instances[RANDOM.nextInt(instances.length)];
    }

    protected HazelcastInstance[] createInstances() {
        return createHazelcastInstanceFactory(2).newInstances(getConfig());
    }

    @Override
    protected Config getConfig() {
        int defaultPartitionCount = Integer.parseInt(GroupProperty.PARTITION_COUNT.getDefaultValue());
        EventJournalConfig eventJournalConfig = new EventJournalConfig()
                .setEnabled(true)
                .setCacheName("default")
                .setCapacity(200 * defaultPartitionCount);

        CacheSimpleConfig nonEvictingCache = new CacheSimpleConfig()
                .setName("cache");
        nonEvictingCache.getEvictionConfig()
                .setSize(Integer.MAX_VALUE);

        CacheSimpleConfig evictingCache = new CacheSimpleConfig()
                .setName("evicting");

        return super.getConfig()
                .addEventJournalConfig(eventJournalConfig)
                .addCacheConfig(nonEvictingCache)
                .addCacheConfig(evictingCache);
    }

    @Test
    public void unparkReadOperation() {
        final ICache<String, Integer> c = getCache();
        assertJournalSize(c, 0);

        final String key = randomPartitionKey();
        final Integer value = RANDOM.nextInt();
        final CountDownLatch latch = new CountDownLatch(1);

        final ExecutionCallback<ReadResultSet<EventJournalCacheEvent<String, Integer>>> ec
                = new ExecutionCallback<ReadResultSet<EventJournalCacheEvent<String, Integer>>>() {
            @Override
            public void onResponse(ReadResultSet<EventJournalCacheEvent<String, Integer>> response) {
                assertEquals(1, response.size());
                final EventJournalCacheEvent<String, Integer> e = response.get(0);

                assertEquals(CacheEventType.CREATED, e.getType());
                assertEquals(e.getKey(), key);
                assertEquals(e.getNewValue(), value);
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                t.printStackTrace();
            }
        };
        readFromEventJournal(c, 0, 100, partitionId, TRUE_PREDICATE, IDENTITY_PROJECTION).andThen(ec);
        readFromEventJournal(c, 0, 100, partitionId + 1, TRUE_PREDICATE, IDENTITY_PROJECTION).andThen(ec);
        readFromEventJournal(c, 0, 100, partitionId + 2, TRUE_PREDICATE, IDENTITY_PROJECTION).andThen(ec);
        readFromEventJournal(c, 0, 100, partitionId + 3, TRUE_PREDICATE, IDENTITY_PROJECTION).andThen(ec);
        readFromEventJournal(c, 0, 100, partitionId + 4, TRUE_PREDICATE, IDENTITY_PROJECTION).andThen(ec);

        c.put(key, value);
        assertOpenEventually(latch, 30);
        assertJournalSize(c, 1);
    }

    @Test
    public void receiveAddedEventsWhenPut() throws Exception {
        final ICache<String, Integer> c = getCache();

        final int count = 100;
        assertJournalSize(c, 0);

        for (int i = 0; i < count; i++) {
            c.put(randomPartitionKey(), i);
        }

        assertJournalSize(c, count);
        final ReadResultSet<EventJournalCacheEvent<String, Integer>> events = getAllEvents(c, null, null);
        assertEquals(count, events.size());

        final HashMap<String, Integer> received = new HashMap<String, Integer>();
        for (EventJournalCacheEvent<String, Integer> e : events) {
            assertEquals(CacheEventType.CREATED, e.getType());
            assertNull(e.getOldValue());
            received.put(e.getKey(), e.getNewValue());
        }

        assertEquals(getEntries(c), received.entrySet());
    }

    @Test
    public void receiveRemoveEventsWhenRemove() throws Exception {
        final ICache<String, Integer> c = getCache();

        final int count = 100;
        assertJournalSize(c, 0);
        final HashMap<String, Integer> initialMap = new HashMap<String, Integer>(count);
        for (int i = 0; i < count; i++) {
            final String k = randomPartitionKey();
            c.put(k, i);
            initialMap.put(k, i);
        }
        assertJournalSize(c, count);

        for (Map.Entry<String, Integer> e : getEntries(c)) {
            final String key = e.getKey();
            c.getAndRemove(key);
        }

        final HashMap<String, Integer> added = new HashMap<String, Integer>(count);
        final HashMap<String, Integer> removed = new HashMap<String, Integer>(count);

        for (EventJournalCacheEvent<String, Integer> e : getAllEvents(c, TRUE_PREDICATE, IDENTITY_PROJECTION)) {
            switch (e.getType()) {
                case CREATED:
                    added.put(e.getKey(), e.getNewValue());
                    break;
                case REMOVED:
                    removed.put(e.getKey(), e.getOldValue());
                    break;
            }
        }

        assertEquals(0, c.size());
        assertJournalSize(c, count * 2);
        assertEquals(initialMap, added);
        assertEquals(initialMap, removed);
    }

    @Test
    public void receiveUpdateEventsOnMapPut() throws Exception {
        final ICache<String, Integer> c = getCache();
        final int count = 100;
        final HashMap<String, Integer> initialMap = new HashMap<String, Integer>(count);
        assertJournalSize(c, 0);
        for (int i = 0; i < count; i++) {
            final String k = randomPartitionKey();
            c.put(k, i);
            initialMap.put(k, i);
        }
        assertJournalSize(c, count);

        for (Map.Entry<String, Integer> e : getEntries(c)) {
            final String key = e.getKey();
            final Integer newVal = initialMap.get(key) + 100;
            c.getAndPut(key, newVal);
        }

        assertJournalSize(c, count * 2);

        final HashMap<String, Integer> updatedFrom = new HashMap<String, Integer>(count);
        final HashMap<String, Integer> updatedTo = new HashMap<String, Integer>(count);

        for (EventJournalCacheEvent<String, Integer> e : getAllEvents(c, TRUE_PREDICATE, IDENTITY_PROJECTION)) {
            switch (e.getType()) {
                case UPDATED:
                    updatedFrom.put(e.getKey(), e.getOldValue());
                    updatedTo.put(e.getKey(), e.getNewValue());
                    break;
            }
        }

        assertEquals(initialMap, updatedFrom);
        assertEquals(getEntries(c), updatedTo.entrySet());
    }

    @Test
    public void testPredicates() throws Exception {
        final ICache<String, Integer> c = getCache();
        final int count = 50;
        assertJournalSize(c, 0);

        for (int i = 0; i < count; i++) {
            c.put(randomPartitionKey(), i);
        }
        assertJournalSize(c, count);

        final HashMap<String, Integer> evenMap = new HashMap<String, Integer>();
        final HashMap<String, Integer> oddMap = new HashMap<String, Integer>();

        for (EventJournalCacheEvent<String, Integer> e : getAllEvents(c, new NewValueParityPredicate(0), IDENTITY_PROJECTION)) {
            assertEquals(CacheEventType.CREATED, e.getType());
            evenMap.put(e.getKey(), e.getNewValue());
        }

        for (EventJournalCacheEvent<String, Integer> e : getAllEvents(c, new NewValueParityPredicate(1), IDENTITY_PROJECTION)) {
            assertEquals(CacheEventType.CREATED, e.getType());
            oddMap.put(e.getKey(), e.getNewValue());
        }

        assertEquals(count / 2, evenMap.size());
        assertEquals(count / 2, oddMap.size());

        for (Entry<String, Integer> e : evenMap.entrySet()) {
            final Integer v = e.getValue();
            assertTrue(v % 2 == 0);
            assertEquals(c.get(e.getKey()), v);
        }
        for (Entry<String, Integer> e : oddMap.entrySet()) {
            final Integer v = e.getValue();
            assertTrue(v % 2 == 1);
            assertEquals(c.get(e.getKey()), v);
        }
    }

    @Test
    public void testProjection() throws Exception {
        final ICache<String, Integer> c = getCache();
        final int count = 50;
        assertJournalSize(c, 0);
        for (int i = 0; i < count; i++) {
            c.put(randomPartitionKey(), i);
        }
        assertJournalSize(c, count);


        final ReadResultSet<Integer> resultSet = getAllEvents(c, TRUE_PREDICATE, new NewValueIncrementingProjection(100));
        final ArrayList<Integer> ints = new ArrayList<Integer>(count);
        for (Integer i : resultSet) {
            ints.add(i);
        }

        assertEquals(count, ints.size());
        for (Entry<String, Integer> e : getEntries(c)) {
            assertTrue(ints.contains(e.getValue() + 100));
        }
    }

    private <K, V> Set<Map.Entry<K, V>> getEntries(ICache<K, V> cache) {
        final Iterator<Cache.Entry<K, V>> it = cache.iterator();
        final HashSet<Entry<K, V>> entries = new HashSet<Map.Entry<K, V>>(cache.size());
        while (it.hasNext()) {
            final Cache.Entry<K, V> e = it.next();
            entries.add(new SimpleImmutableEntry<K, V>(e.getKey(), e.getValue()));
        }
        return entries;
    }

    private <T> ReadResultSet<T> getAllEvents(ICache<String, Integer> c,
                                              Predicate<? super EventJournalCacheEvent<String, Integer>> predicate,
                                              Projection<? super EventJournalCacheEvent<String, Integer>, T> projection)
            throws Exception {
        final EventJournalInitialSubscriberState state = subscribeToEventJournal(c, partitionId);
        return readFromEventJournal(
                c, state.getOldestSequence(), (int) (state.getNewestSequence() - state.getOldestSequence() + 1),
                partitionId, predicate, projection).get();
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

    private String randomPartitionKey() {
        return generateKeyForPartition(instances[0], partitionId);
    }

    private void assertJournalSize(ICache<?, ?> cache, int size) {
        assertJournalSize(partitionId, CacheService.getObjectNamespace(cache.getPrefixedName()), size);
    }

    private void assertJournalSize(int partitionId, ICache<?, ?> cache, int size) {
        assertJournalSize(partitionId, CacheService.getObjectNamespace(cache.getName()), size);
    }

    private void assertJournalSize(int partitionId, ObjectNamespace namespace, int size) {
        HazelcastInstance partitionOwner = null;
        for (HazelcastInstance instance : instances) {
            if (getNode(instance).partitionService.getPartition(partitionId).isLocal()) {
                partitionOwner = instance;
                break;
            }
        }

        final NodeEngineImpl nodeEngine = getNode(partitionOwner).nodeEngine;
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

    private <K, V> ICache<K, V> getCache() {
        return getCache("cache");
    }

    protected <K, V> ICache<K, V> getCache(String cacheName) {
        return (ICache<K, V>) cacheManager.getCache(cacheName);
    }

    protected <K, V> EventJournalInitialSubscriberState subscribeToEventJournal(Cache<K, V> cache, int partitionId) throws Exception {
        return ((CacheProxy<K, V>) cache).subscribeToEventJournal(partitionId).get();
    }

    protected <K, V, T> ICompletableFuture<ReadResultSet<T>> readFromEventJournal(
            Cache<K, V> cache,
            long startSequence,
            int maxSize,
            int partitionId,
            Predicate<? super EventJournalCacheEvent<K, V>> predicate,
            Projection<? super EventJournalCacheEvent<K, V>, T> projection) {
        return ((CacheProxy<K, V>) cache).readFromEventJournal(startSequence, 1, maxSize, partitionId, predicate, projection);
    }

    private static <T> TruePredicate<T> truePredicate() {
        return new TruePredicate<T>();
    }

    private static <T> IdentityProjection<T> identityProjection() {
        return new IdentityProjection<T>();
    }

    public static class NewValueParityPredicate implements Predicate<EventJournalCacheEvent<String, Integer>>, Serializable {
        private final int remainder;

        NewValueParityPredicate(int remainder) {
            this.remainder = remainder;
        }

        @Override
        public boolean test(EventJournalCacheEvent<String, Integer> e) {
            return e.getNewValue() % 2 == remainder;
        }
    }

    private static class NewValueIncrementingProjection extends Projection<EventJournalCacheEvent<String, Integer>, Integer> {
        private final int delta;

        NewValueIncrementingProjection(int delta) {
            this.delta = delta;
        }

        @Override
        public Integer transform(EventJournalCacheEvent<String, Integer> input) {
            return input.getNewValue() + delta;
        }
    }
}
