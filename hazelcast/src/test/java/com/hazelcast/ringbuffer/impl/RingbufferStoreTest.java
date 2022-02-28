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

package com.hazelcast.ringbuffer.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.RingbufferStoreConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.ringbuffer.RingbufferStore;
import com.hazelcast.ringbuffer.RingbufferStoreFactory;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.ringbuffer.OverflowPolicy;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.internal.util.ConcurrencyUtil;
import com.hazelcast.internal.util.ConstructorFunction;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.config.RingbufferConfig.DEFAULT_CAPACITY;
import static com.hazelcast.instance.impl.TestUtil.terminateInstance;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RingbufferStoreTest extends HazelcastTestSupport {

    private static Config getConfig(String ringbufferName,
                                    int capacity,
                                    InMemoryFormat inMemoryFormat,
                                    RingbufferStoreConfig ringbufferStoreConfig) {
        final Config config = new Config();
        final RingbufferConfig rbConfig = config
                .getRingbufferConfig(ringbufferName)
                .setInMemoryFormat(inMemoryFormat)
                .setCapacity(capacity);
        rbConfig.setRingbufferStoreConfig(ringbufferStoreConfig);
        return config;
    }

    @Test
    public void testRingbufferStore() throws Exception {
        final int numItems = 2000;

        final TestRingbufferStore<Integer> rbStore = new TestRingbufferStore<Integer>(2000, 0, 2000);
        final RingbufferStoreConfig rbStoreConfig = new RingbufferStoreConfig()
                .setEnabled(true)
                .setStoreImplementation(rbStore);
        final Config config = getConfig("testRingbufferStore", DEFAULT_CAPACITY, OBJECT, rbStoreConfig);
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);

        // add items to the ring buffer and the store and shut down
        final HazelcastInstance instance = factory.newHazelcastInstance(config);
        final Ringbuffer<Object> ringbuffer = instance.getRingbuffer("testRingbufferStore");
        for (int i = 0; i < numItems; i++) {
            ringbuffer.add(i);
        }
        instance.shutdown();

        // now get a new ring buffer and read the items from the store
        final HazelcastInstance instance2 = factory.newHazelcastInstance(config);
        final Ringbuffer<Object> ringbuffer2 = instance2.getRingbuffer("testRingbufferStore");

        // the actual ring buffer is empty but we can still load items from it
        assertEquals(0, ringbuffer2.size());
        assertEquals(DEFAULT_CAPACITY, ringbuffer2.remainingCapacity());
        assertEquals(numItems, rbStore.store.size());

        for (int i = 0; i < numItems; i++) {
            assertEquals(i, ringbuffer2.readOne(i));
        }

        rbStore.assertAwait(3);
    }

    @Test
    public void testRingbufferStoreAllAndReadFromMemory() throws Exception {
        final int numItems = 200;
        final WriteOnlyRingbufferStore<Integer> rbStore = new WriteOnlyRingbufferStore<Integer>();
        final RingbufferStoreConfig rbStoreConfig = new RingbufferStoreConfig()
                .setEnabled(true)
                .setStoreImplementation(rbStore);
        final Config config = getConfig("testRingbufferStore", DEFAULT_CAPACITY, OBJECT, rbStoreConfig);
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance instance = factory.newHazelcastInstance(config);
        final HazelcastInstance instance2 = factory.newHazelcastInstance(config);
        warmUpPartitions(instance, instance2);

        // add items to both ring buffers (master and backup) and shut down the master
        final Ringbuffer<Object> ringbuffer = instance.getRingbuffer("testRingbufferStore");
        final ArrayList<Integer> items = new ArrayList<Integer>();
        for (int i = 0; i < numItems; i++) {
            items.add(i);
        }
        ringbuffer.addAllAsync(items, OverflowPolicy.OVERWRITE).toCompletableFuture().get();
        terminateInstance(instance);

        // now read items from the backup
        final Ringbuffer<Object> ringbuffer2 = instance2.getRingbuffer("testRingbufferStore");
        assertEquals(numItems, ringbuffer2.size());
        assertEquals(numItems, rbStore.store.size());

        // assert that the backup has all items in memory, without loading from the store
        for (int i = 0; i < numItems; i++) {
            assertEquals(i, ringbuffer2.readOne(i));
        }
    }

    @Test
    public void testRingbufferStoreMoreThanCapacity() throws Exception {
        final int capacity = 1000;

        final TestRingbufferStore<Integer> rbStore = new TestRingbufferStore<Integer>(capacity * 2, 0, 0);
        final RingbufferStoreConfig rbStoreConfig = new RingbufferStoreConfig()
                .setEnabled(true)
                .setStoreImplementation(rbStore);
        final Config config = getConfig("testRingbufferStore", capacity, OBJECT, rbStoreConfig);

        final HazelcastInstance instance = createHazelcastInstance(config);
        final Ringbuffer<Object> ringbuffer = instance.getRingbuffer("testRingbufferStore");

        for (int i = 0; i < capacity * 2; i++) {
            ringbuffer.add(i);
        }

        assertEquals(capacity, ringbuffer.size());
        assertEquals(capacity * 2, rbStore.store.size());

        for (int i = 0; i < capacity * 2; i++) {
            assertEquals(i, ringbuffer.readOne(i));
        }

        rbStore.assertAwait(3);
    }

    @Test
    public void testStoreId_whenNodeDown() throws InterruptedException {
        final IdCheckerRingbufferStore rbStore = new IdCheckerRingbufferStore();
        final RingbufferStoreConfig rbStoreConfig = new RingbufferStoreConfig()
                .setEnabled(true)
                .setStoreImplementation(rbStore);
        final Config config = getConfig("default", DEFAULT_CAPACITY, OBJECT, rbStoreConfig);

        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        final HazelcastInstance instance2 = factory.newHazelcastInstance(config);

        final String name = generateKeyOwnedBy(instance1);
        final Ringbuffer<Object> ringbuffer = instance2.getRingbuffer(name);
        final HashMap<Long, String> addedItems = new HashMap<Long, String>();

        for (int i = 0; i < 3; i++) {
            final String item = randomString();
            addedItems.put(ringbuffer.add(item), item);
        }
        instance1.shutdown();

        final String item = randomString();
        addedItems.put(ringbuffer.add(item), item);

        for (Entry<Long, String> e : addedItems.entrySet()) {
            assertEquals("The ring buffer returned a different object than the one which was stored",
                    e.getValue(), ringbuffer.readOne(e.getKey()));
        }
    }

    @Test
    public void testStoreId_writeToMasterAndReadFromBackup() throws InterruptedException {
        final IdCheckerRingbufferStore rbStore = new IdCheckerRingbufferStore();
        final RingbufferStoreConfig rbStoreConfig = new RingbufferStoreConfig()
                .setEnabled(true)
                .setStoreImplementation(rbStore);
        final Config config = getConfig("default", DEFAULT_CAPACITY, OBJECT, rbStoreConfig);

        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        final HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        final HazelcastInstance instance2 = factory.newHazelcastInstance(config);
        warmUpPartitions(instance1, instance2);

        final String name = generateKeyOwnedBy(instance1);
        final Ringbuffer<Object> masterRB = instance1.getRingbuffer(name);
        final HashMap<Long, Integer> addedItems = new HashMap<Long, Integer>();

        for (int i = 0; i < 100; i++) {
            addedItems.put(masterRB.add(i), i);
        }
        terminateInstance(instance1);

        final Ringbuffer<Object> backupRB = instance2.getRingbuffer(name);
        for (Entry<Long, Integer> e : addedItems.entrySet()) {
            assertEquals("The ring buffer returned a different object than the one which was stored",
                    e.getValue(), backupRB.readOne(e.getKey()));
        }
    }

    @Test
    public void testRingbufferStoreFactory() {
        final String ringbufferName = randomString();
        final SimpleRingbufferStoreFactory rbStoreFactory = new SimpleRingbufferStoreFactory();
        final RingbufferStoreConfig rbStoreConfig = new RingbufferStoreConfig()
                .setEnabled(true)
                .setFactoryImplementation(rbStoreFactory);
        final Config config = getConfig(ringbufferName, DEFAULT_CAPACITY, OBJECT, rbStoreConfig);
        final HazelcastInstance instance = createHazelcastInstance(config);

        final Ringbuffer<Object> ringbuffer = instance.getRingbuffer(ringbufferName);
        ringbuffer.add(1);

        assertEquals(1, rbStoreFactory.stores.size());

        final TestRingbufferStore ringbufferStore = (TestRingbufferStore) rbStoreFactory.stores.get(ringbufferName);
        int size = ringbufferStore.store.size();
        assertEquals("Ring buffer store size should be 1 but found " + size, 1, size);
    }

    @Test
    public void testRingbufferStoreFactoryIsNotInitialized_whenDisabledInRingbufferStoreConfig() {
        final String ringbufferName = randomString();
        final SimpleRingbufferStoreFactory rbStoreFactory = new SimpleRingbufferStoreFactory();
        final RingbufferStoreConfig rbStoreConfig = new RingbufferStoreConfig()
                .setEnabled(false)
                .setFactoryImplementation(rbStoreFactory);
        final Config config = getConfig(ringbufferName, DEFAULT_CAPACITY, OBJECT, rbStoreConfig);
        final HazelcastInstance instance = createHazelcastInstance(config);

        final Ringbuffer<Object> ringbuffer = instance.getRingbuffer(ringbufferName);
        ringbuffer.add(1);

        assertEquals("Expected that the RingbufferStore would not be initialized since we disabled it"
                + " in the RingbufferStoreConfig, but found initialized", 0, rbStoreFactory.stores.size());
    }

    @Test
    public void testRingbufferStore_withBinaryModeOn() throws InterruptedException {
        final String ringbufferName = randomString();
        final RingbufferStoreConfig rbStoreConfig = new RingbufferStoreConfig()
                .setStoreImplementation(new TestRingbufferStore<Data>())
                .setEnabled(true);
        final Config config = getConfig(ringbufferName, DEFAULT_CAPACITY, BINARY, rbStoreConfig);

        final HazelcastInstance node = createHazelcastInstance(config);
        final Ringbuffer<Object> ringbuffer = node.getRingbuffer(ringbufferName);
        ringbuffer.add(1);
        ringbuffer.add(2);
        final long lastSequence = ringbuffer.add(3);

        assertEquals(3, ringbuffer.readOne(lastSequence));
    }

    @Test(expected = HazelcastException.class)
    public void testRingbufferStore_addThrowsException() {
        final String ringbufferName = randomString();
        final RingbufferStoreConfig rbStoreConfig = new RingbufferStoreConfig()
                .setStoreImplementation(new ExceptionThrowingRingbufferStore())
                .setEnabled(true);
        final Config config = getConfig(ringbufferName, DEFAULT_CAPACITY, OBJECT, rbStoreConfig);

        final HazelcastInstance node = createHazelcastInstance(config);
        final Ringbuffer<Object> ringbuffer = node.getRingbuffer(ringbufferName);
        ringbuffer.add(1);
    }

    @Test(expected = ExecutionException.class)
    public void testRingbufferStore_addAllThrowsException() throws Exception {
        final String ringbufferName = randomString();
        final RingbufferStoreConfig rbStoreConfig = new RingbufferStoreConfig()
                .setStoreImplementation(new ExceptionThrowingRingbufferStore())
                .setEnabled(true);
        final Config config = getConfig(ringbufferName, DEFAULT_CAPACITY, OBJECT, rbStoreConfig);

        final HazelcastInstance node = createHazelcastInstance(config);
        final Ringbuffer<Object> ringbuffer = node.getRingbuffer(ringbufferName);
        ringbuffer.addAllAsync(Arrays.asList(1, 2), OverflowPolicy.OVERWRITE).toCompletableFuture().get();
    }

    @Test(expected = HazelcastException.class)
    public void testRingbufferStore_getLargestSequenceThrowsException() {
        final String ringbufferName = randomString();
        final RingbufferStoreConfig rbStoreConfig = new RingbufferStoreConfig()
                .setStoreImplementation(new ExceptionThrowingRingbufferStore(true))
                .setEnabled(true);
        final Config config = getConfig(ringbufferName, DEFAULT_CAPACITY, OBJECT, rbStoreConfig);

        final HazelcastInstance node = createHazelcastInstance(config);
        node.getRingbuffer(ringbufferName).size();
    }


    static class SimpleRingbufferStoreFactory implements RingbufferStoreFactory<Integer> {

        private final ConcurrentMap<String, RingbufferStore> stores = new ConcurrentHashMap<String, RingbufferStore>();

        @Override
        @SuppressWarnings("unchecked")
        public RingbufferStore<Integer> newRingbufferStore(String name, Properties properties) {
            return ConcurrencyUtil.getOrPutIfAbsent(stores, name, new ConstructorFunction<String, RingbufferStore>() {
                @Override
                public RingbufferStore<Integer> createNew(String arg) {
                    return new TestRingbufferStore<Integer>();
                }
            });
        }
    }

    static class IdCheckerRingbufferStore<T> implements RingbufferStore<T> {
        long lastKey = -1;
        final Map<Long, T> store = new LinkedHashMap<Long, T>();

        @Override
        public void store(final long sequence, final T value) {
            if (lastKey >= sequence) {
                throw new RuntimeException("key[" + sequence + "] is already stored");
            }
            lastKey = sequence;
            store.put(sequence, value);
        }

        @Override
        public void storeAll(long firstItemSequence, T[] items) {
            throw new UnsupportedOperationException();
        }

        @Override
        public T load(final long sequence) {
            return store.get(sequence);
        }

        @Override
        public long getLargestSequence() {
            return lastKey;
        }
    }

    static class ExceptionThrowingRingbufferStore<T> implements RingbufferStore<T> {
        private final boolean getLargestSequenceThrowsException;

        ExceptionThrowingRingbufferStore() {
            this(false);
        }

        ExceptionThrowingRingbufferStore(boolean getLargestSequenceThrowsException) {
            this.getLargestSequenceThrowsException = getLargestSequenceThrowsException;
        }

        @Override
        public void store(long sequence, T data) {
            throw new RuntimeException();
        }

        @Override
        public void storeAll(long firstItemSequence, T[] items) {
            throw new RuntimeException();
        }

        @Override
        public T load(long sequence) {
            throw new RuntimeException();
        }

        @Override
        public long getLargestSequence() {
            if (getLargestSequenceThrowsException) {
                throw new RuntimeException();
            }
            return -1;
        }
    }

    static class TestRingbufferStore<T> implements RingbufferStore<T> {

        final Map<Long, T> store = new LinkedHashMap<Long, T>();
        final AtomicInteger callCount = new AtomicInteger();
        final AtomicInteger destroyCount = new AtomicInteger();

        final CountDownLatch latchStore;
        final CountDownLatch latchStoreAll;
        final CountDownLatch latchLoad;

        TestRingbufferStore() {
            this(0, 0, 0);
        }

        TestRingbufferStore(int expectedStore, int expectedStoreAll, int expectedLoad) {
            latchStore = new CountDownLatch(expectedStore);
            latchStoreAll = new CountDownLatch(expectedStoreAll);
            latchLoad = new CountDownLatch(expectedLoad);
        }

        public void destroy() {
            destroyCount.incrementAndGet();
        }

        void assertAwait(int seconds) throws Exception {
            assertTrue("Store remaining: " + latchStore.getCount(), latchStore.await(seconds, SECONDS));
            assertTrue("Store-all remaining: " + latchStoreAll.getCount(), latchStoreAll.await(seconds, SECONDS));
            assertTrue("Load remaining: " + latchLoad.getCount(), latchLoad.await(seconds, SECONDS));
        }

        Map<Long, T> getStore() {
            return store;
        }

        @Override
        public void store(long sequence, T data) {
            store.put(sequence, data);
            callCount.incrementAndGet();
            latchStore.countDown();
        }

        @Override
        public void storeAll(long firstItemSequence, T[] items) {
            for (int i = 0; i < items.length; i++) {
                store.put(firstItemSequence + i, items[i]);
            }
            callCount.incrementAndGet();
            latchStoreAll.countDown();
        }

        @Override
        public T load(long sequence) {
            callCount.incrementAndGet();
            latchLoad.countDown();
            return store.get(sequence);
        }

        @Override
        public long getLargestSequence() {
            final Set<Long> coll = store.keySet();
            return coll.isEmpty() ? -1 : Collections.max(coll);
        }
    }

    static class WriteOnlyRingbufferStore<T> implements RingbufferStore<T> {

        final Map<Long, T> store = new LinkedHashMap<Long, T>();


        WriteOnlyRingbufferStore() {
        }

        Map<Long, T> getStore() {
            return store;
        }

        @Override
        public void store(long sequence, T data) {
            store.put(sequence, data);
        }

        @Override
        public void storeAll(long firstItemSequence, T[] items) {
            for (int i = 0; i < items.length; i++) {
                store.put(firstItemSequence + i, items[i]);
            }
        }

        @Override
        public T load(long sequence) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getLargestSequence() {
            final Set<Long> coll = store.keySet();
            return coll.isEmpty() ? -1 : Collections.max(coll);
        }
    }
}
