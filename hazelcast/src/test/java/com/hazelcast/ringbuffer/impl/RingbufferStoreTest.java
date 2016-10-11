/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.RingbufferStore;
import com.hazelcast.core.RingbufferStoreFactory;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.config.RingbufferConfig.DEFAULT_CAPACITY;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
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

        final HazelcastInstance instance = factory.newHazelcastInstance(config);
        final Ringbuffer<Object> ringbuffer = instance.getRingbuffer("testRingbufferStore");
        for (int i = 0; i < numItems; i++) {
            ringbuffer.add(i);
        }
        instance.shutdown();

        final HazelcastInstance instance2 = factory.newHazelcastInstance(config);
        final Ringbuffer<Object> ringbuffer2 = instance2.getRingbuffer("testRingbufferStore");

        // the actual ring buffer is empty but we can still load items from it
        assertEquals(0, ringbuffer2.size());
        assertEquals(numItems, rbStore.store.size());

        for (int i = 0; i < numItems; i++) {
            assertEquals(i, ringbuffer2.readOne(i));
        }

        rbStore.assertAwait(3);
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
    @Ignore(value = "Since the RingBufferStoreConfig is now propagated correctly to the read-only config the test runs" +
            " into a NPE while casting `null` to `long` in `IdCheckerRingbufferStore.getLargestSequence()`")
    public void testStoreId_whenNodeDown() {
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
        ringbuffer.add(randomString());
        ringbuffer.add(randomString());
        ringbuffer.add(randomString());

        instance1.shutdown();

        ringbuffer.add(randomString());
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

        assertEquals("Expected that the ring buffer store would not be initialized" +
                " since we disabled it in the ring buffer store config but found initialized ", 0, rbStoreFactory.stores.size());
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

    static class IdCheckerRingbufferStore implements RingbufferStore {
        Long lastKey;

        @Override
        public void store(final long sequence, final Object value) {
            if (lastKey != null && lastKey >= sequence) {
                throw new RuntimeException("key[" + sequence + "] is already stored");
            }
            lastKey = sequence;
        }

        @Override
        public void storeAll(long firstItemSequence, Object[] items) {

        }

        @Override
        public Object load(final long sequence) {
            return null;
        }

        @Override
        public long getLargestSequence() {
            return lastKey;
        }
    }

    static class TestRingbufferStore<T> implements RingbufferStore<T> {

        final Map<Long, T> store = new LinkedHashMap<Long, T>();
        final AtomicInteger callCount = new AtomicInteger();
        final AtomicInteger destroyCount = new AtomicInteger();

        final CountDownLatch latchStore;
        final CountDownLatch latchStoreAll;
        final CountDownLatch latchLoad;

        public TestRingbufferStore() {
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

        Map getStore() {
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
}
