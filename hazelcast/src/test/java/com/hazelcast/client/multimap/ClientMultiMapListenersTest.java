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

package com.hazelcast.client.multimap;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.map.MapEvent;
import com.hazelcast.multimap.MultiMap;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.test.HazelcastTestSupport.assertOpenEventually;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientMultiMapListenersTest {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    private HazelcastInstance server;
    private HazelcastInstance client;

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Before
    public void setup() {
        server = hazelcastFactory.newHazelcastInstance();
        client = hazelcastFactory.newHazelcastClient();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAddLocalEntryListener_whenNull() {
        final MultiMap mm = client.getMultiMap(randomString());
        mm.addLocalEntryListener(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAddLocalEntryListener() {
        final MultiMap mm = client.getMultiMap(randomString());
        MyEntryListener myEntryListener = new CountDownValueNotNullListener(1);
        mm.addLocalEntryListener(myEntryListener);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAddLocalEntryListener_whenValueIncluded() {
        final MultiMap mm = client.getMultiMap(randomString());
        MyEntryListener myEntryListener = new CountDownValueNotNullListener(1);
        mm.addLocalEntryListener(myEntryListener, true);
    }

    @Test(expected = NullPointerException.class)
    public void testAddListener_whenListenerNull() throws InterruptedException {
        final MultiMap mm = client.getMultiMap(randomString());
        mm.addEntryListener(null, true);
    }

    @Test
    public void testRemoveListener() throws InterruptedException {
        final MultiMap mm = client.getMultiMap(randomString());

        MyEntryListener listener = new CountDownValueNotNullListener(1);
        final UUID id = mm.addEntryListener(listener, true);

        assertTrue(mm.removeEntryListener(id));
    }

    @Test
    public void testRemoveListener_whenNotExist() throws InterruptedException {
        final MultiMap mm = client.getMultiMap(randomString());

        assertFalse(mm.removeEntryListener(UuidUtil.newUnsecureUUID()));
    }

    @Test
    public void testListenerEntryAddEvent() throws InterruptedException {
        final int maxKeys = 12;
        final int maxItems = 3;
        final MultiMap mm = client.getMultiMap(randomString());

        MyEntryListener listener = new CountDownValueNotNullListener(maxKeys * maxItems);
        mm.addEntryListener(listener, true);

        for (int i = 0; i < maxKeys; i++) {
            for (int j = 0; j < maxKeys; j++) {
                mm.put(i, j);
            }
        }
        assertOpenEventually(listener.addLatch);
    }

    @Test
    public void testListenerEntryAddEvent_whenValueNotIncluded() throws InterruptedException {
        final int maxKeys = 21;
        final int maxItems = 3;
        final MultiMap mm = client.getMultiMap(randomString());

        MyEntryListener listener = new CountDownValueNullListener(maxKeys * maxItems);
        mm.addEntryListener(listener, false);

        for (int i = 0; i < maxKeys; i++) {
            for (int j = 0; j < maxKeys; j++) {
                mm.put(i, j);
            }
        }
        assertOpenEventually(listener.addLatch);
    }

    @Test
    public void testListenerEntryRemoveEvent() throws InterruptedException {
        final int maxKeys = 25;
        final int maxItems = 3;
        final MultiMap mm = client.getMultiMap(randomString());

        MyEntryListener listener = new CountDownValueNotNullListener(maxKeys * maxItems);
        mm.addEntryListener(listener, true);

        for (int i = 0; i < maxKeys; i++) {
            for (int j = 0; j < maxKeys; j++) {
                mm.put(i, j);
                mm.remove(i);
            }
        }
        assertOpenEventually(listener.removeLatch);
    }

    @Test
    public void testListenerEntryRemoveEvent_whenValueNotIncluded() throws InterruptedException {
        final int maxKeys = 31;
        final int maxItems = 3;
        final MultiMap mm = client.getMultiMap(randomString());

        MyEntryListener listener = new CountDownValueNullListener(maxKeys * maxItems);
        mm.addEntryListener(listener, false);

        for (int i = 0; i < maxKeys; i++) {
            for (int j = 0; j < maxKeys; j++) {
                mm.put(i, j);
                mm.remove(i);
            }
        }
        assertOpenEventually(listener.removeLatch);
    }

    @Test
    public void testListenerOnKeyEntryAddEvent() throws InterruptedException {
        final Object key = "key";
        final int maxItems = 42;
        final MultiMap mm = client.getMultiMap(randomString());

        MyEntryListener listener = new CountDownValueNotNullListener(maxItems);
        mm.addEntryListener(listener, key, true);

        for (int i = 0; i < maxItems; i++) {
            mm.put(key, i);
        }

        assertOpenEventually(listener.addLatch);
    }

    @Test
    public void testListenerOnKey_whenOtherKeysAdded() throws InterruptedException {
        final MultiMap mm = client.getMultiMap(randomString());

        final List<EntryEvent> events = new ArrayList<EntryEvent>();
        mm.addEntryListener(new EntryAdapter() {
            @Override
            public void entryAdded(EntryEvent event) {
                events.add(event);
            }
        }, "key", true);

        mm.put("key2", "value");
        mm.put("key", "value");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(1, events.size());
                assertEquals("key", events.get(0).getKey());
            }
        });
    }

    @Test
    public void testListenerOnKeyEntryAddEvent_whenValueNotIncluded() throws InterruptedException {
        final Object key = "key";
        final int maxItems = 72;
        final MultiMap mm = client.getMultiMap(randomString());

        MyEntryListener listener = new CountDownValueNullListener(maxItems);
        mm.addEntryListener(listener, key, false);

        for (int i = 0; i < maxItems; i++) {
            mm.put(key, i);
        }

        assertOpenEventually(listener.addLatch);
    }

    @Test
    public void testListenerOnKeyEntryRemoveEvent() throws InterruptedException {
        final Object key = "key";
        final int maxItems = 88;
        final MultiMap mm = client.getMultiMap(randomString());

        MyEntryListener listener = new CountDownValueNotNullListener(maxItems);
        mm.addEntryListener(listener, key, true);

        for (int i = 0; i < maxItems; i++) {
            mm.put(key, i);
            mm.remove(key, i);
        }

        assertOpenEventually(listener.removeLatch);
    }

    @Test
    public void testListenerOnKeyEntryRemoveEvent_whenValueNotIncluded() throws InterruptedException {
        final Object key = "key";
        final int maxItems = 62;
        final MultiMap mm = client.getMultiMap(randomString());

        MyEntryListener listener = new CountDownValueNullListener(maxItems);
        mm.addEntryListener(listener, key, false);

        for (int i = 0; i < maxItems; i++) {
            mm.put(key, i);
            mm.remove(key, i);
        }

        assertOpenEventually(listener.removeLatch);
    }

    @Test
    public void testListenerOnKeyEntryRemove_WithOneRemove() throws InterruptedException {
        final Object key = "key";
        final int maxItems = 98;
        final MultiMap mm = client.getMultiMap(randomString());

        MyEntryListener listener = new CountDownValueNotNullListener(maxItems, 1);
        final UUID id = mm.addEntryListener(listener, key, true);

        for (int i = 0; i < maxItems; i++) {
            mm.put(key, i);
        }
        mm.remove(key);

        assertOpenEventually(listener.removeLatch);
    }

    @Test
    public void testListenerOnKeyEntryRemove_WithOneRemoveWhenValueNotIncluded() throws InterruptedException {
        final Object key = "key";
        final int maxItems = 56;
        final MultiMap mm = client.getMultiMap(randomString());

        MyEntryListener listener = new CountDownValueNullListener(maxItems, 1);
        final UUID id = mm.addEntryListener(listener, key, false);

        for (int i = 0; i < maxItems; i++) {
            mm.put(key, i);
        }
        mm.remove(key);

        assertOpenEventually(listener.removeLatch);
    }

    @Test
    public void testListeners_clearAll() {
        final MultiMap mm = client.getMultiMap(randomString());
        MyEntryListener listener = new CountDownValueNullListener(1);
        mm.addEntryListener(listener, false);
        mm.put("key", "value");
        mm.clear();
        assertOpenEventually(listener.addLatch);
        assertOpenEventually(listener.clearLatch);
    }

    @Test
    public void testListeners_clearAllFromNode() {
        final String name = randomString();
        final MultiMap mm = client.getMultiMap(name);
        MyEntryListener listener = new CountDownValueNullListener(1);
        mm.addEntryListener(listener, false);
        mm.put("key", "value");
        server.getMultiMap(name).clear();
        assertOpenEventually(listener.addLatch);
        assertOpenEventually(listener.clearLatch);
    }

    abstract static class MyEntryListener extends EntryAdapter {

        public final CountDownLatch addLatch;
        public final CountDownLatch removeLatch;
        public final CountDownLatch updateLatch;
        public final CountDownLatch evictLatch;
        public final CountDownLatch clearLatch;

        MyEntryListener(int latchCount) {
            addLatch = new CountDownLatch(latchCount);
            removeLatch = new CountDownLatch(latchCount);
            updateLatch = new CountDownLatch(1);
            evictLatch = new CountDownLatch(1);
            clearLatch = new CountDownLatch(1);
        }

        MyEntryListener(int addlatchCount, int removeLatchCount) {
            addLatch = new CountDownLatch(addlatchCount);
            removeLatch = new CountDownLatch(removeLatchCount);
            updateLatch = new CountDownLatch(1);
            evictLatch = new CountDownLatch(1);
            clearLatch = new CountDownLatch(1);
        }
    }

    static class CountDownValueNotNullListener extends MyEntryListener {

        CountDownValueNotNullListener(int latchCount) {
            super(latchCount);
        }

        CountDownValueNotNullListener(int addlatchCount, int removeLatchCount) {
            super(addlatchCount, removeLatchCount);
        }

        public void entryAdded(EntryEvent event) {
            if (event.getValue() != null) {
                addLatch.countDown();
            }
        }

        public void entryRemoved(EntryEvent event) {
            if (event.getOldValue() != null) {
                removeLatch.countDown();
            }
        }

        public void entryUpdated(EntryEvent event) {
            if (event.getValue() != null) {
                updateLatch.countDown();
            }
        }

        public void entryEvicted(EntryEvent event) {
            if (event.getValue() != null) {
                evictLatch.countDown();
            }
        }

        @Override
        public void mapEvicted(MapEvent event) {
        }

        @Override
        public void mapCleared(MapEvent event) {
            clearLatch.countDown();
        }
    }

    static class CountDownValueNullListener extends MyEntryListener {

        CountDownValueNullListener(int latchCount) {
            super(latchCount);
        }

        CountDownValueNullListener(int addlatchCount, int removeLatchCount) {
            super(addlatchCount, removeLatchCount);
        }

        public void entryAdded(EntryEvent event) {
            if (event.getValue() == null) {
                addLatch.countDown();
            }
        }

        public void entryRemoved(EntryEvent event) {
            if (event.getOldValue() == null) {
                removeLatch.countDown();
            }
        }

        public void entryUpdated(EntryEvent event) {
            if (event.getValue() == null) {
                updateLatch.countDown();
            }
        }

        public void entryEvicted(EntryEvent event) {
            if (event.getValue() == null) {
                evictLatch.countDown();
            }
        }

        @Override
        public void mapEvicted(MapEvent event) {
        }

        @Override
        public void mapCleared(MapEvent event) {
            clearLatch.countDown();
        }
    }
}
