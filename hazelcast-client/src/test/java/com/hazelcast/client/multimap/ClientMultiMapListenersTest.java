/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MapEvent;
import com.hazelcast.core.MultiMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

import static com.hazelcast.test.HazelcastTestSupport.assertOpenEventually;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ClientMultiMapListenersTest {

    private static HazelcastInstance server;
    private static HazelcastInstance client;

    private String multiMapName;
    private MultiMap<Object, Object> multiMap;

    @BeforeClass
    public static void beforeClass() {
        server = Hazelcast.newHazelcastInstance();
        client = HazelcastClient.newHazelcastClient();
    }

    @AfterClass
    public static void afterClass() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Before
    public void setup()
    {
        multiMapName = randomString();
        multiMap = client.getMultiMap(multiMapName);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAddLocalEntryListener_whenNull() {
        multiMap.addLocalEntryListener(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAddLocalEntryListener() {
        MyEntryListener myEntryListener = new CountDownValueNotNullListener(1);
        multiMap.addLocalEntryListener(myEntryListener);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAddListener_whenListenerNull() throws InterruptedException {
        multiMap.addEntryListener(null, true);
    }

    @Test
    public void testRemoveListener() throws InterruptedException {
        MyEntryListener listener = new CountDownValueNotNullListener(1);
        String id = multiMap.addEntryListener(listener, true);

        assertTrue(multiMap.removeEntryListener(id));
    }

    @Test
    public void testRemoveListener_whenNotExist() throws InterruptedException {
        assertFalse(multiMap.removeEntryListener("NOT_THERE"));
    }

    @Test
    public void testListenerEntryAddEvent() throws InterruptedException {
        int maxKeys = 12;
        int maxItems = 3;

        MyEntryListener listener = new CountDownValueNotNullListener(maxKeys * maxItems);
        multiMap.addEntryListener(listener, true);

        for (int i = 0; i < maxKeys; i++) {
            for (int j = 0; j < maxKeys; j++) {
                multiMap.put(i, j);
            }
        }

        assertOpenEventually(listener.addLatch);
    }

    @Test
    public void testListenerEntryAddEvent_whenValueNotIncluded() throws InterruptedException {
        int maxKeys = 21;
        int maxItems = 3;

        MyEntryListener listener = new CountDownValueNullListener(maxKeys * maxItems);
        multiMap.addEntryListener(listener, false);

        for (int i = 0; i < maxKeys; i++) {
            for (int j = 0; j < maxKeys; j++) {
                multiMap.put(i, j);
            }
        }

        assertOpenEventually(listener.addLatch);
    }

    @Test
    public void testListenerEntryRemoveEvent() throws InterruptedException {
        int maxKeys = 25;
        int maxItems = 3;

        MyEntryListener listener = new CountDownValueNotNullListener(maxKeys * maxItems);
        multiMap.addEntryListener(listener, true);

        for (int i = 0; i < maxKeys; i++) {
            for (int j = 0; j < maxKeys; j++) {
                multiMap.put(i, j);
                multiMap.remove(i);
            }
        }

        assertOpenEventually(listener.removeLatch);
    }

    @Test
    public void testListenerEntryRemoveEvent_whenValueNotIncluded() throws InterruptedException {
        int maxKeys = 31;
        int maxItems = 3;

        MyEntryListener listener = new CountDownValueNullListener(maxKeys * maxItems);
        multiMap.addEntryListener(listener, false);

        for (int i = 0; i < maxKeys; i++) {
            for (int j = 0; j < maxKeys; j++) {
                multiMap.put(i, j);
                multiMap.remove(i);
            }
        }

        assertOpenEventually(listener.removeLatch);
    }

    @Test
    public void testListenerOnKeyEntryAddEvent() throws InterruptedException {
        Object key = "key";
        int maxItems = 42;

        MyEntryListener listener = new CountDownValueNotNullListener(maxItems);
        multiMap.addEntryListener(listener, key, true);

        for (int i = 0; i < maxItems; i++) {
            multiMap.put(key, i);
        }

        assertOpenEventually(listener.addLatch);
    }

    @Test
    public void testListenerOnKeyEntryAddEvent_whenValueNotIncluded() throws InterruptedException {
        Object key = "key";
        int maxItems = 72;

        MyEntryListener listener = new CountDownValueNullListener(maxItems);
        multiMap.addEntryListener(listener, key, false);

        for (int i = 0; i < maxItems; i++) {
            multiMap.put(key, i);
        }

        assertOpenEventually(listener.addLatch);
    }

    @Test
    public void testListenerOnKeyEntryRemoveEvent() throws InterruptedException {
        Object key = "key";
        int maxItems = 88;

        MyEntryListener listener = new CountDownValueNotNullListener(maxItems);
        multiMap.addEntryListener(listener, key, true);

        for (int i = 0; i < maxItems; i++) {
            multiMap.put(key, i);
            multiMap.remove(key, i);
        }

        assertOpenEventually(listener.removeLatch);
    }

    @Test
    public void testListenerOnKeyEntryRemoveEvent_whenValueNotIncluded() throws InterruptedException {
        Object key = "key";
        int maxItems = 62;

        MyEntryListener listener = new CountDownValueNullListener(maxItems);
        multiMap.addEntryListener(listener, key, false);

        for (int i = 0; i < maxItems; i++) {
            multiMap.put(key, i);
            multiMap.remove(key, i);
        }

        assertOpenEventually(listener.removeLatch);
    }

    @Test
    public void testListenerOnKeyEntryRemove_WithOneRemove() throws InterruptedException {
        Object key = "key";
        int maxItems = 98;

        MyEntryListener listener = new CountDownValueNotNullListener(maxItems, 1);
        multiMap.addEntryListener(listener, key, true);

        for (int i = 0; i < maxItems; i++) {
            multiMap.put(key, i);
        }
        multiMap.remove(key);

        assertOpenEventually(listener.removeLatch);
    }

    @Test
    public void testListenerOnKeyEntryRemove_WithOneRemoveWhenValueNotIncluded() throws InterruptedException {
        Object key = "key";
        int maxItems = 56;

        MyEntryListener listener = new CountDownValueNullListener(maxItems, 1);
        multiMap.addEntryListener(listener, key, false);

        for (int i = 0; i < maxItems; i++) {
            multiMap.put(key, i);
        }
        multiMap.remove(key);

        assertOpenEventually(listener.removeLatch);
    }

    @Test
    public void testListeners_clearAll() {
        MyEntryListener listener = new CountDownValueNullListener(1);
        multiMap.addEntryListener(listener, false);
        multiMap.put("key", "value");
        multiMap.clear();

        assertOpenEventually(listener.addLatch);
        assertOpenEventually(listener.clearLatch);
    }

    @Test
    public void testListeners_clearAllFromNode() {
        MyEntryListener listener = new CountDownValueNullListener(1);

        multiMap.addEntryListener(listener, false);
        multiMap.put("key", "value");

        server.getMultiMap(multiMapName).clear();

        assertOpenEventually(listener.addLatch);
        assertOpenEventually(listener.clearLatch);
    }

    private static abstract class MyEntryListener implements EntryListener<Object, Object> {
        protected final CountDownLatch addLatch;
        protected final CountDownLatch removeLatch;
        protected final CountDownLatch updateLatch;
        protected final CountDownLatch evictLatch;
        protected final CountDownLatch clearLatch;

        public MyEntryListener(int latchCount) {
            addLatch = new CountDownLatch(latchCount);
            removeLatch = new CountDownLatch(latchCount);
            updateLatch = new CountDownLatch(1);
            evictLatch = new CountDownLatch(1);
            clearLatch = new CountDownLatch(1);
        }

        public MyEntryListener(int addlatchCount, int removeLatchCount) {
            addLatch = new CountDownLatch(addlatchCount);
            removeLatch = new CountDownLatch(removeLatchCount);
            updateLatch = new CountDownLatch(1);
            evictLatch = new CountDownLatch(1);
            clearLatch = new CountDownLatch(1);
        }
    }

    private static class CountDownValueNotNullListener extends MyEntryListener {
        public CountDownValueNotNullListener(int latchCount) {
            super(latchCount);
        }

        public CountDownValueNotNullListener(int addLatchCount, int removeLatchCount) {
            super(addLatchCount, removeLatchCount);
        }

        public void entryAdded(EntryEvent event) {
            if (event.getValue() != null) {
                addLatch.countDown();
            }
        }

        public void entryRemoved(EntryEvent event) {
            if (event.getValue() != null) {
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

    private static class CountDownValueNullListener extends MyEntryListener {
        public CountDownValueNullListener(int latchCount) {
            super(latchCount);
        }

        public CountDownValueNullListener(int addlatchCount, int removeLatchCount) {
            super(addlatchCount, removeLatchCount);
        }

        public void entryAdded(EntryEvent event) {
            if (event.getValue() == null) {
                addLatch.countDown();
            }
        }

        public void entryRemoved(EntryEvent event) {
            if (event.getValue() == null) {
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