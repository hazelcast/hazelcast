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
import com.hazelcast.core.*;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static com.hazelcast.test.HazelcastTestSupport.sleepSeconds;
import static org.junit.Assert.*;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ClientMultiMapListnersTest {

    static HazelcastInstance server;
    static HazelcastInstance client;

    @BeforeClass
    public static void init() {
        server = Hazelcast.newHazelcastInstance();
        client = HazelcastClient.newHazelcastClient();
    }

    @AfterClass
    public static void destroy() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAddLocalEntryListener_whenNull() {
        final MultiMap mm = client.getMultiMap(randomString());
        mm.addLocalEntryListener(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAddLocalEntryListener() {
        final MultiMap mm = client.getMultiMap(randomString());
        MyEntryListener myEntryListener = new MyEntryListener(1);
        mm.addLocalEntryListener(myEntryListener);
    }

    @Test
    public void testAddListener_whenListnerNull() throws InterruptedException {
        final MultiMap mm = client.getMultiMap(randomString());
        final String id = mm.addEntryListener(null, true);
        mm.put(1, 1);
        assertTrue(mm.removeEntryListener(id));
    }

    @Test
    public void testAddSameListenerTwice_whenSameObject() throws InterruptedException {
        final MultiMap mm = client.getMultiMap(randomString());

        MyEntryListener listener = new MyEntryListener(1);
        final String id = mm.addEntryListener(listener, true);
        final String id2 = mm.addEntryListener(listener, true);

        assertTrue(mm.removeEntryListener(id));
        assertTrue(mm.removeEntryListener(id2));
    }

    @Test
    public void testListener() throws InterruptedException {
        final int maxKeys = 22;
        final int maxItems = 3;
        final MultiMap mm = client.getMultiMap(randomString());

        MyEntryListener listener = new MyEntryListener(maxKeys * maxItems);
        final String id = mm.addEntryListener(listener, true);

        for(int i=0; i<maxKeys; i++){
            for(int j=0; j<maxKeys; j++){
                mm.put(i, j);
                mm.remove(i, j);
            }
        }

        assertTrue(listener.addLatch.await(10, TimeUnit.SECONDS));
        assertTrue(listener.removeLatch.await(10, TimeUnit.SECONDS));
        assertTrue(mm.removeEntryListener(id));
    }

    @Test
    public void testListenerOnKey() throws InterruptedException {
        final Object key = "key";
        final int maxItems = 101;
        final MultiMap mm = client.getMultiMap(randomString());

        MyEntryListener listener = new MyEntryListener(maxItems);
        final String id = mm.addEntryListener(listener, key, true);

        for(int i=0; i<maxItems; i++){
            mm.put(key, "value");
            mm.remove(key, "value");
        }

        assertTrue(listener.addLatch.await(10, TimeUnit.SECONDS));
        assertTrue(listener.removeLatch.await(10, TimeUnit.SECONDS));
        assertTrue(mm.removeEntryListener(id));
        assertEquals(0, mm.size());
    }

    @Test
    public void testListenerOnKey_WithOneRemove() throws InterruptedException {
        final Object key = "key";
        final int maxItems = 101;
        final MultiMap mm = client.getMultiMap(randomString());

        MyEntryListener listener = new MyEntryListener(maxItems, 1);
        final String id = mm.addEntryListener(listener, key, true);

        for(int i=0; i<maxItems; i++){
            mm.put(key, i);
        }
        mm.remove(key);

        assertTrue(listener.addLatch.await(10, TimeUnit.SECONDS));
        assertTrue(listener.removeLatch.await(10, TimeUnit.SECONDS));
        assertTrue(mm.removeEntryListener(id));
        assertEquals(0, mm.size());
    }

    static class MyEntryListener implements EntryListener {

        final public CountDownLatch addLatch;
        final public CountDownLatch removeLatch;
        final public CountDownLatch updateLatch;
        final public CountDownLatch evictLatch;

        public MyEntryListener(int latchCount){
            addLatch = new CountDownLatch(latchCount);
            removeLatch = new CountDownLatch(latchCount);
            updateLatch = new CountDownLatch(1);
            evictLatch = new CountDownLatch(1);
        }

        public MyEntryListener(int addlatchCount, int removeLatchCount){
            addLatch = new CountDownLatch(addlatchCount);
            removeLatch = new CountDownLatch(removeLatchCount);
            updateLatch = new CountDownLatch(1);
            evictLatch = new CountDownLatch(1);
        }

        public void entryAdded(EntryEvent event) {
            addLatch.countDown();
        }

        public void entryRemoved(EntryEvent event) {
            removeLatch.countDown();
        }

        public void entryUpdated(EntryEvent event) {
            updateLatch.countDown();
        }

        public void entryEvicted(EntryEvent event) {
            evictLatch.countDown();
        }
    };
}