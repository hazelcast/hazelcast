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
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static com.hazelcast.test.HazelcastTestSupport.sleepSeconds;
import static org.junit.Assert.*;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ClientMultiMapTest {

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

    @Test
    public void testPut() {
        final Object key = "key1";
        final MultiMap mm = client.getMultiMap(randomString());

        assertTrue(mm.put(key, 1));
        assertTrue(mm.put(key, 2));
    }

    @Test
    public void testPut_WithExistingKeyValue() {
        final Object key = "key1";
        final MultiMap mm = client.getMultiMap(randomString());

        assertTrue(mm.put(key, 1));
        assertFalse(mm.put(key, 1));
    }

    @Test
    public void testValueCount() {
        final Object key = "key1";

        final MultiMap mm = client.getMultiMap(randomString());

        mm.put(key, 1);
        mm.put(key, 2);

        assertEquals(2, mm.valueCount(key));
        assertEquals(0, mm.valueCount("NOT_THERE"));
    }

    @Test
    public void testSizeCount() {
        final Object key1 = "key1";
        final Object key2 = "key2";

        final MultiMap mm = client.getMultiMap(randomString());

        mm.put(key1, 1);
        mm.put(key1, 2);

        mm.put(key2, 1);
        mm.put(key2, 2);

        assertEquals(4, mm.size());
    }

    @Test
    public void testEmptySizeCount() {
        final MultiMap mm = client.getMultiMap(randomString());
        assertEquals(0, mm.size());
    }

    @Test
    public void testGetNotThere() {
        final MultiMap mm = client.getMultiMap(randomString());
        Collection coll = mm.get("NOT_THERE");

        assertEquals(Collections.EMPTY_LIST, coll);
    }

    @Test
    public void testGet() {
        final Object key = "key";
        final int maxItemsPerKey = 3;
        final MultiMap mm = client.getMultiMap(randomString());

        Set expeted = new TreeSet();
        for ( int i=0; i< maxItemsPerKey; i++ ){
            mm.put(key, i);
            expeted.add(i);
        }

        Collection resultSet = new TreeSet( mm.get(key) );

        assertEquals(expeted, resultSet);
    }

    @Test
    public void testRemoveNotThere() {
        final MultiMap mm = client.getMultiMap(randomString());
        Collection coll = mm.remove("NOT_THERE");

        assertEquals(Collections.EMPTY_LIST, coll);
    }

    @Test
    public void testRemoveKey() {
        final Object key = "key";
        final int maxItemsPerKey = 44;
        final MultiMap mm = client.getMultiMap(randomString());

        Set expeted = new TreeSet();
        for ( int i=0; i< maxItemsPerKey; i++ ){
            mm.put(key, i);
            expeted.add(i);
        }
        Set resultSet  = new TreeSet( mm.remove(key) );

        assertEquals(expeted, resultSet);
    }

    @Test
    public void testRemoveKeyValueNotThere() {
        final Object key = "key";
        final int maxItemsPerKey = 4;
        final MultiMap mm = client.getMultiMap(randomString());

        for ( int i=0; i< maxItemsPerKey; i++ ){
            mm.put(key, i);
        }
        boolean result = mm.remove(key, "NOT_THERE");

        assertFalse(result);
    }

    @Test
    public void testRemoveKeyValue() {
        final Object key = "key";
        final int maxItemsPerKey = 4;
        final MultiMap mm = client.getMultiMap(randomString());

        for ( int i=0; i< maxItemsPerKey; i++ ){
            mm.put(key, i);
        }

        for ( int i=0; i< maxItemsPerKey; i++ ){
            boolean result = mm.remove(key, i);
            assertTrue(result);
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testLocalKeySet() {
        final MultiMap mm = client.getMultiMap(randomString());
        mm.localKeySet();
    }


    @Test
    public void testEmptyKeySet() {
        final MultiMap mm = client.getMultiMap(randomString());
        assertEquals(Collections.EMPTY_SET, mm.keySet());
    }

    @Test
    public void testKeySet() {
        final int maxKeys = 23;
        final MultiMap mm = client.getMultiMap(randomString());

        Set expeted = new TreeSet();
        for ( int key=0; key< maxKeys; key++ ){
            mm.put(key, key);
            expeted.add(key);
        }

        assertEquals(expeted, mm.keySet());
    }

    @Test
    public void testEmptyValues() {
        final MultiMap mm = client.getMultiMap(randomString());
        assertEquals(Collections.EMPTY_LIST, mm.values());
    }

    @Test
    public void testKeyValues() {
        final int maxKeys = 31;
        final MultiMap mm = client.getMultiMap(randomString());

        Set expeted = new TreeSet();
        for ( int key=0; key< maxKeys; key++ ){
            mm.put(key, key);
            expeted.add(key);
        }

        Set resultSet = new TreeSet( mm.values() );

        assertEquals(expeted, resultSet);
    }

    @Test
    public void testEmptyEntrySet() {
        final MultiMap mm = client.getMultiMap(randomString());
        assertEquals(Collections.EMPTY_SET, mm.entrySet());
    }

    @Test
    public void testEnterySet() {
        final int maxKeys = 14;
        final MultiMap mm = client.getMultiMap(randomString());

        for ( int key=0; key< maxKeys; key++ ){
            mm.put(key, key);
            mm.put(key, key+maxKeys);
        }

        assertEquals(maxKeys * 2, mm.entrySet().size());
    }

    @Test
    public void testContainsKey() {
        final MultiMap mm = client.getMultiMap(randomString());
        mm.put("key1", "value1");

        assertTrue(mm.containsKey("key1"));
        assertFalse(mm.containsKey("NOT_THERE"));
    }

    @Test
    public void testContainsValue() {
        final MultiMap mm = client.getMultiMap(randomString());
        mm.put("key1", "value1");

        assertTrue(mm.containsValue("value1"));
        assertFalse(mm.containsValue("NOT_THERE"));
    }

    @Test
    public void testContainsEntry() {
        final MultiMap mm = client.getMultiMap(randomString());
        mm.put("key1", "value1");

        assertTrue(mm.containsEntry("key1", "value1"));
        assertFalse(mm.containsEntry("key1", "NOT_THERE"));
        assertFalse(mm.containsEntry("NOT_THERE", "NOT_THERE"));
        assertFalse(mm.containsEntry("NOT_THERE", "value1"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAddLocalEntryListener() {
        final MultiMap mm = client.getMultiMap(randomString());
        mm.addLocalEntryListener(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetLocalMultiMapStats() {
        final MultiMap mm = client.getMultiMap(randomString());
        mm.getLocalMultiMapStats();
    }

    @Test
    public void testisLock() throws Exception {
        final MultiMap mm = client.getMultiMap(randomString());
        final Object key = "KeyNotLocked";
        assertFalse(mm.isLocked(key));
    }

    @Test
    public void testLock() throws Exception {
        final MultiMap mm = client.getMultiMap(randomString());
        final Object key = "Key";
        mm.lock(key);
        assertTrue(mm.isLocked(key));
    }

    @Test
    public void testMulityLockCalls() throws Exception {
        final MultiMap mm = client.getMultiMap(randomString());
        final Object key = "Key";
        mm.lock(key);
        mm.lock(key);
        assertTrue(mm.isLocked(key));
    }

    @Test
    public void testLockAndTryLock() throws Exception {
        final MultiMap mm = client.getMultiMap(randomString());
        final Object key = "Key";
        mm.lock(key);
        assertTrue(mm.tryLock(key));
    }

    @Test
    public void testLock_WithTryLock() throws Exception {
        final MultiMap mm = client.getMultiMap(randomString());
        final Object key = "Key1";
        mm.lock(key);
        final CountDownLatch tryLockFailed = new CountDownLatch(1);
        new Thread() {
            public void run() {
                if (mm.tryLock(key) == false) {
                    tryLockFailed.countDown();
                }
            }
        }.start();
        assertTrue(tryLockFailed.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void testLockTTL() throws Exception {
        final MultiMap mm = client.getMultiMap(randomString());
        final Object key = "Key";

        mm.lock(key, 1, TimeUnit.SECONDS);
        sleepSeconds(1);
        assertFalse(mm.isLocked(key));
    }

    @Test
    public void testLockTTTL_threaded() throws Exception {
        final MultiMap mm = client.getMultiMap(randomString());
        final Object key = "Key";

        mm.lock(key, 2, TimeUnit.SECONDS);
        final CountDownLatch tryLockSuccess = new CountDownLatch(1);
        new Thread() {
            public void run() {
                try {
                    if (mm.tryLock(key, 4, TimeUnit.SECONDS)) {
                        tryLockSuccess.countDown();
                    }
                } catch (InterruptedException e) {
                    fail(e.getMessage());
                }
            }
        }.start();
        assertTrue(tryLockSuccess.await(10, TimeUnit.SECONDS));
    }

    @Test
    public void testUnLock() throws Exception {
        final MultiMap mm = client.getMultiMap(randomString());
        final Object key = "key";

        mm.lock(key);
        mm.unlock(key);
        assertFalse(mm.isLocked(key));
    }

    @Test
    public void testUnLockThreaded() throws Exception {
        final MultiMap mm = client.getMultiMap(randomString());
        final Object key = "keyZ";

        mm.lock(key);

        final CountDownLatch tryLockReturnsTrue = new CountDownLatch(1);
        new Thread(){
            public void run() {
                try {
                    if(mm.tryLock(key, 10, TimeUnit.SECONDS)){
                        tryLockReturnsTrue.countDown();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }.start();

        sleepSeconds(1);
        mm.unlock(key);

        assertTrue(tryLockReturnsTrue.await(20, TimeUnit.SECONDS));
        assertTrue(mm.isLocked(key));
    }

    @Test
    public void testForceUnlock() throws Exception {
        final MultiMap mm = client.getMultiMap(randomString());
        final Object key = "key";
        mm.lock(key);
        final CountDownLatch forceUnlock = new CountDownLatch(1);
        new Thread(){
            public void run() {
                mm.forceUnlock(key);
                forceUnlock.countDown();
            }
        }.start();
        assertTrue(forceUnlock.await(30, TimeUnit.SECONDS));
        assertFalse(mm.isLocked(key));
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