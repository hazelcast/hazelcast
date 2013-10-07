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

package com.hazelcast.client.map;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.Config;
import com.hazelcast.core.*;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.annotation.SerialTest;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;

/**
 * @author ali 5/22/13
 */

@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(SerialTest.class)
public class ClientMapTest {

    static final String name = "test";
    static HazelcastInstance hz;
    static HazelcastInstance server;
    static IMap map;

    @BeforeClass
    public static void init(){
        Config config = new Config();
        server = Hazelcast.newHazelcastInstance(config);
        hz = HazelcastClient.newHazelcastClient(null);
        map = hz.getMap(name);
    }

    @AfterClass
    public static void destroy() {
        hz.getLifecycleService().shutdown();
        Hazelcast.shutdownAll();
    }

    @Before
    @After
    public void clear() throws Exception {
        map.clear();
    }

    @Test
    public void testIssue537() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(2);
        final CountDownLatch nullLatch = new CountDownLatch(2);

        final EntryListener listener = new EntryListener() {
            public void entryAdded(EntryEvent event) {
                latch.countDown();
            }

            public void entryRemoved(EntryEvent event) {
            }

            public void entryUpdated(EntryEvent event) {
            }

            public void entryEvicted(EntryEvent event) {
                final Object value = event.getValue();
                final Object oldValue = event.getOldValue();
                if (value != null){
                    nullLatch.countDown();
                }
                if (oldValue != null){
                    nullLatch.countDown();
                }
                latch.countDown();
            }
        };
        final String id = map.addEntryListener(listener, true);

        map.put("key1", new GenericEvent("value1"), 2, TimeUnit.SECONDS);

        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertTrue(nullLatch.await(1, TimeUnit.SECONDS));

        map.removeEntryListener(id);

        map.put("key2", new GenericEvent("value2"));

        assertEquals(1, map.size());

    }

    @Test
    public void testContains() throws Exception {

        fillMap();

        assertFalse(map.containsKey("key10"));
        assertTrue(map.containsKey("key1"));

        assertFalse(map.containsValue("value10"));
        assertTrue(map.containsValue("value1"));

    }

    @Test
    public void testGet(){
        fillMap();
        for (int i=0; i<10; i++){
            Object o = map.get("key"+i);
            assertEquals("value"+i,o);
        }
    }

    @Test
    public void testRemoveAndDelete(){
        fillMap();
        assertNull(map.remove("key10"));
        map.delete("key9");
        assertEquals(9, map.size());
        for (int i=0; i<9; i++){
            Object o = map.remove("key" + i);
            assertEquals("value"+i,o);
        }
        assertEquals(0, map.size());
    }

    @Test
    public void testRemoveIfSame(){
        fillMap();
        assertFalse(map.remove("key2", "value"));
        assertEquals(10, map.size());

        assertTrue(map.remove("key2", "value2"));
        assertEquals(9, map.size());
    }

    @Test
    public void flush(){
        //TODO map store
    }

    @Test
    public void testGetAllPutAll() {
        Map mm = new HashMap();
        for (int i = 0; i < 100; i++) {
            mm.put(i, i);
        }
        map.putAll(mm);
        assertEquals(map.size(), 100);
        for (int i = 0; i < 100; i++) {
            assertEquals(map.get(i), i);
        }

        Set ss = new HashSet();
        ss.add(1);
        ss.add(3);
        Map m2 = map.getAll(ss);
        assertEquals(m2.size(), 2);
        assertEquals(m2.get(1), 1);
        assertEquals(m2.get(3), 3);
    }

    @Test
    public void testAsyncGet() throws Exception{
        fillMap();
        Future f = map.getAsync("key1");
        Object o = null;
        try {
            o = f.get(0, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e){
        }
        assertNull(o);
        o = f.get();
        assertEquals("value1",o);
    }

    @Test
    public void testAsyncPut() throws Exception{
        fillMap();
        Future f = map.putAsync("key3", "value");

        Object o = null;
        try {
            o = f.get(0, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e){
        }
        assertNull(o);
        o = f.get();
        assertEquals("value3",o);
        assertEquals("value", map.get("key3"));
    }


    @Test
    public void testAsyncPutWithTtl() throws Exception{
        final CountDownLatch latch = new CountDownLatch(1);
        map.addEntryListener(new EntryAdapter<String, String>() {
            public void entryEvicted(EntryEvent<String, String> event) {
                latch.countDown();
            }
        }, true);

        Future<String> f1 = map.putAsync("key", "value1", 3, TimeUnit.SECONDS);
        String f1Val = f1.get();
        assertNull(f1Val);
        assertEquals("value1", map.get("key"));

        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertNull(map.get("key"));
    }

    @Test
    public void testAsyncRemove() throws Exception {
        fillMap();
        Future f = map.removeAsync("key4");
        Object o = null;
        try {
            o = f.get(0, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e){
        }
        assertNull(o);
        o = f.get();
        assertEquals("value4", o);
        assertEquals(9, map.size());
    }

    @Test
    public void testTryPutRemove() throws Exception {
        assertTrue(map.tryPut("key1", "value1", 1, TimeUnit.SECONDS));
        assertTrue(map.tryPut("key2", "value2", 1, TimeUnit.SECONDS));
        map.lock("key1");
        map.lock("key2");

        final CountDownLatch latch = new CountDownLatch(2);

        new Thread(){
            public void run() {
                boolean result = map.tryPut("key1", "value3", 1, TimeUnit.SECONDS);
                if (!result){
                    latch.countDown();
                }
            }
        }.start();

        new Thread(){
            public void run() {
                boolean result = map.tryRemove("key2", 1, TimeUnit.SECONDS);
                if (!result){
                    latch.countDown();
                }
            }
        }.start();

        assertTrue(latch.await(20, TimeUnit.SECONDS));
        assertEquals("value1", map.get("key1"));
        assertEquals("value2", map.get("key2"));
        map.forceUnlock("key1");
        map.forceUnlock("key2");

    }

    @Test
    public void testPutTtl() throws Exception {
        map.put("key1", "value1", 1, TimeUnit.SECONDS);
        assertNotNull(map.get("key1"));
        Thread.sleep(2000);
        assertNull(map.get("key1"));
    }

    @Test
    public void testPutIfAbsent() throws Exception {
        assertNull(map.putIfAbsent("key1", "value1"));
        assertEquals("value1", map.putIfAbsent("key1", "value3"));
    }

    @Test
    public void testPutIfAbsentTtl() throws Exception {
        assertNull(map.putIfAbsent("key1", "value1", 1, TimeUnit.SECONDS));
        assertEquals("value1", map.putIfAbsent("key1", "value3", 1, TimeUnit.SECONDS));
        Thread.sleep(2000);
        assertNull(map.putIfAbsent("key1", "value3", 1, TimeUnit.SECONDS));
        assertEquals("value3", map.putIfAbsent("key1", "value4", 1, TimeUnit.SECONDS));
        Thread.sleep(2000);
    }

    @Test
    public void testSet() throws Exception {
        map.set("key1", "value1");
        assertEquals("value1", map.get("key1"));

        map.set("key1", "value2");
        assertEquals("value2", map.get("key1"));

        map.set("key1", "value3", 1, TimeUnit.SECONDS);
        assertEquals("value3", map.get("key1"));

        Thread.sleep(2000);
        assertNull(map.get("key1"));

    }

    @Test
    public void testPutTransient() throws Exception {
        //TODO mapstore
    }

    @Test
    public void testLock() throws Exception {
        map.put("key1", "value1");
        assertEquals("value1", map.get("key1"));
        map.lock("key1");
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread(){
            public void run() {
                map.tryPut("key1","value2", 1, TimeUnit.SECONDS);
                latch.countDown();
            }
        }.start();
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals("value1", map.get("key1"));
        map.forceUnlock("key1");
    }

    @Test
    public void testLockTtl() throws Exception {
        map.put("key1", "value1");
        assertEquals("value1", map.get("key1"));
        map.lock("key1", 2, TimeUnit.SECONDS);
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread(){
            public void run() {
                map.tryPut("key1","value2", 5, TimeUnit.SECONDS);
                latch.countDown();
            }
        }.start();
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertFalse(map.isLocked("key1"));
        assertEquals("value2", map.get("key1"));
        map.forceUnlock("key1");
    }

    @Test
    public void testLockTtl2() throws Exception {
        map.lock("key1", 3, TimeUnit.SECONDS);
        final CountDownLatch latch = new CountDownLatch(2);
        new Thread() {
            public void run() {
                if (!map.tryLock("key1")) {
                    latch.countDown();
                }
                try {
                    if (map.tryLock("key1", 5, TimeUnit.SECONDS)) {
                        latch.countDown();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }.start();
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        map.forceUnlock("key1");
    }

    @Test
    public void testTryLock() throws Exception {
//        final IMap tempMap = server.getMap(name);
        final IMap tempMap = map;

        assertTrue(tempMap.tryLock("key1", 2, TimeUnit.SECONDS));
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread(){
            public void run() {
                try {
                    if(!tempMap.tryLock("key1", 2, TimeUnit.SECONDS)){
                        latch.countDown();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }.start();
        assertTrue(latch.await(100, TimeUnit.SECONDS));

        assertTrue(tempMap.isLocked("key1"));

        final CountDownLatch latch2 = new CountDownLatch(1);
        new Thread(){
            public void run() {
                try {
                    if(tempMap.tryLock("key1", 20, TimeUnit.SECONDS)){
                        latch2.countDown();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }.start();
        Thread.sleep(1000);
        tempMap.unlock("key1");
        assertTrue(latch2.await(100, TimeUnit.SECONDS));
        assertTrue(tempMap.isLocked("key1"));
        tempMap.forceUnlock("key1");
    }

    @Test
    public void testForceUnlock() throws Exception {
        map.lock("key1");
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread(){
            public void run() {
                map.forceUnlock("key1");
                latch.countDown();
            }
        }.start();
        assertTrue(latch.await(100, TimeUnit.SECONDS));
        assertFalse(map.isLocked("key1"));
    }

    @Test
    public void testValues() {
        fillMap();

        final Collection values = map.values(new SqlPredicate("this == value1"));
        assertEquals(1, values.size());
        assertEquals("value1", values.iterator().next());
    }

    @Test
    public void testReplace() throws Exception {
        assertNull(map.replace("key1","value1"));
        map.put("key1","value1");
        assertEquals("value1", map.replace("key1","value2"));
        assertEquals("value2", map.get("key1"));

        assertFalse(map.replace("key1", "value1", "value3"));
        assertEquals("value2", map.get("key1"));
        assertTrue(map.replace("key1", "value2", "value3"));
        assertEquals("value3", map.get("key1"));
    }

    @Test
    public void testListener() throws InterruptedException {
        final CountDownLatch latch1Add = new CountDownLatch(5);
        final CountDownLatch latch1Remove = new CountDownLatch(2);

        final CountDownLatch latch2Add = new CountDownLatch(1);
        final CountDownLatch latch2Remove = new CountDownLatch(1);

        EntryListener listener1 = new EntryAdapter() {
            public void entryAdded(EntryEvent event) {
                latch1Add.countDown();
            }
            public void entryRemoved(EntryEvent event) {
                latch1Remove.countDown();
            }
        };

        EntryListener listener2 = new EntryAdapter() {
            public void entryAdded(EntryEvent event) {
                latch2Add.countDown();
            }
            public void entryRemoved(EntryEvent event) {
                latch2Remove.countDown();
            }
        };

        map.addEntryListener(listener1, false);
        map.addEntryListener(listener2, "key3", true);

        Thread.sleep(1000);

        map.put("key1", "value1");
        map.put("key2", "value2");
        map.put("key3", "value3");
        map.put("key4", "value4");
        map.put("key5", "value5");

        map.remove("key1");
        map.remove("key3");

        assertTrue(latch1Add.await(10, TimeUnit.SECONDS));
        assertTrue(latch1Remove.await(10, TimeUnit.SECONDS));
        assertTrue(latch2Add.await(5, TimeUnit.SECONDS));
        assertTrue(latch2Remove.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void testBasicPredicate(){
        fillMap();
        final Collection collection = map.values(new SqlPredicate("this == value1"));
        assertEquals("value1", collection.iterator().next());
        final Set set = map.keySet(new SqlPredicate("this == value1"));
        assertEquals("key1", set.iterator().next());
        final Set<Map.Entry<String, String>> set1 = map.entrySet(new SqlPredicate("this == value1"));
        assertEquals("key1",set1.iterator().next().getKey());
        assertEquals("value1",set1.iterator().next().getValue());
    }

    private void fillMap(){
        for (int i=0; i<10; i++){
            map.put("key" + i, "value" + i);
        }
    }

}
