/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.collection;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.Config;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.core.*;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.annotation.SerialTest;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @ali 5/20/13
 */
@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(SerialTest.class)
public class ClientMultiMapTest {

    static final String name = "test";
    static HazelcastInstance hz;
    static MultiMap mm;

    @BeforeClass
    public static void init() {
        Config config = new Config();
        MultiMapConfig multiMapConfig = config.getMultiMapConfig(name);
        multiMapConfig.setValueCollectionType(MultiMapConfig.ValueCollectionType.SET);
        Hazelcast.newHazelcastInstance(config);
        hz = HazelcastClient.newHazelcastClient(null);
        mm = hz.getMultiMap(name);
    }

    @AfterClass
    public static void destroy() {
        hz.getLifecycleService().shutdown();
        Hazelcast.shutdownAll();
    }

    @Before
    @After
    public void clear() throws IOException {
        mm.clear();
    }

    @Test
    public void testPutGetRemove() {
        assertTrue(mm.put("key1", "value1"));
        assertTrue(mm.put("key1", "value2"));
        assertTrue(mm.put("key1", "value3"));

        assertTrue(mm.put("key2", "value4"));
        assertTrue(mm.put("key2", "value5"));

        assertEquals(3, mm.valueCount("key1"));
        assertEquals(2, mm.valueCount("key2"));
        assertEquals(5, mm.size());

        Collection coll = mm.get("key1");
        assertEquals(3, coll.size());

        coll = mm.remove("key2");
        assertEquals(2, coll.size());
        assertEquals(0, mm.valueCount("key2"));
        assertEquals(0, mm.get("key2").size());

        assertFalse(mm.remove("key1", "value4"));
        assertEquals(3, mm.size());

        assertTrue(mm.remove("key1", "value2"));
        assertEquals(2, mm.size());

        assertTrue(mm.remove("key1", "value1"));
        assertEquals(1, mm.size());
        assertEquals("value3", mm.get("key1").iterator().next());
    }

    @Test
    public void testKeySetEntrySetAndValues() {
        assertTrue(mm.put("key1", "value1"));
        assertTrue(mm.put("key1", "value2"));
        assertTrue(mm.put("key1", "value3"));

        assertTrue(mm.put("key2", "value4"));
        assertTrue(mm.put("key2", "value5"));


        assertEquals(2, mm.keySet().size());
        assertEquals(5, mm.values().size());
        assertEquals(5, mm.entrySet().size());
    }

    @Test
    public void testContains() {
        assertTrue(mm.put("key1", "value1"));
        assertTrue(mm.put("key1", "value2"));
        assertTrue(mm.put("key1", "value3"));

        assertTrue(mm.put("key2", "value4"));
        assertTrue(mm.put("key2", "value5"));

        assertFalse(mm.containsKey("key3"));
        assertTrue(mm.containsKey("key1"));

        assertFalse(mm.containsValue("value6"));
        assertTrue(mm.containsValue("value4"));

        assertFalse(mm.containsEntry("key1", "value4"));
        assertFalse(mm.containsEntry("key2", "value3"));
        assertTrue(mm.containsEntry("key1", "value1"));
        assertTrue(mm.containsEntry("key2", "value5"));
    }

    @Test
    public void testListener() throws InterruptedException {
        final CountDownLatch latch1Add = new CountDownLatch(8);
        final CountDownLatch latch1Remove = new CountDownLatch(4);

        final CountDownLatch latch2Add = new CountDownLatch(3);
        final CountDownLatch latch2Remove = new CountDownLatch(3);

        EntryListener listener1 = new EntryListener() {

            public void entryAdded(EntryEvent event) {
                latch1Add.countDown();
            }

            public void entryRemoved(EntryEvent event) {
                latch1Remove.countDown();
            }

            public void entryUpdated(EntryEvent event) {
            }

            public void entryEvicted(EntryEvent event) {
            }
        };

        EntryListener listener2 = new EntryListener() {

            public void entryAdded(EntryEvent event) {
                latch2Add.countDown();
            }

            public void entryRemoved(EntryEvent event) {
                latch2Remove.countDown();
            }

            public void entryUpdated(EntryEvent event) {
            }

            public void entryEvicted(EntryEvent event) {
            }
        };

        mm.addEntryListener(listener1, true);
        mm.addEntryListener(listener2, "key3", true);

        mm.put("key1", "value1");
        mm.put("key1", "value2");
        mm.put("key1", "value3");
        mm.put("key2", "value4");
        mm.put("key2", "value5");

        mm.remove("key1", "value2");

        mm.put("key3", "value6");
        mm.put("key3", "value7");
        mm.put("key3", "value8");

        mm.remove("key");

        assertTrue(latch1Add.await(20, TimeUnit.SECONDS));
        assertTrue(latch1Remove.await(20, TimeUnit.SECONDS));

        assertTrue(latch2Add.await(20, TimeUnit.SECONDS));
        assertTrue(latch2Remove.await(20, TimeUnit.SECONDS));
    }


    @Test
    public void testLock() throws Exception {
        mm.lock("key1");
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread() {
            public void run() {
                if (!mm.tryLock("key1")) {
                    latch.countDown();
                }
            }
        }.start();
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        mm.forceUnlock("key1");
    }

    @Test
    public void testLockTtl() throws Exception {
        mm.lock("key1", 3, TimeUnit.SECONDS);
        final CountDownLatch latch = new CountDownLatch(2);
        new Thread() {
            public void run() {
                if (!mm.tryLock("key1")) {
                    latch.countDown();
                }
                try {
                    if (mm.tryLock("key1", 5, TimeUnit.SECONDS)) {
                        latch.countDown();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }.start();
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        mm.forceUnlock("key1");
    }

    @Test
    public void testTryLock() throws Exception {

        assertTrue(mm.tryLock("key1", 2, TimeUnit.SECONDS));
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread(){
            public void run() {
                try {
                    if(!mm.tryLock("key1", 2, TimeUnit.SECONDS)){
                        latch.countDown();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }.start();
        assertTrue(latch.await(100, TimeUnit.SECONDS));

        assertTrue(mm.isLocked("key1"));

        final CountDownLatch latch2 = new CountDownLatch(1);
        new Thread(){
            public void run() {
                try {
                    if(mm.tryLock("key1", 20, TimeUnit.SECONDS)){
                        latch2.countDown();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }.start();
        Thread.sleep(1000);
        mm.unlock("key1");
        assertTrue(latch2.await(100, TimeUnit.SECONDS));
        assertTrue(mm.isLocked("key1"));
        mm.forceUnlock("key1");
    }

    @Test
    public void testForceUnlock() throws Exception {
        mm.lock("key1");
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread(){
            public void run() {
                mm.forceUnlock("key1");
                latch.countDown();
            }
        }.start();
        assertTrue(latch.await(100, TimeUnit.SECONDS));
        assertFalse(mm.isLocked("key1"));
    }
}
