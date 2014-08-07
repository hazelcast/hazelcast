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

package com.hazelcast.client.collections;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.Config;
import com.hazelcast.core.*;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

/**
 * @author ali 5/20/13
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientSetTest {

    static final String name = "test";
    static HazelcastInstance hz;
    static HazelcastInstance server;
    static ISet set;

    @BeforeClass
    public static void init(){
        Config config = new Config();
        server = Hazelcast.newHazelcastInstance(config);
        hz = HazelcastClient.newHazelcastClient(null);
        set = hz.getSet(name);
    }

    @AfterClass
    public static void destroy() {
        hz.shutdown();
        Hazelcast.shutdownAll();
    }

    @Before
    @After
    public void clear() throws IOException {
        set.clear();
    }

    @Test
    public void testAddAll() {
        List l = new ArrayList();
        l.add("item1");
        l.add("item2");

        assertTrue(set.addAll(l));
        assertEquals(2, set.size());

        assertFalse(set.addAll(l));
        assertEquals(2, set.size());

    }

    @Test
    public void testAddRemove() {
        assertTrue(set.add("item1"));
        assertTrue(set.add("item2"));
        assertTrue(set.add("item3"));
        assertEquals(3, set.size());

        assertFalse(set.add("item3"));
        assertEquals(3, set.size());


        assertFalse(set.remove("item4"));
        assertTrue(set.remove("item3"));

    }

    @Test
    public void testIterator(){
        assertTrue(set.add("item1"));
        assertTrue(set.add("item2"));
        assertTrue(set.add("item3"));
        assertTrue(set.add("item4"));

        Iterator iter = set.iterator();
        assertTrue(((String)iter.next()).startsWith("item"));
        assertTrue(((String)iter.next()).startsWith("item"));
        assertTrue(((String)iter.next()).startsWith("item"));
        assertTrue(((String)iter.next()).startsWith("item"));
        assertFalse(iter.hasNext());
    }

    @Test
    public void testContains(){
        assertTrue(set.add("item1"));
        assertTrue(set.add("item2"));
        assertTrue(set.add("item3"));
        assertTrue(set.add("item4"));

        assertFalse(set.contains("item5"));
        assertTrue(set.contains("item2"));

        List l = new ArrayList();
        l.add("item6");
        l.add("item3");

        assertFalse(set.containsAll(l));
        assertTrue(set.add("item6"));
        assertTrue(set.containsAll(l));
    }

    @Test
    public void removeRetainAll(){
        assertTrue(set.add("item1"));
        assertTrue(set.add("item2"));
        assertTrue(set.add("item3"));
        assertTrue(set.add("item4"));

        List l = new ArrayList();
        l.add("item4");
        l.add("item3");

        assertTrue(set.removeAll(l));
        assertEquals(2, set.size());
        assertFalse(set.removeAll(l));
        assertEquals(2, set.size());

        l.clear();
        l.add("item1");
        l.add("item2");
        assertFalse(set.retainAll(l));
        assertEquals(2, set.size());

        l.clear();
        assertTrue(set.retainAll(l));
        assertEquals(0, set.size());

    }

    @Test
    public void testListener() throws Exception {

//        final ISet tempSet = server.getSet(name);
        final ISet tempSet = set;

        final CountDownLatch latch = new CountDownLatch(6);

        ItemListener listener = new ItemListener() {

            public void itemAdded(ItemEvent itemEvent) {
                latch.countDown();
            }

            public void itemRemoved(ItemEvent item) {
            }
        };
        String registrationId = tempSet.addItemListener(listener, true);

        new Thread(){
            public void run() {
                for (int i=0; i<5; i++){
                    tempSet.add("item" + i);
                }
                tempSet.add("done");
            }
        }.start();
        assertTrue(latch.await(20, TimeUnit.SECONDS));

    }

    @Test
    public void testIsEmpty() {
        assertTrue(set.isEmpty());
        assertEquals(0,set.size());
    }

    @Test
    public void testNotIsEmpty() {
        set.add("item");
        assertFalse(set.isEmpty());
        assertEquals(1,set.size());
    }
}
