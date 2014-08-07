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
import java.util.ListIterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author ali 5/20/13
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientListTest {

    static final String name = "test";
    static HazelcastInstance hz;
    static IList list;

    @BeforeClass
    public static void init(){
        Config config = new Config();
        Hazelcast.newHazelcastInstance(config);
        hz = HazelcastClient.newHazelcastClient();
        list = hz.getList(name);
    }

    @AfterClass
    public static void destroy() {
        hz.shutdown();
        Hazelcast.shutdownAll();
    }

    @Before
    @After
    public void clear() throws IOException {
        list.clear();
    }

    @Test
    public void testAddAll() {
        List l = new ArrayList();
        l.add("item1");
        l.add("item2");

        assertTrue(list.addAll(l));
        assertEquals(2, list.size());

        assertTrue(list.addAll(1, l));
        assertEquals(4, list.size());

        assertEquals("item1", list.get(0));
        assertEquals("item1", list.get(1));
        assertEquals("item2", list.get(2));
        assertEquals("item2", list.get(3));
    }

    @Test
    public void testAddSetRemove() {
        assertTrue(list.add("item1"));
        assertTrue(list.add("item2"));
        list.add(0,"item3");
        assertEquals(3, list.size());
        Object o = list.set(2, "item4");
        assertEquals("item2", o);

        assertEquals(3, list.size());
        assertEquals("item3", list.get(0));
        assertEquals("item1", list.get(1));
        assertEquals("item4", list.get(2));

        assertFalse(list.remove("item2"));
        assertTrue(list.remove("item3"));

        o = list.remove(1);
        assertEquals("item4", o);

        assertEquals(1, list.size());
        assertEquals("item1", list.get(0));
    }

    @Test
    public void testIndexOf(){
        assertTrue(list.add("item1"));
        assertTrue(list.add("item2"));
        assertTrue(list.add("item1"));
        assertTrue(list.add("item4"));

        assertEquals(-1, list.indexOf("item5"));
        assertEquals(0, list.indexOf("item1"));

        assertEquals(-1, list.lastIndexOf("item6"));
        assertEquals(2, list.lastIndexOf("item1"));
    }

    @Test
    public void testIterator(){
        assertTrue(list.add("item1"));
        assertTrue(list.add("item2"));
        assertTrue(list.add("item1"));
        assertTrue(list.add("item4"));

        Iterator iter = list.iterator();
        assertEquals("item1",iter.next());
        assertEquals("item2",iter.next());
        assertEquals("item1",iter.next());
        assertEquals("item4",iter.next());
        assertFalse(iter.hasNext());

        ListIterator listIterator = list.listIterator(2);
        assertEquals("item1",listIterator.next());
        assertEquals("item4",listIterator.next());
        assertFalse(listIterator.hasNext());

        List l = list.subList(1, 3);
        assertEquals(2, l.size());
        assertEquals("item2", l.get(0));
        assertEquals("item1", l.get(1));
    }

    @Test
    public void testContains(){
        assertTrue(list.add("item1"));
        assertTrue(list.add("item2"));
        assertTrue(list.add("item1"));
        assertTrue(list.add("item4"));

        assertFalse(list.contains("item3"));
        assertTrue(list.contains("item2"));

        List l = new ArrayList();
        l.add("item4");
        l.add("item3");

        assertFalse(list.containsAll(l));
        assertTrue(list.add("item3"));
        assertTrue(list.containsAll(l));
    }

    @Test
    public void removeRetainAll(){
        assertTrue(list.add("item1"));
        assertTrue(list.add("item2"));
        assertTrue(list.add("item1"));
        assertTrue(list.add("item4"));

        List l = new ArrayList();
        l.add("item4");
        l.add("item3");

        assertTrue(list.removeAll(l));
        assertEquals(3, list.size());
        assertFalse(list.removeAll(l));
        assertEquals(3, list.size());

        l.clear();
        l.add("item1");
        l.add("item2");
        assertFalse(list.retainAll(l));
        assertEquals(3, list.size());

        l.clear();
        assertTrue(list.retainAll(l));
        assertEquals(0, list.size());

    }

    @Test
    public void testListener() throws Exception {

//        final ISet tempSet = server.getSet(name);
        final IList tempList = list;

        final CountDownLatch latch = new CountDownLatch(6);

        ItemListener listener = new ItemListener() {

            public void itemAdded(ItemEvent itemEvent) {
                latch.countDown();
            }

            public void itemRemoved(ItemEvent item) {
            }
        };
        String registrationId = tempList.addItemListener(listener, true);

        new Thread(){
            public void run() {
                for (int i=0; i<5; i++){
                    tempList.add("item" + i);
                }
                tempList.add("done");
            }
        }.start();
        assertTrue(latch.await(20, TimeUnit.SECONDS));

    }

    @Test
    public void testIsEmpty_whenEmpty() {
        assertTrue(list.isEmpty());
        assertEquals(0,list.size());
    }

    @Test
    public void testIsEmpty_whenNotEmpty() {
        list.add("item");
        assertFalse(list.isEmpty());
        assertEquals(1,list.size());
    }
}
