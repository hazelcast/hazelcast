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

    private static final String name = "test";

    private static ISet<String> set;

    @BeforeClass
    public static void beforeClass(){
        Config config = new Config();
        Hazelcast.newHazelcastInstance(config);
        HazelcastInstance client = HazelcastClient.newHazelcastClient(null);
        set = client.getSet(name);
    }

    @AfterClass
    public static void afterClass() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Before
    @After
    public void reset() throws IOException {
        set.clear();
    }

    @Test
    public void testAddAll() {
        List<String> tempList = new ArrayList<String>();
        tempList.add("item1");
        tempList.add("item2");

        assertTrue(set.addAll(tempList));
        assertEquals(2, set.size());

        assertFalse(set.addAll(tempList));
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

        Iterator iterator = set.iterator();
        assertTrue(((String)iterator.next()).startsWith("item"));
        assertTrue(((String)iterator.next()).startsWith("item"));
        assertTrue(((String)iterator.next()).startsWith("item"));
        assertTrue(((String)iterator.next()).startsWith("item"));
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testContains(){
        assertTrue(set.add("item1"));
        assertTrue(set.add("item2"));
        assertTrue(set.add("item3"));
        assertTrue(set.add("item4"));

        assertFalse(set.contains("item5"));
        assertTrue(set.contains("item2"));

        List<String> tempList = new ArrayList<String>();
        tempList.add("item6");
        tempList.add("item3");

        assertFalse(set.containsAll(tempList));
        assertTrue(set.add("item6"));
        assertTrue(set.containsAll(tempList));
    }

    @Test
    public void removeRetainAll(){
        assertTrue(set.add("item1"));
        assertTrue(set.add("item2"));
        assertTrue(set.add("item3"));
        assertTrue(set.add("item4"));

        List<String> tempList = new ArrayList<String>();
        tempList.add("item4");
        tempList.add("item3");

        assertTrue(set.removeAll(tempList));
        assertEquals(2, set.size());
        assertFalse(set.removeAll(tempList));
        assertEquals(2, set.size());

        tempList.clear();
        tempList.add("item1");
        tempList.add("item2");
        assertFalse(set.retainAll(tempList));
        assertEquals(2, set.size());

        tempList.clear();
        assertTrue(set.retainAll(tempList));
        assertEquals(0, set.size());
    }

    @Test
    public void testListener() throws Exception {
        final CountDownLatch latch = new CountDownLatch(6);

        ItemListener<String> listener = new ItemListener<String>() {
            public void itemAdded(ItemEvent itemEvent) {
                latch.countDown();
            }

            public void itemRemoved(ItemEvent item) {
            }
        };
        String listenerID = set.addItemListener(listener, true);

        new Thread(){
            public void run() {
                for (int i=0; i<5; i++){
                    set.add("item" + i);
                }
                set.add("done");
            }
        }.start();
        assertTrue(latch.await(20, TimeUnit.SECONDS));

        set.removeItemListener(listenerID);
    }
}
