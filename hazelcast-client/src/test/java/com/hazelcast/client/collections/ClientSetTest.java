/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ISet;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemListener;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientSetTest extends HazelcastTestSupport {

    private TestHazelcastFactory hazelcastFactory;
    private HazelcastInstance client;
    private ISet set;

    @After
    public void tearDown() {
        hazelcastFactory.shutdownAll();
    }

    @Before
    public void setup() throws IOException {
        hazelcastFactory = new TestHazelcastFactory();
        hazelcastFactory.newHazelcastInstance();
        client = hazelcastFactory.newHazelcastClient();
        set = client.getSet(randomString());
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
    public void testIterator() {
        assertTrue(set.add("item1"));
        assertTrue(set.add("item2"));
        assertTrue(set.add("item3"));
        assertTrue(set.add("item4"));

        Iterator iter = set.iterator();
        assertTrue(((String) iter.next()).startsWith("item"));
        assertTrue(((String) iter.next()).startsWith("item"));
        assertTrue(((String) iter.next()).startsWith("item"));
        assertTrue(((String) iter.next()).startsWith("item"));
        assertFalse(iter.hasNext());
    }

    @Test
    public void testContains() {
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
    public void removeRetainAll() {
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

        final CountDownLatch latch = new CountDownLatch(6);

        ItemListener listener = new ItemListener() {

            public void itemAdded(ItemEvent itemEvent) {
                latch.countDown();
            }

            public void itemRemoved(ItemEvent item) {
            }
        };
        String registrationId = set.addItemListener(listener, true);

        new Thread() {
            public void run() {
                for (int i = 0; i < 5; i++) {
                    set.add("item" + i);
                }
                set.add("done");
            }
        }.start();
        assertTrue(latch.await(20, TimeUnit.SECONDS));
        set.removeItemListener(registrationId);

    }

    @Test
    public void testIsEmpty_whenEmpty() {
        assertTrue(set.isEmpty());
        assertEquals(0, set.size());
    }

    @Test
    public void testIsEmpty_whenNotEmpty() {
        set.add("item");
        assertFalse(set.isEmpty());
        assertEquals(1, set.size());
    }
}
