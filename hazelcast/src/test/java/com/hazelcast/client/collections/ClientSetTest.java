/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.collection.ISet;
import com.hazelcast.collection.ItemEvent;
import com.hazelcast.collection.ItemListener;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
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
import java.util.Spliterator;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientSetTest extends HazelcastTestSupport {

    private TestHazelcastFactory hazelcastFactory;
    private ISet<String> set;

    @After
    public void tearDown() {
        hazelcastFactory.shutdownAll();
    }

    @Before
    public void setup() throws IOException {
        hazelcastFactory = new TestHazelcastFactory();
        hazelcastFactory.newHazelcastInstance();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient();
        set = client.getSet(randomString());
    }

    @Test
    public void testAddAll() {
        List<String> l = new ArrayList<String>();
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
    public void testSpliterator() {
        assertTrue(set.add("item1"));
        assertTrue(set.add("item2"));
        assertTrue(set.add("item3"));
        assertTrue(set.add("item4"));

        Spliterator spliterator = set.spliterator();

        ArrayList recorder = new ArrayList(set.size());
        Consumer consumer = value -> recorder.add(value);

        // tryAdvance.
        assertTrue(spliterator.tryAdvance(consumer));
        assertTrue(set.contains(recorder.get(0)));

        // forEachRemaining.
        spliterator.forEachRemaining(consumer);
        assertCollection(set, recorder);

        // There should be no more elements remaining in this spliterator.
        assertFalse(spliterator.tryAdvance(consumer));
        spliterator.forEachRemaining(item -> fail());

    }

    @Test
    public void testContains() {
        assertTrue(set.add("item1"));
        assertTrue(set.add("item2"));
        assertTrue(set.add("item3"));
        assertTrue(set.add("item4"));

        assertNotContains(set, "item5");
        assertContains(set, "item2");

        List<String> l = new ArrayList<String>();
        l.add("item6");
        l.add("item3");

        assertFalse(set.containsAll(l));
        assertTrue(set.add("item6"));
        assertContainsAll(set, l);
    }

    @Test
    public void removeRetainAll() {
        assertTrue(set.add("item1"));
        assertTrue(set.add("item2"));
        assertTrue(set.add("item3"));
        assertTrue(set.add("item4"));

        List<String> l = new ArrayList<String>();
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

        ItemListener<String> listener = new ItemListener<String>() {

            public void itemAdded(ItemEvent<String> itemEvent) {
                latch.countDown();
            }

            public void itemRemoved(ItemEvent<String> item) {
            }
        };
        UUID registrationId = set.addItemListener(listener, true);

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
