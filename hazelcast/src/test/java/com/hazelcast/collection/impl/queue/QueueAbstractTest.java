/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection.impl.queue;

import com.hazelcast.config.Config;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.collection.IQueue;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.test.AbstractHazelcastClassRunner.getTestMethodName;
import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class QueueAbstractTest extends HazelcastTestSupport {

    protected HazelcastInstance[] instances;
    protected IAtomicLong atomicLong;
    private IQueue<String> queue;
    private QueueConfig queueConfig;

    @Before
    public void setup() {
        Config config = new Config();
        config.addQueueConfig(new QueueConfig("testOffer_whenFull*").setMaxSize(100));
        config.addQueueConfig(new QueueConfig("testOfferWithTimeout*").setMaxSize(100));

        instances = newInstances(config);
        HazelcastInstance local = instances[0];
        HazelcastInstance target = instances[instances.length - 1];
        String methodName = getTestMethodName();
        String name = randomNameOwnedBy(target, methodName);
        queueConfig = config.getQueueConfig(name);
        queue = local.getQueue(name);
    }

    protected abstract HazelcastInstance[] newInstances(Config config);

    // ================ offer ==============================

    @Test
    public void testOffer() throws Exception {
        int count = 100;
        for (int i = 0; i < count; i++) {
            queue.offer("item" + i);
        }

        assertEquals(100, queue.size());
    }

    @Test
    public void testOffer_whenFull() {
        for (int i = 0; i < queueConfig.getMaxSize(); i++) {
            queue.offer("item" + i);
        }

        boolean accepted = queue.offer("rejected");
        assertFalse(accepted);
        assertEquals(queueConfig.getMaxSize(), queue.size());
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void testOffer_whenNullArgument() {
        try {
            queue.offer(null);
            fail();
        } catch (NullPointerException expected) {
            ignore(expected);
        }

        assertTrue(queue.isEmpty());
    }

    @Test
    public void testOfferWithTimeout() {
        OfferThread offerThread = new OfferThread(queue);
        for (int i = 0; i < queueConfig.getMaxSize(); i++) {
            queue.offer("item" + i);
        }

        assertFalse(queue.offer("rejected"));
        offerThread.start();
        queue.poll();
        assertSizeEventually(queueConfig.getMaxSize(), queue);
        assertContains(queue, "waiting");
    }

    // ================ poll ==============================

    @Test
    public void testPoll() {
        int count = 100;
        for (int i = 0; i < count; i++) {
            queue.offer("item" + i);
        }

        queue.poll();
        queue.poll();
        queue.poll();
        queue.poll();
        assertEquals(96, queue.size());
    }

    @Test
    public void testPoll_whenQueueEmpty() {
        assertNull(queue.poll());
    }

    // ================ poll with timeout ==============================


    @Test
    public void testPollWithTimeout() throws Exception {
        PollThread pollThread = new PollThread(queue);
        pollThread.start();
        queue.offer("offer");
        queue.offer("remain");

        assertSizeEventually(1, queue);
        assertContains(queue, "remain");
    }

    // ================ remove ==============================

    @Test
    public void testRemove() {
        for (int i = 0; i < 10; i++) {
            queue.offer("item" + i);
        }

        assertTrue(queue.remove("item4"));
        assertEquals(queue.size(), 9);
    }

    @Test
    public void testRemove_whenElementNotExists() {
        for (int i = 0; i < 10; i++) {
            queue.offer("item" + i);
        }

        assertFalse(queue.remove("item13"));
        assertEquals(10, queue.size());
    }

    @Test
    public void testRemove_whenQueueEmpty() {
        assertFalse(queue.remove("not in Queue"));
    }

    @Test
    public void testRemove_whenArgNull() {
        queue.add("foo");

        try {
            queue.remove(null);
            fail();
        } catch (NullPointerException expected) {
            ignore(expected);
        }

        assertEquals(1, queue.size());
    }

    // ================ drainTo ==============================

    @Test
    public void testDrainTo() {
        for (int i = 0; i < 10; i++) {
            queue.offer("item" + i);
        }
        List<String> list = new ArrayList<String>(10);

        assertEquals(10, queue.drainTo(list));
        assertEquals(10, list.size());
        assertEquals("item0", list.get(0));
        assertEquals("item5", list.get(5));
        assertEquals(0, queue.size());
    }

    @Test
    public void testDrainTo_whenQueueEmpty() {
        List<String> list = new ArrayList<String>();

        assertEquals(0, queue.drainTo(list));
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void testDrainTo_whenCollectionNull() {
        for (int i = 0; i < 10; i++) {
            queue.offer("item" + i);
        }

        try {
            queue.drainTo(null);
            fail();
        } catch (NullPointerException expected) {
            ignore(expected);
        }
        assertEquals(10, queue.size());
    }

    @Test
    public void testDrainToWithMaxElement() {
        for (int i = 0; i < 10; i++) {
            queue.offer("item" + i);
        }
        List<String> list = new ArrayList<String>(10);

        queue.drainTo(list, 4);
        assertEquals(4, list.size());
        assertContains(list, "item3");
        assertEquals(6, queue.size());
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void testDrainToWithMaxElement_whenCollectionNull() {
        for (int i = 0; i < 10; i++) {
            queue.offer("item" + i);
        }

        try {
            queue.drainTo(null, 4);
            fail();
        } catch (NullPointerException expected) {
            ignore(expected);
        }

        assertEquals(10, queue.size());
    }

    @Test
    public void testDrainToWithMaxElement_whenMaxArgNegative() {
        for (int i = 0; i < 10; i++) {
            queue.offer("item" + i);
        }
        List<String> list = new ArrayList<String>();

        assertEquals(10, queue.drainTo(list, -4));
        assertEquals(0, queue.size());
    }

    // ================ contains ==============================

    @Test
    public void testContains_whenExists() {
        for (int i = 0; i < 10; i++) {
            queue.offer("item" + i);
        }

        assertContains(queue, "item4");
        assertContains(queue, "item8");
    }

    @Test
    public void testContains_whenNotExists() {
        for (int i = 0; i < 10; i++) {
            queue.offer("item" + i);
        }

        assertNotContains(queue, "item10");
        assertNotContains(queue, "item19");
    }

    // ================ containsAll ==============================

    @Test
    public void testAddAll_whenCollectionContainsNull() {
        for (int i = 0; i < 10; i++) {
            queue.offer("item" + i);
        }
        List<String> list = new ArrayList<String>();
        list.add("item10");
        list.add(null);

        try {
            queue.addAll(list);
            fail();
        } catch (NullPointerException expected) {
            ignore(expected);
        }
    }

    @Test
    public void testContainsAll_whenExists() {
        for (int i = 0; i < 10; i++) {
            queue.offer("item" + i);
        }

        List<String> list = new ArrayList<String>();
        list.add("item1");
        list.add("item2");
        list.add("item3");
        assertContainsAll(queue, list);
    }

    @Test
    public void testContainsAll_whenNoneExists() {
        for (int i = 0; i < 10; i++) {
            queue.offer("item" + i);
        }

        List<String> list = new ArrayList<String>();
        list.add("item10");
        list.add("item11");
        list.add("item12");
        assertNotContainsAll(queue, list);
    }

    @Test
    public void testContainsAll_whenSomeExists() {
        for (int i = 0; i < 10; i++) {
            queue.offer("item" + i);
        }

        List<String> list = new ArrayList<String>();
        list.add("item1");
        list.add("item2");
        list.add("item14");
        list.add("item13");
        assertNotContainsAll(queue, list);
    }

    @Test(expected = NullPointerException.class)
    @SuppressWarnings("ConstantConditions")
    public void testContainsAll_whenNull() {
        queue.containsAll(null);
    }

    // ================ addAll ==============================

    @Test
    public void testAddAll() {
        List<String> list = new ArrayList<String>();
        for (int i = 0; i < 10; i++) {
            list.add("item" + i);
        }

        assertTrue(queue.addAll(list));
        assertEquals(queue.size(), 10);
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void testAddAll_whenNullCollection() {
        try {
            queue.addAll(null);
            fail();
        } catch (NullPointerException expected) {
            ignore(expected);
        }

        assertEquals(0, queue.size());
    }

    @Test
    public void testAddAll_whenEmptyCollection() {
        for (int i = 0; i < 10; i++) {
            queue.offer("item" + i);
        }
        assertEquals(10, queue.size());
        assertTrue(queue.addAll(Collections.<String>emptyList()));
        assertEquals(10, queue.size());
    }

    @Test
    public void testAddAll_whenDuplicateItems() {
        for (int i = 0; i < 10; i++) {
            queue.offer("item" + i);
        }
        List<String> list = new ArrayList<String>();
        list.add("item3");

        assertContains(queue, "item3");
        queue.addAll(list);
        assertEquals(11, queue.size());
    }

    // ================ retainAll ==============================

    @Test
    public void testRetainAll() {
        queue.add("item3");
        queue.add("item4");
        queue.add("item5");

        List<String> arrayList = new ArrayList<String>();
        arrayList.add("item3");
        arrayList.add("item4");
        arrayList.add("item31");
        assertTrue(queue.retainAll(arrayList));
        assertEquals(queue.size(), 2);
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void testRetainAll_whenCollectionNull() {
        queue.add("item3");
        queue.add("item4");
        queue.add("item5");

        try {
            queue.retainAll(null);
            fail();
        } catch (NullPointerException expected) {
            ignore(expected);
        }
        assertEquals(3, queue.size());
    }

    @Test
    public void testRetainAll_whenCollectionEmpty() {
        queue.add("item3");
        queue.add("item4");
        queue.add("item5");

        assertTrue(queue.retainAll(emptyList()));
        assertEquals(0, queue.size());
    }

    @Test
    public void testRetainAll_whenCollectionContainsNull() {
        queue.add("item3");
        queue.add("item4");
        queue.add("item5");
        List<String> list = new ArrayList<String>();
        list.add(null);

        assertTrue(queue.retainAll(list));
        assertEquals(0, queue.size());
    }

    // ================ removeAll ==============================

    @Test
    public void testRemoveAll() {
        queue.add("item3");
        queue.add("item4");
        queue.add("item5");

        List<String> arrayList = new ArrayList<String>();
        arrayList.add("item3");
        arrayList.add("item4");
        arrayList.add("item5");
        assertTrue(queue.removeAll(arrayList));
        assertEquals(queue.size(), 0);
    }

    @Test(expected = NullPointerException.class)
    @SuppressWarnings("ConstantConditions")
    public void testRemoveAll_whenCollectionNull() {
        queue.removeAll(null);
    }

    @Test
    public void testRemoveAll_whenCollectionEmpty() {
        queue.add("item3");
        queue.add("item4");
        queue.add("item5");

        assertFalse(queue.removeAll(Collections.<String>emptyList()));
        assertEquals(3, queue.size());
    }

    // ================ toArray ==============================

    @Test
    public void testToArray() {
        for (int i = 0; i < 10; i++) {
            queue.offer("item" + i);
        }

        Object[] array = queue.toArray();
        for (int i = 0; i < array.length; i++) {
            Object o = array[i];
            assertEquals(o, "item" + i++);
        }
        String[] arr = new String[5];
        arr = queue.toArray(arr);
        assertEquals(arr.length, 10);
        for (int i = 0; i < arr.length; i++) {
            Object o = arr[i];
            assertEquals(o, "item" + i++);
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testQueueRemoveFromIterator() {
        queue.add("one");
        Iterator<String> iterator = queue.iterator();
        iterator.next();
        iterator.remove();
    }

    private static class OfferThread extends Thread {

        IQueue<String> queue;

        OfferThread(IQueue<String> queue) {
            this.queue = queue;
        }

        @Override
        public void run() {
            try {
                queue.offer("waiting", 1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static class PollThread extends Thread {

        IQueue queue;

        PollThread(IQueue queue) {
            this.queue = queue;
        }

        @Override
        public void run() {
            try {
                queue.poll(15, TimeUnit.SECONDS);

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
