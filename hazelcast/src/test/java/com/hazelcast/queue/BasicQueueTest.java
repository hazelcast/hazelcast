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

package com.hazelcast.queue;

import com.hazelcast.config.Config;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class BasicQueueTest extends AbstractQueueTest {


    // ================ offer ==============================

    @Test
    public void testOffer() throws Exception {
        int count = 100;
        IQueue<String> queue = newQueue();
        for (int i = 0; i < count; i++) {
            queue.offer("item" + i);
        }

        assertEquals(100, queue.size());
    }

    @Test
    public void testOffer_whenNoCapacity() {
        final String name = randomString();
        int count = 100;
        Config config = new Config();
        QueueConfig queueConfig = config.getQueueConfig(name);
        queueConfig.setMaxSize(count);
        HazelcastInstance instance = createHazelcastInstance(config);
        IQueue<String> queue = instance.getQueue(name);
        for (int i = 0; i < count; i++) {
            queue.offer("item" + i);
        }

        boolean accepted = queue.offer("rejected");
        assertFalse(accepted);
        assertEquals(count, queue.size());
    }

    @Test
    public void testOffer_whenNullArgument() {
        IQueue<String> queue = newQueue();
        try {
            queue.offer(null);
            fail();
        } catch (NullPointerException expected) {
        }

        assertTrue(queue.isEmpty());
    }

    @Test
    public void testOfferWithTimeout() throws InterruptedException {
        final String name = randomString();
        Config config = new Config();
        final int count = 100;
        config.getQueueConfig(name).setMaxSize(count);
        HazelcastInstance instance = createHazelcastInstance(config);
        final IQueue<String> queue = instance.getQueue(name);
        OfferThread offerThread = new OfferThread(queue);
        for (int i = 0; i < 100; i++) {
            queue.offer("item" + i);
        }

        assertFalse(queue.offer("rejected"));
        offerThread.start();
        queue.poll();
        assertSizeEventually(100, queue);
        assertTrue(queue.contains("waiting"));
    }

    // ================ poll ==============================

    @Test
    public void testPoll() {
        int count = 100;
        IQueue<String> queue = newQueue();
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
        IQueue<String> queue = newQueue();
        assertNull(queue.poll());
    }

    // ================ poll with timeout ==============================


    @Test
    public void testPollWithTimeout() throws Exception {
        final IQueue<String> queue = newQueue();
        PollThread pollThread = new PollThread(queue);

        pollThread.start();
        assertTrue(queue.offer("offer"));
        assertSizeEventually(0, queue);
        //todo: you need to check which item you got.
    }

    // ================ remove ==============================

    @Test
    public void testRemove() {
        IQueue<String> queue = newQueue();
        for (int i = 0; i < 10; i++) {
            queue.offer("item" + i);
        }

        assertTrue(queue.remove("item4"));
        assertEquals(queue.size(), 9);
    }

    @Test
    public void testRemove_whenElementNotExists() {
        IQueue<String> queue = newQueue();
        for (int i = 0; i < 10; i++) {
            queue.offer("item" + i);
        }

        assertFalse(queue.remove("item13"));
        assertEquals(10, queue.size());
    }

    @Test
    public void testRemove_whenQueueEmpty() {
        IQueue<String> queue = newQueue();
        assertFalse(queue.remove("not in Queue"));
    }

    @Test
    public void testRemove_whenArgNull() {
        IQueue<String> queue = newQueue();
        queue.add("foo");

        try {
            queue.remove(null);
            fail();
        } catch (NullPointerException expected) {
        }

        assertEquals(1, queue.size());
    }

    // ================ drainTo ==============================

    @Test
    public void testDrainTo() {
        IQueue<String> queue = newQueue();
        for (int i = 0; i < 10; i++) {
            queue.offer("item" + i);
        }
        List list = new ArrayList(10);

        assertEquals(10, queue.drainTo(list));
        assertEquals(10, list.size());
        assertEquals("item0", list.get(0));
        assertEquals("item5", list.get(5));
        assertEquals(0, queue.size());
    }

    @Test
    public void testDrainTo_whenQueueEmpty() {
        IQueue<String> queue = newQueue();
        List list = new ArrayList();

        assertEquals(0, queue.drainTo(list));
    }

    @Test
    public void testDrainTo_whenCollectionNull() {
        IQueue<String> queue = newQueue();
        for (int i = 0; i < 10; i++) {
            queue.offer("item" + i);
        }

        try {
            queue.drainTo(null);
            fail();
        } catch (NullPointerException expected) {
        }
        assertEquals(0, queue.size());
    }

    @Test
    public void testDrainToWithMaxElement() {
        IQueue<String> queue = newQueue();
        for (int i = 0; i < 10; i++) {
            queue.offer("item" + i);
        }
        List list = new ArrayList(10);

        queue.drainTo(list, 4);
        assertEquals(4, list.size());
        assertTrue(list.contains("item3"));
        assertEquals(6, queue.size());
    }

    // TODO: There is a bug here because the items are now lost if the collection is null.
    // We should immediately check the collection when you call 'drainTo'.
    @Test
    public void testDrainToWithMaxElement_whenCollectionNull() {
        IQueue<String> queue = newQueue();
        for (int i = 0; i < 10; i++) {
            queue.offer("item" + i);
        }

        try {
            queue.drainTo(null, 4);
            fail();
        } catch (NullPointerException expected) {
        }

        assertEquals(6, queue.size());
    }

    @Test
    public void testDrainToWithMaxElement_whenMaxArgNegative() {
        IQueue<String> queue = newQueue();
        for (int i = 0; i < 10; i++) {
            queue.offer("item" + i);
        }
        List list = new ArrayList();

        assertEquals(10, queue.drainTo(list, -4));
        assertEquals(0, queue.size());
    }

    // ================ contains ==============================

    @Test
    public void testContains_whenExists() {
        IQueue<String> queue = newQueue();
        for (int i = 0; i < 10; i++) {
            queue.offer("item" + i);
        }

        assertTrue(queue.contains("item4"));
        assertTrue(queue.contains("item8"));
    }

    @Test
    public void testContains_whenNotExists() {
        IQueue<String> queue = newQueue();
        for (int i = 0; i < 10; i++) {
            queue.offer("item" + i);
        }

        assertFalse(queue.contains("item10"));
        assertFalse(queue.contains("item19"));
    }

    // ================ containsAll ==============================

    @Test
    public void testAddAll_whenCollectionContainsNull() {
        HazelcastInstance instance = createHazelcastInstance();
        IQueue<String> queue = instance.getQueue(randomString());
        for (int i = 0; i < 10; i++) {
            queue.offer("item" + i);
        }
        List<String> list = new ArrayList<String>();
        list.add("item10");
        list.add(null);

        try {
            queue.addAll(list);
            fail();
        } catch (NullPointerException e) {
        }
    }

    public void testContainsAll_whenExists() {
        IQueue<String> queue = newQueue();
        for (int i = 0; i < 10; i++) {
            queue.offer("item" + i);
        }

        List<String> list = new ArrayList<String>();
        list.add("item1");
        list.add("item2");
        list.add("item3");
        assertTrue(queue.containsAll(list));
    }

    @Test
    public void testContainsAll_whenNoneExists() {
        IQueue<String> queue = newQueue();
        for (int i = 0; i < 10; i++) {
            queue.offer("item" + i);
        }

        List<String> list = new ArrayList<String>();
        list.add("item10");
        list.add("item11");
        list.add("item12");
        assertFalse(queue.containsAll(list));
    }

    @Test
    public void testContainsAll_whenSomeExists() {
        IQueue<String> queue = newQueue();
        for (int i = 0; i < 10; i++) {
            queue.offer("item" + i);
        }

        List<String> list = new ArrayList<String>();
        list.add("item1");
        list.add("item2");
        list.add("item14");
        list.add("item13");
        assertFalse(queue.containsAll(list));
    }

    @Test(expected = NullPointerException.class)
    public void testContainsAll_whenNull() {
        IQueue<String> queue = newQueue();
        for (int i = 0; i < 10; i++) {
            queue.offer("item" + i);
        }
        queue.containsAll(null);
    }

    // ================ addAll ==============================

    @Test
    public void testAddAll() {
        IQueue<String> queue = newQueue();
        List<String> list = new ArrayList<String>();
        for (int i = 0; i < 10; i++) {
            list.add("item" + i);
        }

        assertTrue(queue.addAll(list));
        assertEquals(queue.size(), 10);
    }

    @Test
    public void testAddAll_whenNullCollection() {
        IQueue<String> queue = newQueue();

        try {
            queue.addAll(null);
            fail();
        } catch (NullPointerException expected) {
        }

        assertEquals(0, queue.size());
    }

    @Test
    public void testAddAll_whenEmptyCollection() {
        IQueue<String> queue = newQueue();
        for (int i = 0; i < 10; i++) {
            queue.offer("item" + i);
        }
        List<String> list = new ArrayList<String>();

        assertEquals(10, queue.size());
        assertTrue(queue.addAll(list));
        assertEquals(10, queue.size());
    }

    @Test
    public void testAddAll_whenDuplicateItems() {
        IQueue<String> queue = newQueue();
        for (int i = 0; i < 10; i++) {
            queue.offer("item" + i);
        }
        List<String> list = new ArrayList<String>();
        list.add("item3");

        assertTrue(queue.contains("item3"));
        queue.addAll(list);
        assertEquals(11, queue.size());
    }

    // ================ retainAll ==============================

    @Test
    public void testRetainAll() {
        IQueue<String> queue = newQueue();
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
    public void testRetainAll_whenCollectionNull() {
        IQueue<String> queue = newQueue();
        queue.add("item3");
        queue.add("item4");
        queue.add("item5");

        try {
            queue.retainAll(null);
            fail();
        } catch (NullPointerException e) {
        }
        assertEquals(3, queue.size());
    }

    @Test
    public void testRetainAll_whenCollectionEmpty() {
        IQueue<String> queue = newQueue();
        queue.add("item3");
        queue.add("item4");
        queue.add("item5");
        List list = new ArrayList();

        assertTrue(queue.retainAll(list));
        assertEquals(0, queue.size());
    }

    @Test
    public void testRetainAll_whenCollectionContainsNull() {
        IQueue<String> queue = newQueue();
        queue.add("item3");
        queue.add("item4");
        queue.add("item5");
        List list = new ArrayList();
        list.add(null);

        assertTrue(queue.retainAll(list));
        assertEquals(0, queue.size());
    }

    // ================ removeAll ==============================

    @Test
    public void testRemoveAll() {
        IQueue<String> queue = newQueue();
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

    @Test
    public void testRemoveAll_whenCollectionNull() {
        IQueue<String> queue = newQueue();
        queue.add("item3");
        queue.add("item4");
        queue.add("item5");

        try {
            queue.removeAll(null);
            fail();
        } catch (NullPointerException expected) {
        }

        assertEquals(3, queue.size());
    }

    @Test
    public void testRemoveAll_whenCollectionEmpty() {
        IQueue<String> queue = newQueue();
        queue.add("item3");
        queue.add("item4");
        queue.add("item5");
        List<String> list = new ArrayList<String>();

        assertFalse(queue.removeAll(list));
        assertEquals(3, queue.size());
    }

    // ================ toArray ==============================

    @Test
    public void testToArray() {
        final String name = randomString();
        HazelcastInstance instance = createHazelcastInstance();
        IQueue<String> queue = instance.getQueue(name);
        for (int i = 0; i < 10; i++) {
            queue.offer("item" + i);
        }

        Object[] array = queue.toArray();
        for (int i = 0; i < array.length; i++) {
            Object o = array[i];
            assertEquals(o, "item" + i++);
        }
        String[] arr = new String[5];
        IQueue<String> q = instance.getQueue(name);
        arr = q.toArray(arr);
        assertEquals(arr.length, 10);
        for (int i = 0; i < arr.length; i++) {
            Object o = arr[i];
            assertEquals(o, "item" + i++);
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testQueueRemoveFromIterator() {
        IQueue<String> queue = createHazelcastInstance().getQueue(randomString());
        queue.add("one");
        Iterator<String> iterator = queue.iterator();
        iterator.next();
        iterator.remove();
    }

    private class OfferThread extends Thread {
        IQueue queue;

        OfferThread(IQueue queue) {
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

    private class PollThread extends Thread {
        IQueue queue;

        PollThread(IQueue queue) {
            this.queue = queue;
        }

        @Override
        public void run() {
            try {
                queue.poll(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
