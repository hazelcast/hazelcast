/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.queue;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.collection.IQueue;
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
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientQueueTest extends HazelcastTestSupport {

    private static final int MAX_SIZE_FOR_QUEUE = 8;
    private static final String QUEUE_WITH_MAX_SIZE = "queueWithMaxSize*";

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();
    private HazelcastInstance client;

    @Before
    public void setup() {
        Config config = new Config();

        QueueConfig queueConfig = config.getQueueConfig(QUEUE_WITH_MAX_SIZE);
        queueConfig.setMaxSize(MAX_SIZE_FOR_QUEUE);

        hazelcastFactory.newHazelcastInstance(config);
        client = hazelcastFactory.newHazelcastClient();
    }

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testOffer() {
        IQueue<Integer> q = client.getQueue(randomString());
        assertTrue(q.offer(1));
        assertEquals(1, q.size());
    }

    @Test(expected = NullPointerException.class)
    @SuppressWarnings("ConstantConditions")
    public void testOffer_whenNullItem() {
        IQueue<Integer> q = client.getQueue(randomString());
        q.offer(null);
    }

    @Test(expected = NullPointerException.class)
    public void testOfferWithTimeout_whenNullItem() throws InterruptedException {
        IQueue<Integer> q = client.getQueue(randomString());
        q.offer(null, 1, TimeUnit.SECONDS);
    }

    @Test
    public void testAdd() {
        IQueue<Integer> q = client.getQueue(randomString());
        assertTrue(q.add(1));
        assertEquals(1, q.size());
    }

    @Test
    public void testAdd_whenReachingMaximumCapacity() {
        IQueue<Integer> q = client.getQueue(QUEUE_WITH_MAX_SIZE + randomString());
        for (int i = 0; i < MAX_SIZE_FOR_QUEUE; i++) {
            q.add(1);
        }
        assertEquals(MAX_SIZE_FOR_QUEUE, q.size());
    }

    @Test(expected = IllegalStateException.class)
    public void testAdd_whenExceedingMaximumCapacity() {
        IQueue<Integer> q = client.getQueue(QUEUE_WITH_MAX_SIZE + randomString());
        for (int i = 0; i < MAX_SIZE_FOR_QUEUE; i++) {
            q.add(1);
        }
        q.add(1);
    }

    @Test(expected = NullPointerException.class)
    public void testAdd_whenNullItem() {
        IQueue<Integer> q = client.getQueue(randomString());
        q.add(null);
    }

    @Test
    public void testPut() throws InterruptedException {
        IQueue<Integer> q = client.getQueue(randomString());
        q.put(1);
        assertEquals(1, q.size());
    }

    @Test(expected = NullPointerException.class)
    public void testPut_whenNullItem() throws InterruptedException {
        IQueue<Integer> q = client.getQueue(randomString());
        q.put(null);
    }

    @Test
    public void testTake() throws InterruptedException {
        IQueue<Integer> q = client.getQueue(randomString());
        q.put(1);
        assertEquals(1, q.take().intValue());
    }


    @Test(expected = InterruptedException.class)
    public void testTake_whenInterruptedWhileBlocking() throws InterruptedException {
        IQueue queue = client.getQueue(randomString());
        interruptCurrentThread(2000);

        queue.take();
    }

    @Test
    public void testEmptyPeak() throws InterruptedException {
        IQueue<Integer> q = client.getQueue(randomString());
        assertNull(q.peek());
    }

    @Test
    public void testPeak() throws InterruptedException {
        IQueue<Integer> q = client.getQueue(randomString());
        q.offer(1);
        assertEquals(1, q.peek().intValue());
        assertEquals(1, q.peek().intValue());
        assertEquals(1, q.size());
    }

    @Test(expected = NoSuchElementException.class)
    public void testEmptyElement() throws InterruptedException {
        IQueue<Integer> q = client.getQueue(randomString());
        q.element();
    }

    @Test
    public void testElement() throws InterruptedException {
        IQueue<Integer> q = client.getQueue(randomString());
        q.offer(1);
        assertEquals(1, q.element().intValue());
    }

    @Test
    public void testPoll() throws InterruptedException {
        IQueue<Integer> q = client.getQueue(randomString());
        q.offer(1);
        assertEquals(1, q.poll().intValue());
    }

    @Test(expected = InterruptedException.class)
    public void testPoll_whenInterruptedWhileBlocking() throws InterruptedException {
        IQueue queue = client.getQueue(randomString());
        interruptCurrentThread(2000);

        queue.poll(1, TimeUnit.MINUTES);
    }

    @Test
    public void testOfferWithTimeOut() throws IOException, InterruptedException {
        IQueue<Integer> q = client.getQueue(randomString());
        boolean result = q.offer(1, 50, TimeUnit.MILLISECONDS);
        assertTrue(result);
    }

    @Test
    public void testRemainingCapacity() throws IOException {
        final IQueue<String> q = client.getQueue(randomString());

        assertEquals(Integer.MAX_VALUE, q.remainingCapacity());
        q.offer("one");
        assertEquals(Integer.MAX_VALUE - 1, q.remainingCapacity());
    }

    @Test(expected = NoSuchElementException.class)
    public void testEmptyRemove() throws IOException {
        IQueue<Integer> q = client.getQueue(randomString());
        q.remove();
    }

    @Test
    public void testRemoveTop() throws IOException, InterruptedException {
        IQueue<Integer> q = client.getQueue(randomString());
        q.offer(1);
        assertEquals(1, q.remove().intValue());
    }

    @Test
    public void testRemove() throws IOException {
        IQueue<Integer> q = client.getQueue(randomString());

        q.offer(1);
        assertTrue(q.remove(1));
        assertFalse(q.remove(2));
    }

    @Test
    public void testContains() {
        IQueue<Integer> q = client.getQueue(randomString());

        q.offer(1);
        assertContains(q, 1);
        assertNotContains(q, 2);
    }

    @Test
    public void testContainsAll() {
        final int maxItems = 11;
        IQueue<Integer> q = client.getQueue(randomString());

        List<Integer> trueList = new ArrayList<Integer>();
        List<Integer> falseList = new ArrayList<Integer>();
        for (int i = 0; i < maxItems; i++) {
            q.offer(i);
            trueList.add(i);
            falseList.add(i + 1);
        }
        assertContainsAll(q, trueList);
        assertNotContainsAll(q, falseList);
    }

    @Test
    public void testDrain() {
        final int maxItems = 12;
        IQueue<Integer> q = client.getQueue(randomString());

        List<Integer> offeredList = new LinkedList<Integer>();
        for (int i = 0; i < maxItems; i++) {
            q.offer(i);
            offeredList.add(i);
        }
        List<Integer> drainedList = new LinkedList<Integer>();
        int totalDrained = q.drainTo(drainedList);

        assertEquals(maxItems, totalDrained);
        assertEquals(offeredList, drainedList);
    }

    @Test
    public void testPartialDrain() {
        final int maxItems = 15;
        final int itemsToDrain = maxItems / 2;

        IQueue<Integer> q = client.getQueue(randomString());

        List<Integer> expectedList = new LinkedList<Integer>();
        for (int i = 0; i < maxItems; i++) {
            q.offer(i);
            if (i < itemsToDrain) {
                expectedList.add(i);
            }
        }
        List<Integer> drainedList = new LinkedList<Integer>();
        int totalDrained = q.drainTo(drainedList, itemsToDrain);

        assertEquals(itemsToDrain, totalDrained);
        assertEquals(expectedList, drainedList);
    }

    @Test
    public void testIterator() {
        final int maxItems = 18;
        IQueue<Integer> q = client.getQueue(randomString());

        for (int i = 0; i < maxItems; i++) {
            q.offer(i);
        }

        int i = 0;
        for (Object o : q) {
            assertEquals(i++, o);
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testQueueRemoveFromIterator() {
        IQueue<String> queue = client.getQueue(randomString());
        queue.add("one");
        Iterator<String> iterator = queue.iterator();
        iterator.next();
        iterator.remove();
    }


    @Test
    public void testToArray() {
        final int maxItems = 19;
        IQueue<Integer> q = client.getQueue(randomString());

        Object[] offered = new Object[maxItems];
        for (int i = 0; i < maxItems; i++) {
            q.offer(i);
            offered[i] = i;
        }

        Object[] result = q.toArray();
        assertArrayEquals(offered, result);
    }

    @Test
    public void testToEmptyArray() {
        final int maxItems = 23;
        IQueue<Integer> q = client.getQueue(randomString());

        Object[] offered = new Object[maxItems];
        for (int i = 0; i < maxItems; i++) {
            q.offer(i);
            offered[i] = i;
        }

        Object[] result = q.toArray(new Object[0]);
        assertArrayEquals(offered, result);
    }

    @Test
    public void testToPreSizedArray() {
        final int maxItems = 74;
        IQueue<Integer> q = client.getQueue(randomString());

        Object[] offered = new Object[maxItems];
        for (int i = 0; i < maxItems; i++) {
            q.offer(i);
            offered[i] = i;
        }

        Object[] result = q.toArray(new Object[maxItems / 2]);
        assertArrayEquals(offered, result);
    }

    @Test
    public void testAddAll() throws IOException {
        final int maxItems = 13;
        IQueue<Integer> q = client.getQueue(randomString());

        Collection<Integer> coll = new ArrayList<Integer>(maxItems);
        for (int i = 0; i < maxItems; i++) {
            coll.add(i);
        }
        assertTrue(q.addAll(coll));
        assertEquals(coll.size(), q.size());

        // assert queue is same
        ArrayList<Integer> actual = new ArrayList<Integer>();
        actual.addAll(q);

        assertEquals(coll, actual);
    }

    @Test
    public void testRemoveList() throws IOException {
        final int maxItems = 131;
        IQueue<Integer> q = client.getQueue(randomString());

        List<Integer> removeList = new LinkedList<Integer>();
        for (int i = 0; i < maxItems; i++) {
            q.add(i);
            removeList.add(i);
        }

        assertTrue(q.removeAll(removeList));
        assertEquals(0, q.size());
    }

    @Test
    public void testRemoveList_whereNotFound() throws IOException {
        final int maxItems = 131;
        IQueue<Integer> q = client.getQueue(randomString());

        List<Integer> removeList = new LinkedList<Integer>();
        for (int i = 0; i < maxItems; i++) {
            q.add(i);
        }
        removeList.add(maxItems + 1);
        removeList.add(maxItems + 2);

        assertFalse(q.removeAll(removeList));
        assertEquals(maxItems, q.size());
    }

    @Test
    public void testRetainEmptyList() throws IOException {
        final int maxItems = 131;
        IQueue<Integer> q = client.getQueue(randomString());

        for (int i = 0; i < maxItems; i++) {
            q.add(i);
        }

        List<Integer> retain = emptyList();
        assertTrue(q.retainAll(retain));

        assertEquals(0, q.size());
    }

    @Test
    public void testRetainAllList() throws IOException {
        final int maxItems = 181;
        IQueue<Integer> q = client.getQueue(randomString());

        List<Integer> retain = new LinkedList<Integer>();
        for (int i = 0; i < maxItems; i++) {
            q.add(i);
            retain.add(i);
        }

        assertFalse(q.retainAll(retain));
        assertEquals(maxItems, q.size());
    }

    @Test
    public void testRetainAll_ListNotFound() throws IOException {
        final int maxItems = 181;
        IQueue<Integer> q = client.getQueue(randomString());

        List<Integer> retain = new LinkedList<Integer>();
        for (int i = 0; i < maxItems; i++) {
            q.add(i);
        }
        retain.add(maxItems + 1);
        retain.add(maxItems + 2);

        assertTrue(q.retainAll(retain));
        assertEquals(0, q.size());
    }

    @Test
    public void testRetainAll_mixedList() throws IOException {
        final int maxItems = 181;
        IQueue<Integer> q = client.getQueue(randomString());

        List<Integer> retain = new LinkedList<Integer>();
        for (int i = 0; i < maxItems; i++) {
            q.add(i);
        }
        retain.add(maxItems - 1);
        retain.add(maxItems + 1);

        assertTrue(q.retainAll(retain));
        assertEquals(1, q.size());
    }

    @Test
    public void testSize() {
        final int maxItems = 143;
        IQueue<Integer> q = client.getQueue(randomString());

        for (int i = 0; i < maxItems; i++) {
            q.add(i);
        }
        assertEquals(maxItems, q.size());
    }

    @Test
    public void testIsEmpty() {
        IQueue<Integer> q = client.getQueue(randomString());
        assertTrue(q.isEmpty());
    }

    @Test
    public void testNotIsEmpty() {
        IQueue<Integer> q = client.getQueue(randomString());
        q.offer(1);
        assertFalse(q.isEmpty());
    }

    @Test
    public void testClear() {
        final int maxItems = 123;
        IQueue<Integer> q = client.getQueue(randomString());

        for (int i = 0; i < maxItems; i++) {
            q.add(i);
        }

        assertEquals(maxItems, q.size());
        q.clear();
        assertEquals(0, q.size());
    }

    @Test
    public void testListener() throws Exception {
        final int maxItems = 10;
        final CountDownLatch itemAddedLatch = new CountDownLatch(maxItems);
        final CountDownLatch itemRemovedLatch = new CountDownLatch(maxItems);


        final IQueue<Integer> queue = client.getQueue(randomString());

        UUID id = queue.addItemListener(new ItemListener<Integer>() {

            public void itemAdded(ItemEvent<Integer> itemEvent) {
                itemAddedLatch.countDown();
            }

            public void itemRemoved(ItemEvent<Integer> item) {
                itemRemovedLatch.countDown();
            }
        }, true);

        new Thread() {
            public void run() {
                for (int i = 0; i < maxItems; i++) {
                    queue.offer(i);
                    queue.remove(i);
                }
            }
        }.start();

        assertTrue(itemAddedLatch.await(5, TimeUnit.SECONDS));
        assertTrue(itemRemovedLatch.await(5, TimeUnit.SECONDS));
        queue.removeItemListener(id);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetLocalQueueStats() {
        IQueue<Integer> q = client.getQueue(randomString());
        q.getLocalQueueStats();
    }

    @Test
    public void testOffer_whenExceedingMaxCapacity() {
        IQueue<Integer> q = client.getQueue(QUEUE_WITH_MAX_SIZE + randomString());

        for (int i = 0; i < MAX_SIZE_FOR_QUEUE; i++) {
            q.offer(i);
        }
        assertFalse(q.offer(MAX_SIZE_FOR_QUEUE));
    }

    @Test
    public void testOfferPoll() throws IOException, InterruptedException {
        final IQueue<String> q = client.getQueue(QUEUE_WITH_MAX_SIZE + randomString());

        for (int i = 0; i < 10; i++) {
            boolean result = q.offer("item");
            if (i < MAX_SIZE_FOR_QUEUE) {
                assertTrue(result);
            } else {
                assertFalse(result);
            }
        }
        assertEquals(MAX_SIZE_FOR_QUEUE, q.size());

        spawn(new Runnable() {
            @Override
            public void run() {
                sleepSeconds(1);
                q.poll();
            }
        });

        boolean result = q.offer("item", 15, TimeUnit.SECONDS);
        assertTrue(result);


        for (int i = 0; i < 10; i++) {
            Object o = q.poll();
            if (i < MAX_SIZE_FOR_QUEUE) {
                assertNotNull(o);
            } else {
                assertNull(o);
            }
        }
        assertEquals(0, q.size());

        spawn(new Runnable() {
            @Override
            public void run() {
                sleepSeconds(1);
                q.offer("item1");
            }
        });

        Object o = q.poll(15, TimeUnit.SECONDS);
        assertEquals("item1", o);
    }
}
