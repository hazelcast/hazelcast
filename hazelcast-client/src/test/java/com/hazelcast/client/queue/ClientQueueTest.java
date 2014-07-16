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

package com.hazelcast.client.queue;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.Config;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemListener;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.test.HazelcastTestSupport.interruptCurrentThread;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ClientQueueTest {

    final static int MAX_QUEUE_SIZE = 8;
    final static String QUEUE_WITH_MAX_SIZE_NAME = "QUEUE_WITH_MAX_SIZE_NAME*";

    private static HazelcastInstance client;

    private IQueue<Object> randomQueue;
    private IQueue<Object> queueWithMaxSize;

    @BeforeClass
    public static void beforeClass() {
        Config config = new Config();

        QueueConfig queueConfig = config.getQueueConfig(QUEUE_WITH_MAX_SIZE_NAME);
        queueConfig.setMaxSize(MAX_QUEUE_SIZE);

        Hazelcast.newHazelcastInstance(config);
        client = HazelcastClient.newHazelcastClient();
    }

    @AfterClass
    public static void afterClass() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Before
    public void setup() {
        randomQueue = client.getQueue(randomString());
        queueWithMaxSize = client.getQueue(QUEUE_WITH_MAX_SIZE_NAME + randomString());
    }

    @Test
    public void testOffer() {
        assertTrue(randomQueue.offer(1));
        assertEquals(1, randomQueue.size());
    }

    @Test
    public void testAdd() {
        assertTrue(randomQueue.add(1));
        assertEquals(1, randomQueue.size());
    }

    @Test
    public void testPut() throws InterruptedException {
        randomQueue.put(1);
        assertEquals(1, randomQueue.size());
    }

    @Test
    public void testTake() throws InterruptedException {
        randomQueue.put(1);
        assertEquals(1, randomQueue.take());
    }


    @Test(expected = InterruptedException.class)
    public void testTake_whenInterruptedWhileBlocking() throws InterruptedException {
        interruptCurrentThread(2000);

        randomQueue.take();
    }

    @Test
    public void testEmptyPeak() throws InterruptedException {
        assertNull(randomQueue.peek());
    }

    @Test
    public void testPeak() throws InterruptedException {
        randomQueue.offer(1);
        assertEquals(1, randomQueue.peek());
        assertEquals(1, randomQueue.peek());
        assertEquals(1, randomQueue.size());
    }

    @Test(expected = NoSuchElementException.class)
    public void testEmptyElement() throws InterruptedException {
        randomQueue.element();
    }

    @Test
    public void testElement() throws InterruptedException {
        randomQueue.offer(1);
        assertEquals(1, randomQueue.element());
    }

    @Test
    public void testPoll() throws InterruptedException {
        randomQueue.offer(1);
        assertEquals(1, randomQueue.poll());
    }

    @Test(expected = InterruptedException.class)
    public void testPoll_whenInterruptedWhileBlocking() throws InterruptedException {
        interruptCurrentThread(2000);

        randomQueue.poll(1, TimeUnit.MINUTES);
    }

    @Test
    public void testOfferWithTimeOut() throws IOException, InterruptedException {
        boolean result = randomQueue.offer(1, 50, TimeUnit.MILLISECONDS);
        assertTrue(result);
    }

    @Test
    public void testRemainingCapacity() throws IOException {
        assertEquals(Integer.MAX_VALUE, randomQueue.remainingCapacity());
        randomQueue.offer("one");
        assertEquals(Integer.MAX_VALUE - 1, randomQueue.remainingCapacity());
    }

    @Test(expected = NoSuchElementException.class)
    public void testEmptyRemove() throws IOException {
        randomQueue.remove();
    }

    @Test
    public void testRemoveTop() throws IOException, InterruptedException {
        randomQueue.offer(1);
        assertEquals(1, randomQueue.remove());
    }

    @Test
    public void testRemove() throws IOException {
        randomQueue.offer(1);
        assertTrue(randomQueue.remove(1));
        assertFalse(randomQueue.remove(2));
    }

    @Test
    public void testContains() {
        randomQueue.offer(1);
        assertTrue(randomQueue.contains(1));
        assertFalse(randomQueue.contains(2));
    }

    @Test
    public void testContainsAll() {
        int maxItems = 11;

        List<Object> trueList = new ArrayList<Object>();
        List<Object> falseList = new ArrayList<Object>();
        for (int i = 0; i < maxItems; i++) {
            randomQueue.offer(i);
            trueList.add(i);
            falseList.add(i + 1);
        }

        assertTrue(randomQueue.containsAll(trueList));
        assertFalse(randomQueue.containsAll(falseList));
    }

    @Test
    public void testDrain() {
        int maxItems = 12;

        List<Object> offeredList = new LinkedList<Object>();
        for (int i = 0; i < maxItems; i++) {
            randomQueue.offer(i);
            offeredList.add(i);
        }
        List<Object> drainedList = new LinkedList<Object>();
        int totalDrained = randomQueue.drainTo(drainedList);

        assertEquals(maxItems, totalDrained);
        assertEquals(offeredList, drainedList);
    }

    @Test
    public void testPartialDrain() {
        int maxItems = 15;
        int itemsToDrain = maxItems / 2;

        List<Object> expectedList = new LinkedList<Object>();
        for (int i = 0; i < maxItems; i++) {
            randomQueue.offer(i);
            if (i < itemsToDrain) {
                expectedList.add(i);
            }
        }
        List<Object> drainedList = new LinkedList<Object>();
        int totalDrained = randomQueue.drainTo(drainedList, itemsToDrain);

        assertEquals(itemsToDrain, totalDrained);
        assertEquals(expectedList, drainedList);
    }

    @Test
    public void testIterator() {
        int maxItems = 18;

        for (int i = 0; i < maxItems; i++) {
            randomQueue.offer(i);
        }

        int i = 0;
        for (Object o : randomQueue) {
            assertEquals(i++, o);
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testQueueRemoveFromIterator() {
        randomQueue.add("one");
        Iterator<Object> iterator = randomQueue.iterator();
        iterator.next();
        iterator.remove();
    }

    @Test
    public void testToArray() {
        int maxItems = 19;

        Object[] offered = new Object[maxItems];
        for (int i = 0; i < maxItems; i++) {
            randomQueue.offer(i);
            offered[i] = i;
        }

        Object[] result = randomQueue.toArray();
        for (int i = 0; i < offered.length; i++) {
            assertEquals(offered[i], result[i]);
        }
    }

    @Test
    public void testToPartialArray() {
        int maxItems = 74;
        int arraySZ = maxItems / 2;

        Object[] offered = new Object[maxItems];
        for (int i = 0; i < maxItems; i++) {
            randomQueue.offer(i);
            offered[i] = i;
        }

        Object[] result = randomQueue.toArray(new Object[arraySZ]);
        for (int i = 0; i < offered.length; i++) {
            assertEquals(offered[i], result[i]);
        }
    }

    @Test
    public void testAddAll() throws IOException {
        int maxItems = 13;

        Collection<Object> coll = new ArrayList<Object>(maxItems);
        for (int i = 0; i < maxItems; i++) {
            coll.add(i);
        }

        assertTrue(randomQueue.addAll(coll));
        assertEquals(coll.size(), randomQueue.size());
    }

    @Test
    public void testRemoveList() throws IOException {
        int maxItems = 131;

        List<Object> removeList = new LinkedList<Object>();
        for (int i = 0; i < maxItems; i++) {
            randomQueue.add(i);
            removeList.add(i);
        }

        assertTrue(randomQueue.removeAll(removeList));
        assertEquals(0, randomQueue.size());
    }

    @Test
    public void testRemoveList_whereNotFound() throws IOException {
        int maxItems = 131;

        List<Object> removeList = new LinkedList<Object>();
        for (int i = 0; i < maxItems; i++) {
            randomQueue.add(i);
        }
        removeList.add(maxItems + 1);
        removeList.add(maxItems + 2);

        assertFalse(randomQueue.removeAll(removeList));
        assertEquals(maxItems, randomQueue.size());
    }

    @Test
    public void testRetainEmptyList() throws IOException {
        int maxItems = 131;

        for (int i = 0; i < maxItems; i++) {
            randomQueue.add(i);
        }

        List<Object> retain = new LinkedList<Object>();
        assertTrue(randomQueue.retainAll(retain));
        assertEquals(0, randomQueue.size());
    }

    @Test
    public void testRetainAllList() throws IOException {
        int maxItems = 181;

        List<Object> retain = new LinkedList<Object>();
        for (int i = 0; i < maxItems; i++) {
            randomQueue.add(i);
            retain.add(i);
        }

        assertFalse(randomQueue.retainAll(retain));
        assertEquals(maxItems, randomQueue.size());
    }

    @Test
    public void testRetainAll_ListNotFound() throws IOException {
        int maxItems = 181;

        List<Object> retain = new LinkedList<Object>();
        for (int i = 0; i < maxItems; i++) {
            randomQueue.add(i);
        }
        retain.add(maxItems + 1);
        retain.add(maxItems + 2);

        assertTrue(randomQueue.retainAll(retain));
        assertEquals(0, randomQueue.size());
    }

    @Test
    public void testRetainAll_mixedList() throws IOException {
        int maxItems = 181;

        List<Object> retain = new LinkedList<Object>();
        for (int i = 0; i < maxItems; i++) {
            randomQueue.add(i);
        }
        retain.add(maxItems - 1);
        retain.add(maxItems + 1);

        assertTrue(randomQueue.retainAll(retain));
        assertEquals(1, randomQueue.size());
    }

    @Test
    public void testSize() {
        int maxItems = 143;

        for (int i = 0; i < maxItems; i++) {
            randomQueue.add(i);
        }
        assertEquals(maxItems, randomQueue.size());
    }

    @Test
    public void testIsEmpty() {
        assertTrue(randomQueue.isEmpty());
    }

    @Test
    public void testNotIsEmpty() {
        randomQueue.offer(1);
        assertFalse(randomQueue.isEmpty());
    }

    @Test
    public void testClear() {
        int maxItems = 123;

        for (int i = 0; i < maxItems; i++) {
            randomQueue.add(i);
        }

        assertEquals(maxItems, randomQueue.size());
        randomQueue.clear();
        assertEquals(0, randomQueue.size());
    }

    @Test
    public void testListener() throws Exception {
        final int maxItems = 10;
        final CountDownLatch itemAddedLatch = new CountDownLatch(maxItems);
        final CountDownLatch itemRemovedLatch = new CountDownLatch(maxItems);

        String id = randomQueue.addItemListener(new ItemListener<Object>() {

            public void itemAdded(ItemEvent itemEvent) {
                itemAddedLatch.countDown();
            }

            public void itemRemoved(ItemEvent item) {
                itemRemovedLatch.countDown();
            }
        }, true);

        new Thread() {
            public void run() {
                for (int i = 0; i < maxItems; i++) {
                    randomQueue.offer(i);
                    randomQueue.remove(i);
                }
            }
        }.start();

        assertTrue(itemAddedLatch.await(5, TimeUnit.SECONDS));
        assertTrue(itemRemovedLatch.await(5, TimeUnit.SECONDS));

        randomQueue.removeItemListener(id);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetLocalQueueStats() {
        randomQueue.getLocalQueueStats();
    }

    @Test
    public void testAdd_whenReachingMaximumCapacity() {
        for (int i = 0; i < MAX_QUEUE_SIZE; i++) {
            queueWithMaxSize.add(1);
        }
        assertEquals(MAX_QUEUE_SIZE, queueWithMaxSize.size());
    }

    @Test(expected = IllegalStateException.class)
    public void testAdd_whenExceedingMaximumCapacity() {
        for (int i = 0; i < MAX_QUEUE_SIZE; i++) {
            queueWithMaxSize.add(1);
        }
        queueWithMaxSize.add(1);
    }

    @Test
    public void testOffer_whenExceedingMaxCapacity() {
        for (int i = 0; i < MAX_QUEUE_SIZE; i++) {
            queueWithMaxSize.offer(i);
        }
        assertFalse(queueWithMaxSize.offer(MAX_QUEUE_SIZE));
    }

    @Test
    public void testOfferPoll() throws IOException, InterruptedException {
        for (int i = 0; i < 10; i++) {
            boolean result = queueWithMaxSize.offer("item");
            if (i < MAX_QUEUE_SIZE) {
                assertTrue(result);
            } else {
                assertFalse(result);
            }
        }
        assertEquals(MAX_QUEUE_SIZE, queueWithMaxSize.size());

        final Thread t1 = new Thread() {
            public void run() {
                try {
                    Thread.sleep(2 * 1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                queueWithMaxSize.poll();
            }
        };
        t1.start();

        boolean result = queueWithMaxSize.offer("item", 5, TimeUnit.SECONDS);
        assertTrue(result);

        for (int i = 0; i < 10; i++) {
            Object o = queueWithMaxSize.poll();
            if (i < MAX_QUEUE_SIZE) {
                assertNotNull(o);
            } else {
                assertNull(o);
            }
        }
        assertEquals(0, queueWithMaxSize.size());


        final Thread t2 = new Thread() {
            public void run() {
                try {
                    Thread.sleep(2 * 1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                queueWithMaxSize.offer("item1");
            }
        };
        t2.start();

        Object o = queueWithMaxSize.poll(5, TimeUnit.SECONDS);
        assertEquals("item1", o);
        t1.join(10000);
        t2.join(10000);
    }
}