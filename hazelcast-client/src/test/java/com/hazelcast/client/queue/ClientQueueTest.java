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
import com.hazelcast.core.*;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static org.junit.Assert.*;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ClientQueueTest extends HazelcastTestSupport{

    final static int maxSizeForQueue = 8;
    final static String queueForTestQueueWithSizeLimit = "testQueueWithSizeLimit";
    final static String queueForTestOfferPoll = "queueForTestOfferPoll";

    static HazelcastInstance client;
    static HazelcastInstance server;

    @BeforeClass
    public static void init(){
        Config config = new Config();

        QueueConfig queueConfig = config.getQueueConfig(queueForTestQueueWithSizeLimit);
        queueConfig.setMaxSize(maxSizeForQueue);

        queueConfig = config.getQueueConfig(queueForTestOfferPoll);
        queueConfig.setMaxSize(maxSizeForQueue);

        server = Hazelcast.newHazelcastInstance(config);
        client = HazelcastClient.newHazelcastClient();
    }

    @AfterClass
    public static void destroy() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testOffer() {
        final IQueue q = client.getQueue(randomString());
        assertTrue(q.offer(1));
        assertEquals(1, q.size());
    }

    @Test
    public void testadd() {
        final IQueue q = client.getQueue(randomString());
        assertTrue(q.add(1));
        assertEquals(1, q.size());
    }

    @Test
    public void testPut() throws InterruptedException {
        final IQueue q = client.getQueue(randomString());
        q.put(1);
        assertEquals(1, q.size());
    }

    @Test
    public void testTake() throws InterruptedException {
        final IQueue q = client.getQueue(randomString());
        q.put(1);
        assertEquals(1, q.take());
    }


    @Test(expected = InterruptedException.class)
    public void testTake_whenInterruptedWhileBlocking() throws InterruptedException {
        IQueue queue = client.getQueue(randomString());
        interruptCurrentThread(2000);

        queue.take();
    }

    @Test
    public void testEmptyPeak() throws InterruptedException {
        final IQueue q = client.getQueue(randomString());
        assertNull(q.peek());
    }

    @Test
    public void testPeak() throws InterruptedException {
        final IQueue q = client.getQueue(randomString());
        q.offer(1);
        assertEquals(1, q.peek());
        assertEquals(1, q.peek());
        assertEquals(1, q.size());
    }

    @Test(expected = NoSuchElementException.class)
    public void testEmptyElement() throws InterruptedException {
        final IQueue q = client.getQueue(randomString());
        q.element();
    }

    @Test
    public void testElement() throws InterruptedException {
        final IQueue q = client.getQueue(randomString());
        q.offer(1);
        assertEquals(1, q.element());
    }

    @Test
    public void testPoll() throws InterruptedException {
        final IQueue q = client.getQueue(randomString());
        q.offer(1);
        assertEquals(1, q.poll());
    }

    @Test(expected = InterruptedException.class)
    public void testPoll_whenInterruptedWhileBlocking() throws InterruptedException {
        IQueue queue = client.getQueue(randomString());
        interruptCurrentThread(2000);

        queue.poll(1, TimeUnit.MINUTES);
    }

    @Test
    public void testOfferWithTimeOut() throws IOException, InterruptedException {
        final IQueue q = client.getQueue(randomString());
        boolean result = q.offer(1, 50, TimeUnit.MILLISECONDS);
        assertTrue(result);
    }

    @Test
    public void testRemainingCapacity() throws IOException {
        final IQueue q = client.getQueue(randomString());

        assertEquals(Integer.MAX_VALUE, q.remainingCapacity());
        q.offer("one");
        assertEquals(Integer.MAX_VALUE-1, q.remainingCapacity());
    }

    @Test(expected = NoSuchElementException.class)
    public void testEmptyRemove() throws IOException {
        final IQueue q = client.getQueue(randomString());
        q.remove();
    }

    @Test
    public void testRemoveTop() throws IOException, InterruptedException {
        final IQueue q = client.getQueue(randomString());
        q.offer(1);
        assertEquals(1, q.remove());
    }

    @Test
    public void testRemove() throws IOException {
        final IQueue q = client.getQueue(randomString());

        q.offer(1);
        assertTrue(q.remove(1));
        assertFalse(q.remove(2));
    }

    @Test
    public void testContains() {
        final IQueue q = client.getQueue(randomString());

        q.offer(1);
        assertTrue(q.contains(1));
        assertFalse(q.contains(2));
    }

    @Test
    public void testContainsAll() {
        final int maxItems = 11;
        final IQueue q = client.getQueue(randomString());

        List trueList = new ArrayList();
        List falseList = new ArrayList();
        for(int i=0; i<maxItems; i++){
            q.offer(i);
            trueList.add(i);
            falseList.add(i+1);
        }
        assertTrue(q.containsAll(trueList));
        assertFalse(q.containsAll(falseList));
    }


    @Test
    public void testDrain() {
        final int maxItems = 12;
        final IQueue q = client.getQueue(randomString());

        List offeredList = new LinkedList();
        for(int i=0; i<maxItems; i++){
            q.offer(i);
            offeredList.add(i);
        }
        List drainedList = new LinkedList();
        int totalDrained = q.drainTo(drainedList);

        assertEquals(maxItems, totalDrained);
        assertEquals(offeredList, drainedList);
    }

    @Test
    public void testPartialDrain() {
        final int maxItems = 15;
        final int itemsToDrain = maxItems/2;

        final IQueue q = client.getQueue(randomString());

        List expectedList = new LinkedList();
        for(int i=0; i<maxItems; i++){
            q.offer(i);
            if(i < itemsToDrain){
                expectedList.add(i);
            }
        }
        List drainedList = new LinkedList();
        int totalDrained = q.drainTo(drainedList, itemsToDrain);

        assertEquals(itemsToDrain, totalDrained);
        assertEquals(expectedList, drainedList);
    }

    @Test
    public void testIterator(){
        final int maxItems = 18;
        final IQueue q = client.getQueue(randomString());

        for(int i=0; i<maxItems; i++){
            q.offer(i);
        }

        int i=0;
        for (Object o : q) {
            assertEquals(i++, o);
        }
    }

    @Test
    public void testToArray(){
        final int maxItems = 19;
        final IQueue q = client.getQueue(randomString());

        Object[] offered = new Object[maxItems];
        for(int i=0; i<maxItems; i++){
            q.offer(i);
            offered[i]=i;
        }

        Object[] result =  q.toArray();
        assertEquals(offered, result);
    }

    @Test
    public void testToPartialArray(){
        final int maxItems = 74;
        final int arraySZ = maxItems / 2 ;
        final IQueue q = client.getQueue(randomString());

        Object[] offered = new Object[maxItems];
        for(int i=0; i<maxItems; i++){
            q.offer(i);
            offered[i]=i;
        }

        Object[] result = q.toArray(new Object[arraySZ]);
        assertEquals(offered, result);
    }

    @Test
    public void testAddAll() throws IOException {
        final int maxItems = 13;
        final IQueue q = client.getQueue(randomString());

        Collection coll = new ArrayList(maxItems);

        for(int i=0; i<maxItems; i++){
            coll.add(i);
        }

        assertTrue(q.addAll(coll));
        assertEquals(coll.size(), q.size());
    }

    @Test
    public void testRemoveList() throws IOException {
        final int maxItems = 131;
        final IQueue q = client.getQueue(randomString());

        List removeList = new LinkedList();
        for(int i=0; i<maxItems; i++){
            q.add(i);
            removeList.add(i);
        }

        assertTrue(q.removeAll(removeList));
        assertEquals(0, q.size());
    }

    @Test
    public void testRemoveList_whereNotFound() throws IOException {
        final int maxItems = 131;
        final IQueue q = client.getQueue(randomString());

        List removeList = new LinkedList();
        for(int i=0; i<maxItems; i++){
            q.add(i);
        }
        removeList.add(maxItems+1);
        removeList.add(maxItems+2);

        assertFalse(q.removeAll(removeList));
        assertEquals(maxItems, q.size());
    }

    @Test
    public void testRetainEmptyList() throws IOException {
        final int maxItems = 131;
        final IQueue q = client.getQueue(randomString());

        for(int i=0; i<maxItems; i++){
            q.add(i);
        }

        List retain = new LinkedList();
        assertTrue(q.retainAll(retain));
        assertEquals(0, q.size());
    }

    @Test
    public void testRetainAllList() throws IOException {
        final int maxItems = 181;
        final IQueue q = client.getQueue(randomString());

        List retain = new LinkedList();
        for(int i=0; i<maxItems; i++){
            q.add(i);
            retain.add(i);
        }

        assertFalse(q.retainAll(retain));
        assertEquals(maxItems, q.size());
    }

    @Test
    public void testRetainAll_ListNotFound() throws IOException {
        final int maxItems = 181;
        final IQueue q = client.getQueue(randomString());

        List retain = new LinkedList();
        for(int i=0; i<maxItems; i++){
            q.add(i);
        }
        retain.add(maxItems+1);
        retain.add(maxItems+2);

        assertTrue(q.retainAll(retain));
        assertEquals(0, q.size());
    }

    @Test
    public void testRetainAll_mixedList() throws IOException {
        final int maxItems = 181;
        final IQueue q = client.getQueue(randomString());

        List retain = new LinkedList();
        for(int i=0; i<maxItems; i++){
            q.add(i);
        }
        retain.add(maxItems-1);
        retain.add(maxItems+1);

        assertTrue(q.retainAll(retain));
        assertEquals(1, q.size());
    }

    @Test
    public void testSize(){
        final int maxItems = 143;
        final IQueue q = client.getQueue(randomString());

        for(int i=0; i<maxItems; i++){
            q.add(i);
        }
        assertEquals(maxItems, q.size());
    }

    @Test
    public void testIsEmpty(){
        final IQueue q = client.getQueue(randomString());
        assertTrue(q.isEmpty());
    }

    @Test
    public void testNotIsEmpty(){
        final IQueue q = client.getQueue(randomString());
        q.offer(1);
        assertFalse(q.isEmpty());
    }

    @Test
    public void testClear(){
        final int maxItems = 123;
        final IQueue q = client.getQueue(randomString());

        for(int i=0; i<maxItems; i++){
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


        final IQueue queue = client.getQueue(randomString());

        String id = queue.addItemListener(new ItemListener() {

            public void itemAdded(ItemEvent itemEvent) {
                itemAddedLatch.countDown();
            }

            public void itemRemoved(ItemEvent item) {
                itemRemovedLatch.countDown();
            }
        }, true);

        new Thread(){
            public void run() {
                for (int i=0; i<maxItems; i++){
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
    public void testGetLocalQueueStats()  {
        final IQueue q = client.getQueue(randomString());
        q.getLocalQueueStats();
    }

    @Test
    public void testQueueWithSizeLimit(){
        final IQueue q = client.getQueue(queueForTestQueueWithSizeLimit);

        for(int i=0; i< maxSizeForQueue; i++){
            q.offer(i);
        }
        assertFalse(q.offer(maxSizeForQueue));
    }

    @Test
    public void testOfferPoll() throws IOException, InterruptedException {

        final IQueue q = client.getQueue(queueForTestOfferPoll);

        for (int i=0; i<10; i++){
            boolean result = q.offer("item");
            if (i<maxSizeForQueue){
                assertTrue(result);
            }
            else {
                assertFalse(result);
            }
        }
        assertEquals(maxSizeForQueue, q.size());

        final Thread t1 = new Thread() {
            public void run() {
                try {
                    Thread.sleep(2 * 1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                q.poll();
            }
        };
        t1.start();

        boolean result = q.offer("item",5, TimeUnit.SECONDS);
        assertTrue(result);


        for (int i=0; i<10; i++){
            Object o = q.poll();
            if (i<maxSizeForQueue){
                assertNotNull(o);
            }
            else {
                assertNull(o);
            }
        }
        assertEquals(0, q.size());


        final Thread t2 = new Thread() {
            public void run() {
                try {
                    Thread.sleep(2 * 1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                q.offer("item1");
            }
        };
        t2.start();

        Object o = q.poll(5, TimeUnit.SECONDS);
        assertEquals("item1", o);
        t1.join(10000);
        t2.join(10000);
    }
}