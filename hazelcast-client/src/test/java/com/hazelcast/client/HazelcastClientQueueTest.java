/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemListener;
import org.junit.AfterClass;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class HazelcastClientQueueTest extends HazelcastClientTestBase {

    @Test(expected = NullPointerException.class)
    public void testPutNull() throws InterruptedException {
        HazelcastClient hClient = getHazelcastClient();
        IQueue<?> queue = hClient.getQueue("testPutNull");
        queue.put(null);
    }

    @Test
    public void testQueueName() {
        HazelcastClient hClient = getHazelcastClient();
        IQueue<?> queue = hClient.getQueue("testQueueName");
        assertEquals("testQueueName", queue.getName());
    }

    @Test
    public void testQueueOffer() throws InterruptedException {
        HazelcastClient hClient = getHazelcastClient();
        IQueue<String> queue = hClient.getQueue("testQueueOffer");
        assertTrue(queue.offer("a"));
        assertTrue(queue.offer("b", 10, TimeUnit.MILLISECONDS));
        assertEquals("a", queue.poll());
        assertEquals("b", queue.poll());
    }

    @Test
    public void testQueuePoll() throws InterruptedException {
        HazelcastClient hClient = getHazelcastClient();
        final CountDownLatch cl = new CountDownLatch(1);
        final IQueue<String> queue = hClient.getQueue("testQueuePoll");
        assertTrue(queue.offer("a"));
        assertEquals("a", queue.poll());
        new Thread(new Runnable() {

            public void run() {
                try {
                    Thread.sleep(100);
                    assertEquals("b", queue.poll(100, TimeUnit.MILLISECONDS));
                    cl.countDown();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();
        Thread.sleep(50);
        assertTrue(queue.offer("b"));
        assertTrue(cl.await(200, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testQueueRemove() {
        HazelcastClient hClient = getHazelcastClient();
        IQueue<String> queue = hClient.getQueue("testQueueRemove");
        assertTrue(queue.offer("a"));
        assertEquals("a", queue.remove());
    }

    @Test
    public void testQueuePeek() {
        HazelcastClient hClient = getHazelcastClient();
        IQueue<String> queue = hClient.getQueue("testQueuePeek");
        assertTrue(queue.offer("a"));
        assertEquals("a", queue.peek());
    }

    @Test
    public void element() {
        HazelcastClient hClient = getHazelcastClient();
        IQueue<String> queue = hClient.getQueue("element");
        assertTrue(queue.offer("a"));
        assertEquals("a", queue.element());
    }

    @Test
    public void addAll() {
        HazelcastClient hClient = getHazelcastClient();
        IQueue<String> queue = hClient.getQueue("addAll");
        List<String> list = new ArrayList<String>();
        list.add("a");
        list.add("b");
        assertTrue(queue.addAll(list));
        assertEquals("a", queue.poll());
        assertEquals("b", queue.poll());
    }

    @Test
    public void clear() {
        HazelcastClient hClient = getHazelcastClient();
        IQueue<String> queue = hClient.getQueue("clear");
        List<String> list = new ArrayList<String>();
        list.add("a");
        list.add("b");
        assertTrue(queue.size() == 0);
        assertTrue(queue.addAll(list));
        assertTrue(queue.size() == 2);
        queue.clear();
        assertTrue(queue.size() == 0);
    }

    @Test
    public void containsAll() {
        HazelcastClient hClient = getHazelcastClient();
        IQueue<String> queue = hClient.getQueue("containsAll");
        List<String> list = new ArrayList<String>();
        list.add("a");
        list.add("b");
        assertTrue(queue.size() == 0);
        assertTrue(queue.addAll(list));
        assertTrue(queue.size() == 2);
        assertTrue(queue.containsAll(list));
    }

    @Test
    public void isEmpty() {
        HazelcastClient hClient = getHazelcastClient();
        IQueue<String> queue = hClient.getQueue("isEmpty");
        assertTrue(queue.isEmpty());
        queue.offer("asd");
        assertFalse(queue.isEmpty());
    }

    @Test
    public void iterator() {
        HazelcastClient hClient = getHazelcastClient();
        IQueue<String> queue = hClient.getQueue("iterator");
        assertTrue(queue.isEmpty());
        int count = 100;
        Map<Integer, Integer> map = new HashMap<Integer, Integer>();
        for (int i = 0; i < count; i++) {
            queue.offer("" + i);
            map.put(i, 1);
        }
        Iterator<String> it = queue.iterator();
        while (it.hasNext()) {
            String o = it.next();
            map.put(Integer.valueOf(o), map.get(Integer.valueOf(o)) - 1);
        }
        for (int i = 0; i < count; i++) {
            assertTrue(map.get(i) == 0);
        }
    }

    @Test
    public void removeAll() {
        HazelcastClient hClient = getHazelcastClient();
        IQueue<String> queue = hClient.getQueue("removeAll");
        assertTrue(queue.isEmpty());
        int count = 100;
        Map<Integer, Integer> map = new HashMap<Integer, Integer>();
        for (int i = 0; i < count; i++) {
            queue.offer("" + i);
            map.put(i, 1);
        }
        List<String> list = new ArrayList<String>();
        for (int i = 0; i < count / 2; i++) {
            list.add(String.valueOf(i));
        }
        queue.removeAll(list);
        assertTrue(queue.size() == count / 2);
    }

    @Test
    public void testIterator() {
        HazelcastClient hClient = getHazelcastClient();
        IQueue<String> queue = hClient.getQueue("testIterator");
        assertTrue(queue.isEmpty());
        int count = 100;
        Map<Integer, Integer> map = new HashMap<Integer, Integer>();
        for (int i = 0; i < count; i++) {
            queue.offer("" + i);
            map.put(i, 1);
        }
        Iterator<String> it = queue.iterator();
        while (it.hasNext()) {
            String item = it.next();
            map.remove(Integer.valueOf(item));
            it.remove();
        }
        assertEquals(0, queue.size());
        assertEquals(0, map.size());
    }

    @Test(timeout = 2000)
    public void testQueueAddFromServerGetFromClient() {
        HazelcastInstance h = getHazelcastInstance();
        HazelcastInstance client = getHazelcastClient();
        String name = "testQueueAddFromServerGetFromClient";
        h.getQueue(name).offer("message");
        client.getQueue(name).poll();
    }

    @Test
    public void testQItemListener() {
        final CountDownLatch latch = new CountDownLatch(2);
        String name = "testListListener";
        IQueue<String> qOperation = getHazelcastInstance().getQueue(name);
        IQueue<String> qListener = getHazelcastClient().getQueue(name);
        qListener.addItemListener(new ItemListener<String>() {
            public void itemAdded(ItemEvent<String> itemEvent) {
                assertEquals("hello", itemEvent.getItem());
                latch.countDown();
            }

            public void itemRemoved(ItemEvent<String> itemEvent) {
                assertEquals("hello", itemEvent.getItem());
                latch.countDown();
            }
        }, true);
        qOperation.add("hello");
        qOperation.poll();
        try {
            assertTrue(latch.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException ignored) {
        }
    }

    @Test
    public void testQueueItemListener() {
        final CountDownLatch latch = new CountDownLatch(2);
        HazelcastClient hClient = getHazelcastClient();
        IQueue<String> queue = hClient.getQueue("testQueueListener");
        queue.addItemListener(new ItemListener<String>() {
            public void itemAdded(ItemEvent<String> itemEvent) {
                assertEquals("hello", itemEvent.getItem());
                latch.countDown();
            }

            public void itemRemoved(ItemEvent<String> itemEvent) {
                assertEquals("hello", itemEvent.getItem());
                latch.countDown();
            }
        }, true);
        queue.offer("hello");
        assertEquals("hello", queue.poll());
        try {
            assertTrue(latch.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException ignored) {
        }
    }

    @AfterClass
    public static void shutdown() {
    }
}
