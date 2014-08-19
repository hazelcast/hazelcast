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
import com.hazelcast.config.ItemListenerConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.core.DistributedObjectEvent;
import com.hazelcast.core.DistributedObjectListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemListener;
import com.hazelcast.monitor.LocalQueueStats;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class BasicQueueTest extends HazelcastTestSupport {

    @Test
    public void testConfigListenerRegistration() throws InterruptedException {
        Config config = new Config();
        final String name = "queue";
        final QueueConfig queueConfig = config.getQueueConfig(name);
        final DummyListener dummyListener = new DummyListener();
        final ItemListenerConfig itemListenerConfig = new ItemListenerConfig(dummyListener, true);
        queueConfig.addItemListenerConfig(itemListenerConfig);
        final HazelcastInstance instance = createHazelcastInstance(config);
        final IQueue queue = instance.getQueue(name);
        queue.offer("item");
        queue.poll();
        assertTrue(dummyListener.latch.await(10, TimeUnit.SECONDS));
    }

    private static class DummyListener implements ItemListener, Serializable {

        public final CountDownLatch latch = new CountDownLatch(2);

        public DummyListener() {
        }

        public void itemAdded(ItemEvent item) {
            latch.countDown();
        }

        public void itemRemoved(ItemEvent item) {
            latch.countDown();
        }
    }

    @Test
    public void testQueueEviction() throws Exception {
        final Config config = new Config();
        config.getQueueConfig("q").setEmptyQueueTtl(2);
        final HazelcastInstance hz = createHazelcastInstance(config);
        final IQueue<Object> q = hz.getQueue("q");

        try {
            assertTrue(q.offer("item"));
            assertEquals("item", q.poll());
            q.take();
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof DistributedObjectDestroyedException);
        }
        q.size();

    }

    @Test
    public void testQueueEviction2() throws Exception {
        final Config config = new Config();
        config.getQueueConfig("q2").setEmptyQueueTtl(0);
        final HazelcastInstance hz = createHazelcastInstance(config);

        final CountDownLatch latch = new CountDownLatch(2);
        hz.addDistributedObjectListener(new DistributedObjectListener() {
            public void distributedObjectCreated(DistributedObjectEvent event) {
                latch.countDown();
            }

            public void distributedObjectDestroyed(DistributedObjectEvent event) {
                latch.countDown();
            }
        });

        final IQueue<Object> q = hz.getQueue("q2");
        q.offer("item");
        q.poll();

        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    @Test
    public void testQueueStats() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final String name = "testQueueStats";

        HazelcastInstance ins1 = factory.newHazelcastInstance();
        final int items = 20;
        IQueue q1 = ins1.getQueue(name);
        for (int i = 0; i < items / 2; i++) {
            q1.offer("item" + i);
        }

        HazelcastInstance ins2 = factory.newHazelcastInstance();
        IQueue q2 = ins2.getQueue(name);
        for (int i = 0; i < items / 2; i++) {
            q2.offer("item" + i);
        }

        LocalQueueStats stats1 = ins1.getQueue(name).getLocalQueueStats();
        LocalQueueStats stats2 = ins2.getQueue(name).getLocalQueueStats();

        assertTrue(stats1.getOwnedItemCount() == items || stats2.getOwnedItemCount() == items);
        assertFalse(stats1.getOwnedItemCount() == items && stats2.getOwnedItemCount() == items);

        if (stats1.getOwnedItemCount() == items) {
            assertEquals(items, stats2.getBackupItemCount());
            assertEquals(0, stats1.getBackupItemCount());
        } else {
            assertEquals(items, stats1.getBackupItemCount());
            assertEquals(0, stats2.getBackupItemCount());
        }
    }

    @Test
    public void testOffer() throws Exception {
        final String name = "testOffer";
        int count = 100;
        HazelcastInstance instance = createHazelcastInstance();
        IQueue<String> queue = instance.getQueue(name);
        for (int i = 0; i < count; i++) {
            queue.offer("item" + i);
        }

        assertEquals(100, queue.size());
    }

    @Test
    public void testOffer_whenNoCapacity() {
        final String name = "testOffer_whenNoCapacity";
        int count = 100;
        Config config = new Config();
        QueueConfig queueConfig = config.getQueueConfig(name);
        queueConfig.setMaxSize(count);
        HazelcastInstance instance = createHazelcastInstance(config);
        IQueue<String> queue = instance.getQueue(name);
        for (int i = 0; i < count; i++) {
            queue.offer("item" + i);
        }

        assertEquals(100, queue.size());
        assertFalse(queue.offer("rejected"));
    }

    @Test
    public void testOffer_whenNullArgument() {
        final String name = "testOffer_whenNullArgument";
        HazelcastInstance instance = createHazelcastInstance();
        IQueue<String> queue = instance.getQueue(name);
        try {
            queue.offer(null);
            fail();
        } catch (NullPointerException e) {
        }
    }

    @Test
    public void testOfferWithTimeout() throws InterruptedException {
        final String name = "testOfferWithTimeout";
        Config config = new Config();
        final int count = 100;
        config.getQueueConfig(name).setMaxSize(count);
        HazelcastInstance instance = createHazelcastInstance(config);
        final IQueue<String> queue = instance.getQueue(name);
        OfferThread offerThread = new OfferThread(queue);
        for (int i = 0; i < 100; i++) {
            queue.offer("item" + i);
        }

        assertSizeEventually(100, queue);
        assertFalse(queue.offer("rejected"));
        offerThread.start();
        queue.poll();
        assertTrue(queue.contains("waiting"));
        assertSizeEventually(100, queue);
    }

    @Test
    public void testPoll() {
        final String name = "testPoll";
        int count = 100;
        HazelcastInstance instance = createHazelcastInstance();
        IQueue<String> queue = instance.getQueue(name);
        for (int i = 0; i < count; i++) {
            queue.offer("item" + i);
        }

        assertEquals(100, queue.size());
        queue.poll();
        queue.poll();
        queue.poll();
        queue.poll();
        assertEquals(96, queue.size());
    }


    @Test
    public void testPollWithTimeout() throws Exception {
        final String name = "testPollWithTimeout";
        HazelcastInstance instance = createHazelcastInstance();
        final IQueue<String> queue = instance.getQueue(name);
        PollThread pollThread = new PollThread(queue);

        assertSizeEventually(0, queue);
        pollThread.start();
        assertTrue(queue.offer("offer"));
        assertSizeEventually(0, queue);
    }

    @Test
    public void testPoll_whenQueueEmpty() {
        final String name = "testPoll_whenQueueEmpty";
        HazelcastInstance instance = createHazelcastInstance();
        IQueue<String> queue = instance.getQueue(name);

        assertEquals(0, queue.size());
        assertNull(queue.poll());
    }

    @Test
    public void testRemove() {
        final String name = "testRemove";
        HazelcastInstance instance = createHazelcastInstance();
        IQueue<String> queue = instance.getQueue(name);
        for (int i = 0; i < 10; i++) {
            queue.offer("item" + i);
        }

        assertTrue(queue.remove("item4"));
        assertEquals(queue.size(), 9);
    }

    @Test
    public void testRemove_whenElementNotExists() {
        final String name = "testRemove_whenElementNotExists";
        HazelcastInstance instance = createHazelcastInstance();
        IQueue<String> queue = instance.getQueue(name);
        for (int i = 0; i < 10; i++) {
            queue.offer("item" + i);
        }

        assertFalse(queue.remove("item13"));
        assertEquals(10, queue.size());
    }

    @Test
    public void testRemove_whenQueueEmpty() {
        final String name = "testRemove_whenQueueEmpty";
        HazelcastInstance instance = createHazelcastInstance();
        IQueue<String> queue = instance.getQueue(name);

        assertSizeEventually(0, queue);
        try {
            queue.remove("not in Queue");
        } catch (NoSuchElementException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testDrainTo() {
        final String name = "testDrainTo";
        HazelcastInstance instance = createHazelcastInstance();
        IQueue<String> queue = instance.getQueue(name);
        for (int i = 0; i < 10; i++) {
            queue.offer("item" + i);
        }
        List list = new ArrayList(10);

        assertEquals(10, queue.drainTo(list));
        assertEquals(10, list.size());
        assertEquals("item0", list.remove(0));
        assertEquals(0, queue.size());
    }

    @Test
    public void testDrainToWithMaxElement() {
        final String name = "testDrainToWithMaxElement";
        HazelcastInstance instance = createHazelcastInstance();
        IQueue<String> queue = instance.getQueue(name);
        for (int i = 0; i < 10; i++) {
            queue.offer("item" + i);
        }
        List list = new ArrayList(10);

        queue.drainTo(list, 4);
        assertEquals(4, list.size());
        assertTrue(list.contains("item3"));
        assertEquals(6, queue.size());
    }

    @Test
    public void testDrainTo_whenQueueEmpty() {
        final String name = "testDrainTo_whenQueueEmpty";
        HazelcastInstance instance = createHazelcastInstance();
        IQueue<String> queue = instance.getQueue(name);
        List list = new ArrayList();

        assertEquals(0, queue.size());
        assertEquals(0, queue.drainTo(list));
    }

    @Test
    public void testDrainTo_whenCollectionNull() {
        final String name = "testDrainTo_whenCollectionNull";
        HazelcastInstance instance = createHazelcastInstance();
        IQueue<String> queue = instance.getQueue(name);
        for (int i = 0; i < 10; i++) {
            queue.offer("item" + i);
        }
        List list = null;

        try {
            assertEquals(0, queue.drainTo(list));
        } catch (NullPointerException e) {
        }
        assertEquals(0, queue.size());
    }

    @Test
    public void testContains_whenExists() {
        final String name = "testContains_whenExists";
        HazelcastInstance instance = createHazelcastInstance();
        IQueue<String> queue = instance.getQueue(name);
        for (int i = 0; i < 10; i++) {
            queue.offer("item" + i);
        }

        assertTrue(queue.contains("item4"));
        assertTrue(queue.contains("item5"));
        assertTrue(queue.contains("item1"));
    }

    @Test
    public void testContains_whenNotExists() {
        final String name = "testContains_whenNotExists";
        HazelcastInstance instance = createHazelcastInstance();
        IQueue<String> queue = instance.getQueue(name);
        for (int i = 0; i < 10; i++) {
            queue.offer("item" + i);
        }

        assertTrue(queue.contains("item4"));
        assertTrue(queue.remove("item4"));
        assertFalse(queue.contains("item4"));
        assertFalse(queue.contains("item10"));
    }

    @Test
    public void testContains_whenNull() {
        final String name = "testContains";
        HazelcastInstance instance = createHazelcastInstance();
        IQueue queue = instance.getQueue(name);
        for (int i = 0; i < 10; i++) {
            queue.offer("item" + i);
        }

        assertFalse(queue.contains(null));
    }

    @Test
    public void testContainsAll_whenExists() {
        final String name = "testContainsAll_whenExists";
        HazelcastInstance instance = createHazelcastInstance();
        IQueue<String> queue = instance.getQueue(name);
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
    public void testContainsAll_whenNotExists() {
        final String name = "testContainsAll_whenNotExists";
        HazelcastInstance instance = createHazelcastInstance();
        IQueue<String> queue = instance.getQueue(name);
        for (int i = 0; i < 10; i++) {
            queue.offer("item" + i);
        }

        List<String> list = new ArrayList<String>();
        list.add("item1");
        list.add("item2");
        list.add("item14");
        assertFalse(queue.containsAll(list));
    }

    @Test
    public void testContainsAll_whenNull() {
        final String name = "testContainsAll_whenNull";
        HazelcastInstance instance = createHazelcastInstance();
        IQueue<String> queue = instance.getQueue(name);
        for (int i = 0; i < 10; i++) {
            queue.offer("item" + i);
        }

        List list = null;
        try {
            assertFalse(queue.containsAll(list));
        } catch (NullPointerException e) {
        }
    }

    @Test
    public void testAddAll() {
        final String name = "testAddAll";
        HazelcastInstance instance = createHazelcastInstance();
        IQueue<String> queue = instance.getQueue(name);
        List<String> list = new ArrayList<String>();
        for (int i = 0; i < 10; i++) {
            list.add("item" + i);
        }

        assertTrue(queue.addAll(list));
        assertEquals(queue.size(), 10);
    }

    @Test
    public void testAddAll_whenNullCollection() {
        final String name = "testAddAll_whenNullCollection";
        HazelcastInstance instance = createHazelcastInstance();
        IQueue<String> queue = instance.getQueue(name);
        List<String> list = null;

        try {
            assertFalse(queue.addAll(list));
        } catch (NullPointerException e) {
        }
    }

    @Test
    public void testAddAll_whenEmptyCollection() {
        final String name = "testAddAll_whenNullCollection";
        HazelcastInstance instance = createHazelcastInstance();
        IQueue<String> queue = instance.getQueue(name);
        for (int i = 0; i < 10; i++) {
            queue.offer("item" + i);
        }
        List<String> list = new ArrayList<String>();

        assertEquals(10, queue.size());
        assertTrue(queue.addAll(list));
        assertEquals(10, queue.size());
    }

    @Test
    public void testAddAll_whenCollectionContainsNull() {
        final String name = "testAddAll_whenCollectionContainsNull";
        HazelcastInstance instance = createHazelcastInstance();
        IQueue<String> queue = instance.getQueue(name);
        for (int i = 0; i < 10; i++) {
            queue.offer("item" + i);
        }
        List<String> list = new ArrayList<String>();
        list.add("item10");
        list.add(null);

        try {
            assertTrue(queue.addAll(list));
        } catch (NullPointerException e) {
        }

        assertEquals(12, queue.size());
    }

    @Test
    public void testAddAll_whenDuplicateItems() {
        final String name = "testAddAll_whenDuplicateItems";
        HazelcastInstance instance = createHazelcastInstance();
        IQueue<String> queue = instance.getQueue(name);
        for (int i = 0; i < 10; i++) {
            queue.offer("item" + i);
        }
        List<String> list = new ArrayList<String>();
        list.add("item3");

        assertTrue(queue.contains("item3"));
        queue.addAll(list);
        assertEquals(11, queue.size());
    }

    @Test
    public void testRetainAll() {
        final String name = "testRetainAll";
        HazelcastInstance instance = createHazelcastInstance();
        IQueue<String> queue = instance.getQueue(name);
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
        final String name = "testRetainAll_whenCollectionNull";
        HazelcastInstance instance = createHazelcastInstance();
        IQueue<String> queue = instance.getQueue(name);
        queue.add("item3");
        queue.add("item4");
        queue.add("item5");
        List list = null;

        try {
            assertFalse(queue.retainAll(list));
        } catch (NullPointerException e) {
        }
        assertEquals(3, queue.size());
    }

    @Test
    public void testRetainAll_whenCollectionEmpty() {
        final String name = "testRetainAll_whenCollectionEmpty";
        HazelcastInstance instance = createHazelcastInstance();
        IQueue<String> queue = instance.getQueue(name);
        queue.add("item3");
        queue.add("item4");
        queue.add("item5");
        List list = new ArrayList();

        assertTrue(queue.retainAll(list));
        assertEquals(0, queue.size());
    }

    @Test
    public void testRetainAll_whenCollectionContainsNull() {
        final String name = "testRetainAll_whenCollectionEmpty";
        HazelcastInstance instance = createHazelcastInstance();
        IQueue<String> queue = instance.getQueue(name);
        queue.add("item3");
        queue.add("item4");
        queue.add("item5");
        List list = new ArrayList();
        list.add(null);

        assertTrue(queue.retainAll(list));
        assertEquals(0, queue.size());
    }

    @Test
    public void testRemoveAll() {
        final String name = "testRemoveAll";
        HazelcastInstance instance = createHazelcastInstance();
        IQueue<String> queue = instance.getQueue(name);
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
        final String name = "testRemoveAll_whenCollectionNull";
        HazelcastInstance instance = createHazelcastInstance();
        IQueue<String> queue = instance.getQueue(name);
        queue.add("item3");
        queue.add("item4");
        queue.add("item5");
        List<String> list = null;

        try {
            assertTrue(queue.removeAll(list));
        } catch (NullPointerException e) {
        }

        assertEquals(3, queue.size());
    }

    @Test
    public void testRemoveAll_whenCollectionEmpty() {
        final String name = "testRemoveAll_whenCollectionEmpty";
        HazelcastInstance instance = createHazelcastInstance();
        IQueue<String> queue = instance.getQueue(name);
        queue.add("item3");
        queue.add("item4");
        queue.add("item5");
        List<String> list = new ArrayList<String>();

        assertFalse(queue.removeAll(list));
        assertEquals(3, queue.size());
    }

    @Test
    public void testIterator() {
        final String name = "testIterator";
        HazelcastInstance instance = createHazelcastInstance();
        IQueue<String> queue = instance.getQueue(name);
        for (int i = 0; i < 10; i++) {
            queue.offer("item" + i);
        }
        Iterator<String> iterator = queue.iterator();
        int i = 0;
        while (iterator.hasNext()) {
            Object o = iterator.next();
            assertEquals(o, "item" + i++);
        }
    }

    @Test
    public void testIterator_whenQueueEmpty() {
        final String name = "testIterator_whenQueueEmpty";
        HazelcastInstance instance = createHazelcastInstance();
        IQueue<String> queue = instance.getQueue(name);
        Iterator<String> iterator = queue.iterator();

        assertFalse(iterator.hasNext());
        try {
            assertNull(iterator.next());
        } catch (NoSuchElementException e) {
        }
    }

    @Test
    public void testIteratorRemove() {
        final String name = "testIteratorRemove";
        HazelcastInstance instance = createHazelcastInstance();
        IQueue<String> queue = instance.getQueue(name);
        for (int i = 0; i < 10; i++) {
            queue.offer("item" + i);
        }

        Iterator<String> iterator = queue.iterator();
        while (iterator.hasNext()) {
            iterator.next();
            try {
                iterator.remove();
            } catch (UnsupportedOperationException e) {
            }
        }

        assertEquals(10, queue.size());
    }

    @Test
    public void testToArray() {
        final String name = "testToArray";
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
                queue.offer("waiting", 2, TimeUnit.SECONDS);
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
