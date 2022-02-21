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

package com.hazelcast.collection.impl.queue;

import com.hazelcast.collection.IQueue;
import com.hazelcast.collection.impl.queue.model.VersionedObject;
import com.hazelcast.collection.impl.queue.model.VersionedObjectComparator;
import com.hazelcast.config.Config;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.test.AbstractHazelcastClassRunner.getTestMethodName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class QueueAbstractTest extends HazelcastTestSupport {

    @Parameterized.Parameters(name = "comparatorClassName: {0}")
    public static Collection<Object> parameters() {
        return Arrays.asList(new Object[]{null, VersionedObjectComparator.class.getName()});
    }

    @Parameterized.Parameter
    public String comparatorClassName;

    protected HazelcastInstance[] instances;
    protected IAtomicLong atomicLong;
    private IQueue<VersionedObject<String>> queue;
    private QueueConfig queueConfig;

    @Before
    public void setup() {
        Config config = smallInstanceConfig();
        config.getQueueConfig("default")
              .setPriorityComparatorClassName(comparatorClassName);
        config.getQueueConfig("testOffer_whenFull*")
              .setMaxSize(100);
        config.getQueueConfig("testOfferWithTimeout*")
              .setMaxSize(100);

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
    public void testOffer() {
        int count = 100;
        for (int i = 0; i < count; i++) {
            queue.offer(new VersionedObject<>("item" + i, i));
        }

        assertEquals(100, queue.size());
    }

    @Test
    public void testOffer_whenFull() {
        for (int i = 0; i < queueConfig.getMaxSize(); i++) {
            queue.offer(new VersionedObject<>("item" + i, i));
        }

        boolean accepted = queue.offer(new VersionedObject<>("rejected"));
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
            queue.offer(new VersionedObject<>("item" + i, i));
        }

        assertFalse(queue.offer(new VersionedObject<>("rejected")));
        offerThread.start();
        queue.poll();
        assertSizeEventually(queueConfig.getMaxSize(), queue);
        assertContains(queue, new VersionedObject<>("waiting"));
    }

    // ================ poll ==============================

    @Test
    public void testPoll() {
        int count = 100;
        for (int i = 0; i < count; i++) {
            queue.offer(new VersionedObject<>("item" + i, i));
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
    public void testPollWithTimeout() {
        PollThread pollThread = new PollThread(queue);
        pollThread.start();
        queue.offer(new VersionedObject<>("offer"));
        queue.offer(new VersionedObject<>("remain"));

        assertSizeEventually(1, queue);
        assertContains(queue, new VersionedObject<>("remain"));
    }

    // ================ remove ==============================

    @Test
    public void testRemove() {
        for (int i = 0; i < 10; i++) {
            queue.offer(new VersionedObject<>("item" + i, i));
        }

        assertTrue(queue.remove(new VersionedObject<>("item4", 4)));
        assertEquals(queue.size(), 9);
    }

    @Test
    public void testRemove_whenElementNotExists() {
        for (int i = 0; i < 10; i++) {
            queue.offer(new VersionedObject<>("item" + i, i));
        }

        assertFalse(queue.remove(new VersionedObject<>("item13", 13)));
        assertEquals(10, queue.size());
    }

    @Test
    public void testRemove_whenQueueEmpty() {
        assertFalse(queue.remove(new VersionedObject<>("not in Queue")));
    }

    @Test
    public void testRemove_whenArgNull() {
        queue.add(new VersionedObject<>("foo"));

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
            queue.offer(new VersionedObject<>("item" + i, i));
        }
        List<VersionedObject<String>> list = new ArrayList<>(10);

        assertEquals(10, queue.drainTo(list));
        assertEquals(10, list.size());
        assertEquals(new VersionedObject<>("item0", 0), list.get(0));
        assertEquals(new VersionedObject<>("item5", 5), list.get(5));
        assertEquals(0, queue.size());
    }

    @Test
    public void testDrainTo_whenQueueEmpty() {
        List<VersionedObject<String>> list = new ArrayList<>();

        assertEquals(0, queue.drainTo(list));
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void testDrainTo_whenCollectionNull() {
        for (int i = 0; i < 10; i++) {
            queue.offer(new VersionedObject<>("item" + i, i));
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
            queue.offer(new VersionedObject<>("item" + i, i));
        }
        List<VersionedObject<String>> list = new ArrayList<>(10);

        queue.drainTo(list, 4);
        assertEquals(4, list.size());
        assertContains(list, new VersionedObject<>("item3", 3));
        assertEquals(6, queue.size());
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void testDrainToWithMaxElement_whenCollectionNull() {
        for (int i = 0; i < 10; i++) {
            queue.offer(new VersionedObject<>("item" + i, i));
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
            queue.offer(new VersionedObject<>("item" + i, i));
        }
        List<VersionedObject<String>> list = new ArrayList<>();

        assertEquals(10, queue.drainTo(list, -4));
        assertEquals(0, queue.size());
    }

    // ================ contains ==============================

    @Test
    public void testContains_whenExists() {
        for (int i = 0; i < 10; i++) {
            queue.offer(new VersionedObject<>("item" + i, i));
        }

        assertContains(queue, new VersionedObject<>("item4", 4));
        assertContains(queue, new VersionedObject<>("item8", 8));
    }

    @Test
    public void testContains_whenNotExists() {
        for (int i = 0; i < 10; i++) {
            queue.offer(new VersionedObject<>("item" + i, i));
        }

        assertNotContains(queue, new VersionedObject<>("item10", 10));
        assertNotContains(queue, new VersionedObject<>("item19", 19));
    }

    // ================ containsAll ==============================

    @Test
    public void testAddAll_whenCollectionContainsNull() {
        for (int i = 0; i < 10; i++) {
            queue.offer(new VersionedObject<>("item" + i, i));
        }
        List<VersionedObject<String>> list = new ArrayList<>();
        list.add(new VersionedObject<>("item10"));
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
            queue.offer(new VersionedObject<>("item" + i, i));
        }

        List<VersionedObject<String>> list = new ArrayList<>();
        list.add(new VersionedObject<>("item1", 1));
        list.add(new VersionedObject<>("item2", 2));
        list.add(new VersionedObject<>("item3", 3));
        assertContainsAll(queue, list);
    }

    @Test
    public void testContainsAll_whenNoneExists() {
        for (int i = 0; i < 10; i++) {
            queue.offer(new VersionedObject<>("item" + i, i));
        }

        List<VersionedObject<String>> list = new ArrayList<>();
        list.add(new VersionedObject<>("item10", 10));
        list.add(new VersionedObject<>("item11", 11));
        list.add(new VersionedObject<>("item12", 12));
        assertNotContainsAll(queue, list);
    }

    @Test
    public void testContainsAll_whenSomeExists() {
        for (int i = 0; i < 10; i++) {
            queue.offer(new VersionedObject<>("item" + i, i));
        }

        List<VersionedObject<String>> list = new ArrayList<>();
        list.add(new VersionedObject<>("item1", 1));
        list.add(new VersionedObject<>("item2", 2));
        list.add(new VersionedObject<>("item14", 14));
        list.add(new VersionedObject<>("item13", 13));
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
        List<VersionedObject<String>> list = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            list.add(new VersionedObject<>("item" + i, i));
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
            queue.offer(new VersionedObject<>("item" + i, i));
        }
        assertEquals(10, queue.size());
        assertTrue(queue.addAll(Collections.emptyList()));
        assertEquals(10, queue.size());
    }

    @Test
    public void testAddAll_whenDuplicateItems() {
        for (int i = 0; i < 10; i++) {
            queue.offer(new VersionedObject<>("item" + i, i));
        }
        List<VersionedObject<String>> list = new ArrayList<>();
        list.add(new VersionedObject<>("item3"));

        assertContains(queue, new VersionedObject<>("item3", 3));
        queue.addAll(list);
        assertEquals(11, queue.size());
    }

    // ================ retainAll ==============================

    @Test
    public void testRetainAll() {
        queue.add(new VersionedObject<>("item3"));
        queue.add(new VersionedObject<>("item4"));
        queue.add(new VersionedObject<>("item5"));

        List<VersionedObject<String>> arrayList = new ArrayList<>();
        arrayList.add(new VersionedObject<>("item3"));
        arrayList.add(new VersionedObject<>("item4"));
        arrayList.add(new VersionedObject<>("item31"));
        assertTrue(queue.retainAll(arrayList));
        assertEquals(queue.size(), 2);
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void testRetainAll_whenCollectionNull() {
        queue.add(new VersionedObject<>("item3"));
        queue.add(new VersionedObject<>("item4"));
        queue.add(new VersionedObject<>("item5"));

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
        queue.add(new VersionedObject<>("item3"));
        queue.add(new VersionedObject<>("item4"));
        queue.add(new VersionedObject<>("item5"));

        assertTrue(queue.retainAll(Collections.<VersionedObject<String>>emptyList()));
        assertEquals(0, queue.size());
    }

    @Test
    public void testRetainAll_whenCollectionContainsNull() {
        queue.add(new VersionedObject<>("item3"));
        queue.add(new VersionedObject<>("item4"));
        queue.add(new VersionedObject<>("item5"));
        List<VersionedObject<String>> list = new ArrayList<>();
        list.add(null);

        assertTrue(queue.retainAll(list));
        assertEquals(0, queue.size());
    }

    // ================ removeAll ==============================

    @Test
    public void testRemoveAll() {
        queue.add(new VersionedObject<>("item3"));
        queue.add(new VersionedObject<>("item4"));
        queue.add(new VersionedObject<>("item5"));

        List<VersionedObject<String>> arrayList = new ArrayList<>();
        arrayList.add(new VersionedObject<>("item3"));
        arrayList.add(new VersionedObject<>("item4"));
        arrayList.add(new VersionedObject<>("item5"));
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
        queue.add(new VersionedObject<>("item3"));
        queue.add(new VersionedObject<>("item4"));
        queue.add(new VersionedObject<>("item5"));

        assertFalse(queue.removeAll(Collections.<VersionedObject<String>>emptyList()));
        assertEquals(3, queue.size());
    }

    // ================ toArray ==============================

    @Test
    public void testToArray() {
        for (int i = 0; i < 10; i++) {
            queue.offer(new VersionedObject<>("item" + i, i));
        }

        Object[] array = queue.toArray();
        for (int i = 0; i < array.length; i++) {
            Object o = array[i];
            int id = i++;
            assertEquals(o, new VersionedObject<>("item" + id, id));
        }
        @SuppressWarnings("unchecked")
        VersionedObject<String>[] arr = queue.toArray(new VersionedObject[0]);
        assertEquals(arr.length, 10);
        for (int i = 0; i < arr.length; i++) {
            Object o = arr[i];
            int id = i++;
            assertEquals(o, new VersionedObject<>("item" + id, id));
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testQueueRemoveFromIterator() {
        queue.add(new VersionedObject<>("one"));
        Iterator<VersionedObject<String>> iterator = queue.iterator();
        iterator.next();
        iterator.remove();
    }

    private static class OfferThread extends Thread {

        IQueue<VersionedObject<String>> queue;

        OfferThread(IQueue<VersionedObject<String>> queue) {
            this.queue = queue;
        }

        @Override
        public void run() {
            try {
                queue.offer(new VersionedObject<>("waiting"), 1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static class PollThread extends Thread {

        IQueue<?> queue;

        PollThread(IQueue<?> queue) {
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
