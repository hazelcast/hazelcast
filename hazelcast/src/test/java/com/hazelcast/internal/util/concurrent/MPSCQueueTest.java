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

package com.hazelcast.internal.util.concurrent;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MPSCQueueTest extends HazelcastTestSupport {

    private MPSCQueue<String> queue;

    @Before
    public void setup() {
        this.queue = new MPSCQueue<String>(new BusySpinIdleStrategy());
    }

    @Test(expected = NullPointerException.class)
    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    public void setOwningThread_whenNull() {
        MPSCQueue queue = new MPSCQueue(new BusySpinIdleStrategy());
        queue.setConsumerThread(null);
    }

    // ============== poll ==========================================

    @Test
    public void poll() throws InterruptedException {
        queue.setConsumerThread(Thread.currentThread());

        queue.offer("1");
        queue.offer("2");

        assertEquals("1", queue.poll());
        assertEquals("2", queue.poll());
    }

    @Test
    public void poll_whenEmpty() {
        queue.setConsumerThread(Thread.currentThread());

        assertNull(queue.poll());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void pollWithTimeout_thenUnsupportedOperation() throws InterruptedException {
        queue.setConsumerThread(Thread.currentThread());

        queue.poll(1, TimeUnit.SECONDS);
    }

    // ============== take ==========================================

    @Test
    public void take_whenItemAvailable() throws Exception {
        queue.setConsumerThread(Thread.currentThread());

        queue.offer("1");
        queue.offer("2");

        assertEquals("1", queue.take());
        assertEquals("2", queue.take());
    }

    @Test(expected = InterruptedException.class)
    public void take_whenInterruptedWhileWaiting() throws Exception {
        final Thread owningThread = Thread.currentThread();
        queue.setConsumerThread(owningThread);

        spawn(new Runnable() {
            @Override
            public void run() {
                sleepSeconds(3);
                owningThread.interrupt();
            }
        });

        queue.take();
    }

    @Test
    public void take_whenItemAvailableAfterSomeBlocking() throws Exception {
        queue.setConsumerThread(Thread.currentThread());

        spawn(new Runnable() {
            @Override
            public void run() {
                sleepSeconds(3);
                queue.offer("1");
            }
        });

        Object item = queue.take();
        assertEquals("1", item);
    }

    /**
     * A test that verifies if the array is expanded.
     */
    @Test
    public void take_whenManyItems() throws Exception {
        queue.setConsumerThread(Thread.currentThread());

        int count = MPSCQueue.INITIAL_ARRAY_SIZE * 10;
        for (int k = 0; k < count; k++) {
            queue.add("item" + k);
        }

        for (int k = 0; k < count; k++) {
            assertEquals("item" + k, queue.take());
        }

        assertEquals(0, queue.size());
    }

    // ============= isEmpty ====================================

    @Test
    public void isEmpty_whenEmpty() {
        assertTrue(queue.isEmpty());
    }

    @Test
    public void isEmpty_whenSomeItemsOnPutStack() throws InterruptedException {
        queue.put("item1");
        assertFalse(queue.isEmpty());

        queue.put("item2");
        assertFalse(queue.isEmpty());
    }

    @Test
    public void isEmpty_whenSomeItemsOnTakeStack() throws InterruptedException {
        queue.setConsumerThread(Thread.currentThread());

        queue.put("item1");
        queue.put("item2");
        queue.put("item3");
        queue.take();
        assertFalse(queue.isEmpty());
    }

    @Test
    public void isEmpty_whenSomeItemsOnTakeStackAndSomeOnPutStack() throws InterruptedException {
        queue.setConsumerThread(Thread.currentThread());

        queue.put("item1");
        queue.put("item2");
        queue.put("item3");
        queue.take();

        queue.put("item4");
        queue.put("item5");

        assertFalse(queue.isEmpty());
    }

    // ============= size ====================================

    @Test
    public void size_whenEmpty() {
        assertEquals(0, queue.size());
    }

    @Test
    public void size_whenSomeItemsOnPutStack() throws InterruptedException {
        queue.setConsumerThread(Thread.currentThread());

        queue.put("item1");
        assertEquals(1, queue.size());

        queue.put("item2");
        assertEquals(2, queue.size());
    }

    @Test
    public void size_whenSomeItemsOnTakeStack() throws InterruptedException {
        queue.setConsumerThread(Thread.currentThread());

        queue.put("item1");
        queue.put("item2");
        queue.put("item3");
        queue.take();
        assertEquals(2, queue.size());
    }

    @Test
    public void size_whenSomeItemsOnTakeStackAndSomeOnPutStack() throws InterruptedException {
        queue.setConsumerThread(Thread.currentThread());

        queue.put("item1");
        queue.put("item2");
        queue.put("item3");
        queue.take();

        queue.put("item4");
        queue.put("item5");

        assertEquals(4, queue.size());
    }

    @Test
    public void size_whenTakeStackEmptyAgain() throws InterruptedException {
        queue.setConsumerThread(Thread.currentThread());

        queue.put("item1");
        queue.put("item2");
        queue.put("item3");
        queue.take();
        queue.take();
        queue.take();

        assertEquals(0, queue.size());
    }

    // ============= offer ====================================

    @Test
    public void offer_withTimeout() throws InterruptedException {
        assertTrue(queue.offer("item1", 1, MINUTES));
        assertTrue(queue.offer("item2", 1, MINUTES));
        assertTrue(queue.offer("item3", 3, MINUTES));

        assertEquals(3, queue.size());
    }

    @Test
    public void offer_noTimeout() throws InterruptedException {
        assertTrue(queue.offer("item1"));
        assertTrue(queue.offer("item2"));
        assertTrue(queue.offer("item3"));

        assertEquals(3, queue.size());
    }

    // ============= drain ====================================


    @Test(expected = UnsupportedOperationException.class)
    public void drain() {
        queue.drainTo(new LinkedList<String>());
    }


    @Test(expected = UnsupportedOperationException.class)
    public void drainMaxItems() {
        queue.drainTo(new LinkedList<String>(), 10);
    }

    // ============= clear ====================================

    @Test
    public void clear_whenEmpty() {
        queue.setConsumerThread(Thread.currentThread());

        queue.clear();

        assertEquals(0, queue.size());
        assertSame(MPSCQueue.BLOCKED, queue.putStack.get());
    }

    @Test
    public void clear_whenThreadWaiting() {
        queue.setConsumerThread(Thread.currentThread());

        queue.putStack.set(MPSCQueue.BLOCKED);

        queue.clear();

        assertEquals(0, queue.size());
        assertSame(MPSCQueue.BLOCKED, queue.putStack.get());
    }

    @Test
    public void clear_whenItemsOnPutStack() {
        queue.setConsumerThread(Thread.currentThread());

        queue.offer("1");
        queue.offer("2");
        queue.clear();

        assertEquals(0, queue.size());
        assertSame(MPSCQueue.BLOCKED, queue.putStack.get());
    }

    @Test
    public void clear_whenItemsOnTakeStack() throws Exception {
        queue.setConsumerThread(Thread.currentThread());

        queue.offer("1");
        queue.offer("2");
        queue.offer("3");
        // copies the putStack into the takeStack
        queue.take();

        queue.clear();

        // since 1 item was taken, 2 items are remaining
        assertEquals(2, queue.size());
        assertSame(MPSCQueue.BLOCKED, queue.putStack.get());
    }

    @Test
    public void clear_whenItemsOnBothStacks() throws Exception {
        queue.setConsumerThread(Thread.currentThread());

        queue.offer("1");
        queue.offer("2");
        queue.offer("3");
        // copies the putStack into the takeStack
        queue.take();

        queue.offer("5");
        queue.offer("6");

        queue.clear();

        // since 1 item was taken, 2 items are remaining
        assertEquals(2, queue.size());
        assertSame(MPSCQueue.BLOCKED, queue.putStack.get());
    }

    // ============= misc ====================================

    @Test
    public void when_peek_then_getButNotRemove() {
        queue.offer("1");
        assertEquals("1", queue.peek());
        assertEquals("1", queue.peek());
    }

    @Test
    public void when_peekEmpty_then_Null() {
        assertNull(queue.peek());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void iterator_whenCalled_thenUnsupportedOperationException() {
        queue.iterator();
    }

    @Test
    public void remainingCapacity() {
        assertEquals(Integer.MAX_VALUE, queue.remainingCapacity());
    }
}
