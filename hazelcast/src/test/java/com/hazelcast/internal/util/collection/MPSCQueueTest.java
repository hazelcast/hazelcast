package com.hazelcast.internal.util.collection;

import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.concurrent.NoOpIdleStrategy;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class MPSCQueueTest extends HazelcastTestSupport {

    @Test
    public void foo() {
        MPSCQueue<Integer> queue = new MPSCQueue<Integer>(Thread.currentThread(), new NoOpIdleStrategy());

        int item = 1;
        int lastRetrievedItem = 0;
        Random random = new Random();
        for (int iteration = 0; iteration < 10; iteration++) {
            boolean iterationDone = false;
            while (!iterationDone) {
                // we add some
                for (int k = 0; k < random.nextInt(50); k++) {
                    queue.add(item);
                    item++;
                }

                // we remove some
                for (int k = 0; k < random.nextInt(5); k++) {
                    Integer found = queue.poll();
                    if (found == null) {
                        iterationDone = true;
                        break;
                    }
                    assertEquals(lastRetrievedItem + 1, found.intValue());
                    lastRetrievedItem = found.intValue();
                }


                // once and a while we do a full drain
                if (random.nextInt(50) == 0) {
                    for (; ; ) {
                        Integer found = queue.poll();
                        if (found == null) {
                            iterationDone = true;
                            break;
                        }
                        assertEquals(lastRetrievedItem + 1, found.intValue());
                        lastRetrievedItem = found.intValue();
                    }
                }
            }
        }
    }

    @Test
    public void clear_whenEmpty(){
        MPSCQueue queue = new MPSCQueue(Thread.currentThread(), new NoOpIdleStrategy());
        queue.clear();

        assertSame(MPSCQueue.BLOCKED, queue.putStack.get());
        assertEquals(0, queue.size());
    }

    @Test
    public void clear_whenItemsOnPutStack(){
        MPSCQueue queue = new MPSCQueue(Thread.currentThread(), new NoOpIdleStrategy());
        queue.add(1);
        queue.add(2);
        queue.clear();

        assertSame(MPSCQueue.BLOCKED, queue.putStack.get());
        assertEquals(0, queue.size());
    }

    public void clear_whenItemsOnTakeStack_thenTheseAreNotCleared(){

    }

    @Test
    public void arrayIncrease() {
        MPSCQueue queue = new MPSCQueue(Thread.currentThread(), new NoOpIdleStrategy());
        for (int k = 0; k < 10 * MPSCQueue.INITIAL_ARRAY_SIZE; k++) {
            queue.add(k);
        }

        for (int k = 0; k < 10 * MPSCQueue.INITIAL_ARRAY_SIZE; k++) {
            assertEquals(new Integer(k), queue.poll());
        }

        assertNull(queue.poll());
    }

    @Test(expected = NullPointerException.class)
    public void offer_whenNull() {
        MPSCQueue queue = new MPSCQueue(null, new NoOpIdleStrategy());
        queue.offer(null);
    }

    @Test
    public void offer_whenNotNull() {
        MPSCQueue queue = new MPSCQueue(null, new NoOpIdleStrategy());
        assertTrue(queue.offer(1));
        assertTrue(queue.offer(2));

        assertEquals(2, queue.size());
    }

    @Test
    public void poll_whenEmpty() {
        MPSCQueue queue = new MPSCQueue(null, new NoOpIdleStrategy());
        assertNull(queue.poll());
    }

    @Test
    public void poll_whenNotEmpty() {
        MPSCQueue queue = new MPSCQueue(null, new NoOpIdleStrategy());
        queue.offer(1);
        queue.offer(2);
        queue.offer(3);

        assertEquals(1, queue.poll());
        assertEquals(0, queue.size());

        assertEquals(2, queue.poll());
        assertEquals(0, queue.size());

        assertEquals(3, queue.poll());
        assertEquals(0, queue.size());
    }

    @Test
    public void take_whenNotEmpty() throws Exception {
        MPSCQueue queue = new MPSCQueue(Thread.currentThread(), new NoOpIdleStrategy());
        queue.offer(1);
        queue.offer(2);
        queue.offer(3);

        assertEquals(1, queue.take());
        assertEquals(0, queue.size());

        assertEquals(2, queue.take());
        assertEquals(0, queue.size());

        assertEquals(3, queue.take());
        assertEquals(0, queue.size());
    }

    @Test
    public void take_whenEmpty() {
        final AtomicReference result = new AtomicReference();
        final MPSCQueue queue = new MPSCQueue(null, new NoOpIdleStrategy());
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    result.set(queue.take());
                } catch (Throwable t) {
                    result.set(t);
                }
            }
        });
        queue.setOwningThread(thread);
        thread.start();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertSame(MPSCQueue.BLOCKED, queue.putStack.get());
            }
        });

        // give the thread some time to really fall asleep.
        sleepSeconds(2);

        final Object response = 123456;
        queue.add(response);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(response, result.get());
            }
        });
    }

    @Test
    public void testRemainingCapacity() {
        MPSCQueue queue = new MPSCQueue(Thread.currentThread(), new NoOpIdleStrategy());
        assertEquals(Integer.MAX_VALUE, queue.remainingCapacity());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testDrainTo() {
        MPSCQueue queue = new MPSCQueue(Thread.currentThread(), new NoOpIdleStrategy());
        queue.drainTo(new LinkedList());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testDrainTo2() {
        MPSCQueue queue = new MPSCQueue(Thread.currentThread(), new NoOpIdleStrategy());
        queue.drainTo(new LinkedList(), 0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testIterator() {
        MPSCQueue queue = new MPSCQueue(Thread.currentThread(), new NoOpIdleStrategy());
        queue.iterator();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testPeek() {
        MPSCQueue queue = new MPSCQueue(Thread.currentThread(), new NoOpIdleStrategy());
        queue.peek();
    }
}
