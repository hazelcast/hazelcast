package com.hazelcast.spi.impl.operationexecutor.classic;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class DefaultScheduleQueueTest extends HazelcastTestSupport {

    private DefaultScheduleQueue queue;
    private LinkedBlockingQueue normalQueue;
    private ConcurrentLinkedQueue priorityQueue;

    @Before
    public void setup() {
        normalQueue = new LinkedBlockingQueue();
        priorityQueue = new ConcurrentLinkedQueue();
        queue = new DefaultScheduleQueue(normalQueue, priorityQueue);
    }

    // ================== poll =====================

    @Test
    public void poll_whenEmpty() {
        Object task = queue.poll();

        assertNull(task);
    }

    @Test
    public void poll_whenOnlyPriorityTask() {
        Object task = new Object();
        queue.addUrgent(task);

        assertSame(task, queue.poll());
        assertNull(queue.poll());
    }

    @Test
    public void poll_whenOnlyOrdinaryTask() {
        Object task = new Object();
        queue.add(task);

        assertSame(task, queue.poll());
        assertNull(queue.poll());
    }

    @Test
    public void poll_whenPriorityAndOrdinaryTask() {
        Object ordinaryTask = new Object();
        queue.add(ordinaryTask);
        Object priorityTask = new Object();
        queue.addUrgent(priorityTask);

        assertSame(priorityTask, queue.poll());
        assertSame(ordinaryTask, queue.poll());
        assertNull(queue.poll());
    }

    // ================== add =====================

    @Test(expected = NullPointerException.class)
    public void add_whenNull() {
        queue.add(null);
    }

    @Test
    public void add_whenPriority() {

        Object task = new Object();
        queue.addUrgent(task);

        assertContent(priorityQueue, task);
        assertContent(normalQueue, DefaultScheduleQueue.TRIGGER_TASK);
        assertEquals(1, queue.prioritySize());
        assertEquals(1, queue.normalSize());
        assertEquals(2, queue.size());
    }

    @Test
    public void add_whenNormal() {
        Object task = new Object();

        queue.add(task);

        assertContent(normalQueue, task);
        assertEmpty(priorityQueue);
        assertEquals(0, queue.prioritySize());
        assertEquals(1, queue.normalSize());
        assertEquals(1, queue.size());
    }

    // ================== take =====================

    @Test
    public void take_priorityIsRetrievedFirst() throws InterruptedException {
        Object priorityTask1 = "priority1";
        Object priorityTask2 = "priority2";
        Object priorityTask3 = "priority4";

        Object normalTask1 = "normalTask1";
        Object normalTask2 = "normalTask2";
        Object normalTask3 = "normalTask3";

        queue.addUrgent(priorityTask1);
        queue.add(normalTask1);
        queue.add(normalTask2);

        queue.addUrgent(priorityTask2);
        queue.add(normalTask3);
        queue.addUrgent(priorityTask3);

        assertSame(priorityTask1, queue.take());
        assertSame(priorityTask2, queue.take());
        assertSame(priorityTask3, queue.take());
        assertSame(normalTask1, queue.take());
        assertSame(normalTask2, queue.take());
        assertSame(normalTask3, queue.take());

        assertEmpty(priorityQueue);
        assertContent(normalQueue, DefaultScheduleQueue.TRIGGER_TASK);
    }


    public void assertEmpty(Queue q) {
        assertEquals("expecting an empty queue, but the queue is:" + q, 0, q.size());
    }

    private void assertContent(Queue q, Object... expected) {
        assertEquals(asList(expected), new LinkedList(q));
    }
}
