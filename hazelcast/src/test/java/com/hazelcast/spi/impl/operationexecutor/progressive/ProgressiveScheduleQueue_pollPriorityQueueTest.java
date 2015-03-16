package com.hazelcast.spi.impl.operationexecutor.progressive;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ProgressiveScheduleQueue_pollPriorityQueueTest extends ProgressiveScheduleQueueAbstractTest {

    @Test
    public void whenNoItemAvailable_thenNull() {
        beforeTest();
        PartitionQueue actual = scheduleQueue.pollNextPriorityQueue();

        assertNull(actual);
        assertNoHeadChange();
        assertNoBuffersChanges();
    }

    @Test
    public void whenBufferedLowPriority_thenNull() {
        PartitionQueue pq = new PartitionQueue(0, scheduleQueue, operationRunner);

        UnparkNode node = new UnparkNode(pq, false);
        node.normalSize = 1;

        scheduleQueue.appendToBuffers(node);

        beforeTest();
        PartitionQueue actual = scheduleQueue.pollNextPriorityQueue();

        assertNull(actual);
        assertNoHeadChange();
        assertNoBuffersChanges();
    }

    @Test
    public void whenBufferedHighPriority_thenItemReturned() {
        PartitionQueue queue1 = new PartitionQueue(0, scheduleQueue, operationRunner);
        PartitionQueue queue2 = new PartitionQueue(0, scheduleQueue, operationRunner);

        UnparkNode node1 = new UnparkNode(queue1, true);
        node1.prioritySize = 1;

        UnparkNode node2 = new UnparkNode(queue2, true);
        node2.prioritySize = 2;
        node2.prev = node1;

        scheduleQueue.appendToBuffers(node2);

        beforeTest();
        PartitionQueue first = scheduleQueue.pollNextPriorityQueue();

        assertSame(first, queue1);
        assertNoHeadChange();
        assertNoLowPriorityBufferChanges();

        assertEquals(previousPriorityBufferSize, scheduleQueue.priorityBufferSize);
        assertEquals(previousPriorityBufferPos + 1, scheduleQueue.priorityBufferPos);

        PartitionQueue second = scheduleQueue.pollNextPriorityQueue();

        assertSame(second, queue2);
        assertNoHeadChange();
        assertNoLowPriorityBufferChanges();

        assertEquals(0, scheduleQueue.priorityBufferSize);
        assertEquals(0, scheduleQueue.priorityBufferPos);
    }

    @Test
    public void whenPendingLowPriority_thenNothingHappensAndNullReturned() {
        PartitionQueue pq = new PartitionQueue(0, scheduleQueue, operationRunner);

        UnparkNode node = new UnparkNode(pq, false);
        node.normalSize = 1;

        scheduleQueue.head.set(node);

        beforeTest();
        PartitionQueue actual = scheduleQueue.pollNextPriorityQueue();

        assertNull(actual);
        assertNoHeadChange();
        assertNoBuffersChanges();
    }

    @Test
    public void whenPendingHighPriority_thenWorkPulledAndItemReturned() {
        PartitionQueue pq = new PartitionQueue(0, scheduleQueue, operationRunner);

        UnparkNode node = new UnparkNode(pq, true);
        node.prioritySize = 1;

        scheduleQueue.head.set(node);

        beforeTest();
        PartitionQueue actual = scheduleQueue.pollNextPriorityQueue();

        assertSame(pq, actual);
        assertHead(null);

        // the buffer was changed, but because the item was fully processed,
        // the buffers have reverted to their original state.
        assertNoBuffersChanges();
    }

    @Test
    public void testOrdering() {
        //todo
    }


}
