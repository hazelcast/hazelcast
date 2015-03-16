package com.hazelcast.spi.impl.operationexecutor.progressive;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ProgressiveScheduleQueue_unparkTest extends ProgressiveScheduleQueueAbstractTest {

    @Test(expected = NullPointerException.class)
    public void whenNull_thenNullPointerException() {
        scheduleQueue.unpark(null);
    }

    @Test(expected = IllegalStateException.class)
    public void whenExecuting_thenIllegalStateException() {
        PartitionQueue partitionQueue = new PartitionQueue(0, scheduleQueue, operationRunner);
        partitionQueue.head.set(Node.EXECUTING);

        scheduleQueue.unpark(partitionQueue);
    }

    @Test(expected = IllegalStateException.class)
    public void whenStolen_thenIllegalStateException() {
        PartitionQueue partitionQueue = new PartitionQueue(0, scheduleQueue, operationRunner);
        partitionQueue.head.set(Node.STOLEN_UNPARKED);

        scheduleQueue.unpark(partitionQueue);
    }

    @Test(expected = IllegalStateException.class)
    public void whenParked_thenIllegalStateException() {
        PartitionQueue partitionQueue = new PartitionQueue(0, scheduleQueue, operationRunner);
        partitionQueue.head.set(Node.PARKED);

        scheduleQueue.unpark(partitionQueue);
    }

    @Test(expected = IllegalStateException.class)
    public void whenStolenUnparked_thenIllegalStateException() {
        PartitionQueue partitionQueue = new PartitionQueue(0, scheduleQueue, operationRunner);
        partitionQueue.head.set(Node.STOLEN_UNPARKED);

        scheduleQueue.unpark(partitionQueue);
    }

    @Test
    public void whenFirst_thenNotify() {
        PartitionQueue partitionQueue = new PartitionQueue(0, scheduleQueue, operationRunner);

        Node node = new Node();
        node.state = PartitionQueueState.Unparked;
        node.task = new MockPartitionOperation();
        node.normalSize = 1;
        partitionQueue.head.set(node);

        scheduleQueue.unpark(partitionQueue);
        assertSame(partitionQueue.unparkNode, scheduleQueue.head.get());
        assertNull(partitionQueue.unparkNode.prev);
    }

    //todo: also test the blocking and notification

    @Test
    public void whenNotFirst() {
        PartitionQueue first = new PartitionQueue(0, scheduleQueue, operationRunner);
        first.head.set(Node.UNPARKED);

        PartitionQueue second = new PartitionQueue(0, scheduleQueue, operationRunner);
        second.head.set(Node.UNPARKED);

        scheduleQueue.unpark(first);
        scheduleQueue.unpark(second);

        assertSame(second.unparkNode, scheduleQueue.head.get());
        assertSame(first.unparkNode, second.unparkNode.prev);
        assertNull(first.unparkNode.prev);
    }
}
