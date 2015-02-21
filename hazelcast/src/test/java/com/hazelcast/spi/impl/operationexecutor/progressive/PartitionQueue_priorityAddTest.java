package com.hazelcast.spi.impl.operationexecutor.progressive;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.spi.impl.operationexecutor.progressive.PartitionQueueState.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class PartitionQueue_priorityAddTest  extends PartitionQueueAbstractTest {

    @Test(expected = NullPointerException.class)
    public void whenNull_thenNullPointerException() {
        partitionQueue.priorityAdd(null);
    }

    @Test
    public void whenParked_thenPriorityUnpark() {
        MockPartitionOperation operation = new MockPartitionOperation();

        beforeTest();
        partitionQueue.priorityAdd(operation);

        assertEquals(0, operation.counter.get());
        assertPriorityNodeAdded(UnparkedPriority,operation);
        assertPriorityUnparkNodeAdded();
    }

    @Test
    public void whenUnparked_andPendingWork_thenRemainUnparked() throws Exception {
        MockPartitionOperation operation1 = new MockPartitionOperation();
        partitionQueue.add(operation1);
        MockPartitionOperation operation2 = new MockPartitionOperation();

        beforeTest();
        partitionQueue.priorityAdd(operation2);

        assertEquals(0, operation1.counter.get());
        assertEquals(0, operation2.counter.get());
        assertPriorityNodeAdded(Unparked, operation2);
        assertPriorityUnparkNodeAdded();
    }

    @Test
    public void whenUnparked_andNoPendingWork_thenRemainUnparked() {
        partitionQueue.head.set(Node.UNPARKED);
        MockPartitionOperation operation = new MockPartitionOperation();

        beforeTest();
        partitionQueue.priorityAdd(operation);

        assertEquals(0, operation.counter.get());
        assertPriorityNodeAdded(Unparked, operation);
        assertPriorityUnparkNodeAdded();
    }

    @Test
    public void whenPriorityUnparked_thenRemainUnparked() throws Exception {
        MockPartitionOperation operation1 = new MockPartitionOperation();
        partitionQueue.priorityAdd(operation1);
        previousUnparkNode = scheduleQueue.head.get();
        MockPartitionOperation operation2 = new MockPartitionOperation();

        beforeTest();
        partitionQueue.priorityAdd(operation2);

        assertEquals(0, operation1.counter.get());
        assertEquals(0, operation2.counter.get());
        assertPriorityNodeAdded(UnparkedPriority, operation2);
        assertPriorityUnparkNodeAdded();
    }


    @Test
    public void whenExecuting_thenRemainExecuting() throws Exception {
        partitionQueue.head.set(Node.EXECUTING);
        MockPartitionOperation op = new MockPartitionOperation();

        beforeTest();
        partitionQueue.priorityAdd(op);

        assertEquals(0, op.counter.get());
        assertPriorityNodeAdded(Executing, op);
        assertPriorityUnparkNodeAdded();
    }

    @Test
    public void whenStolen_thenRemainExecuting() throws Exception {
        partitionQueue.head.set(Node.STOLEN);
        MockPartitionOperation op = new MockPartitionOperation();

        beforeTest();
        partitionQueue.priorityAdd(op);

        assertEquals(0, op.counter.get());
        assertPriorityNodeAdded(Stolen, op);
        assertPriorityUnparkNodeAdded();
    }

    @Test
    public void whenStolenUnparked_thenRemainUnparkedStolen() {
        partitionQueue.head.set(Node.STOLEN_UNPARKED);
        MockPartitionOperation op = new MockPartitionOperation();

        beforeTest();
        partitionQueue.priorityAdd(op);

        assertEquals(0, op.counter.get());
        assertPriorityNodeAdded(StolenUnparked, op);
        assertPriorityUnparkNodeAdded();
    }
}
