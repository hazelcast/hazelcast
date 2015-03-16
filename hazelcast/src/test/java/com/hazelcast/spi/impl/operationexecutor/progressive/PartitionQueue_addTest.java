package com.hazelcast.spi.impl.operationexecutor.progressive;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.spi.impl.operationexecutor.progressive.PartitionQueueState.Executing;
import static com.hazelcast.spi.impl.operationexecutor.progressive.PartitionQueueState.Stolen;
import static com.hazelcast.spi.impl.operationexecutor.progressive.PartitionQueueState.StolenUnparked;
import static com.hazelcast.spi.impl.operationexecutor.progressive.PartitionQueueState.Unparked;
import static org.junit.Assert.assertEquals;


@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class PartitionQueue_addTest extends PartitionQueueAbstractTest {

    @Test(expected = NullPointerException.class)
    public void whenNull_thenNullPointerException() {
        partitionQueue.add(null);
    }

    //todo: we need to check the buffers

    @Test
    public void whenParked_thenUnparked() throws Exception {
        MockPartitionOperation operation = new MockPartitionOperation();

        beforeTest();
        partitionQueue.add(operation);

        assertEquals(0, operation.counter.get());

        assertNodeAdded(Unparked, operation);
        assertUnparkNodeAdded();
    }

    @Test
    public void whenUnparked_andNoPending_thenRemainUnparked() throws Exception {
        partitionQueue.head.set(Node.UNPARKED);
        MockPartitionOperation op = new MockPartitionOperation();

        beforeTest();
        partitionQueue.add(op);

        assertEquals(0, op.counter.get());
        assertNodeAdded(Unparked, op);
        assertNoNewUnparks();
    }

    @Test
    public void whenUnparked_andPendingWork_thenRemainUnparked() throws Exception {
        MockPartitionOperation operation1 = new MockPartitionOperation();
        partitionQueue.add(operation1);
        MockPartitionOperation operation2 = new MockPartitionOperation();

        beforeTest();
        partitionQueue.add(operation2);

        assertEquals(0, operation1.counter.get());
        assertEquals(0, operation2.counter.get());

        assertNodeAdded(Unparked, operation2);
        assertNoNewUnparks();
    }

    @Test
    public void whenPriorityUnparked_thenConvertToUnparked() {
        MockPartitionOperation priorityOperation = new MockPartitionOperation();
        partitionQueue.priorityAdd(priorityOperation);
        MockPartitionOperation regularOperation = new MockPartitionOperation();

        beforeTest();
        partitionQueue.add(regularOperation);

        assertEquals(0, priorityOperation.counter.get());
        assertEquals(0, regularOperation.counter.get());

        assertNodeAdded(Unparked, regularOperation);
        // since we moved from UnparkedPriority to regular Unpark, a unpark call should have been done.
        assertUnparkNodeAdded();
    }

    @Test
    public void whenExecuting_thenRemainExecuting() throws Exception {
        partitionQueue.head.set(Node.EXECUTING);
        MockPartitionOperation op = new MockPartitionOperation();

        beforeTest();
        partitionQueue.add(op);

        assertEquals(0, op.counter.get());
        assertNodeAdded(Executing, op);
        assertNoNewUnparks();
    }

    @Test
    public void whenPriorityExecuting_thenConvertToExecuting() throws Exception {
        partitionQueue.head.set(Node.EXECUTING_PRIORITY);
        MockPartitionOperation op = new MockPartitionOperation();

        beforeTest();
        partitionQueue.add(op);

        assertEquals(0, op.counter.get());
        assertNodeAdded(Executing, op);
        assertUnparkNodeAdded();
    }

    @Test
    public void whenStolen_thenRemainStolen() throws Exception {
        partitionQueue.head.set(Node.STOLEN);
        MockPartitionOperation op = new MockPartitionOperation();

        beforeTest();
        partitionQueue.add(op);

        assertEquals(0, op.counter.get());
        assertNodeAdded(Stolen, op);
        assertNoNewUnparks();
        assertBuffersUnchanged();
    }

    @Test
    public void whenStolenUnparked_thenRemainStolenUnparked() throws Exception {
        partitionQueue.head.set(Node.STOLEN_UNPARKED);
        MockPartitionOperation op = new MockPartitionOperation();

        beforeTest();
        partitionQueue.add(op);

        assertEquals(0, op.counter.get());
        assertNodeAdded(StolenUnparked, op);
        assertNoNewUnparks();
    }
}
