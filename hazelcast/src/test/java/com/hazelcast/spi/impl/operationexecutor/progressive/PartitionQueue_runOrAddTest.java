package com.hazelcast.spi.impl.operationexecutor.progressive;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.spi.impl.operationexecutor.progressive.AbstractProgressiveOperationExecutorTest.assertExecutedByCurrentThread;
import static com.hazelcast.spi.impl.operationexecutor.progressive.Node.UNPARKED;
import static com.hazelcast.spi.impl.operationexecutor.progressive.PartitionQueueState.Executing;
import static com.hazelcast.spi.impl.operationexecutor.progressive.PartitionQueueState.Stolen;
import static com.hazelcast.spi.impl.operationexecutor.progressive.PartitionQueueState.StolenUnparked;
import static org.junit.Assert.assertEquals;


@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class PartitionQueue_runOrAddTest extends PartitionQueueAbstractTest {

    @Test(expected = NullPointerException.class)
    public void whenNull_thenNullPointerException() {
        partitionQueue.runOrAdd(null);
    }

    // this is one of the most important tests because it proves the caller runs optimization
    @Test
    public void whenParked_thenRun_andRemainParked() throws Throwable {
        DummyOperation op = new DummyOperation();

        beforeTest();
        partitionQueue.runOrAdd(op);

        // todo: we should check if it has been flipped to stolen during execution
        assertExecutedByCurrentThread(op);
        assertHead(Node.PARKED);
        assertNoNewUnparks();
    }

    @Test
    public void whenUnparked_andPendingWork_thenRun_andRemainUnparked() throws Exception {
        DummyOperation op1 = new DummyOperation();
        partitionQueue.add(op1);

        DummyOperation op2 = new DummyOperation();

        beforeTest();
        partitionQueue.runOrAdd(op2);

        assertExecutedByCurrentThread(op1, op2);
        // todo: we should check if it has been flipped to stolen during execution
        assertHead(UNPARKED);
        assertNoNewUnparks();
    }

    @Test
    public void whenUnparked_andNoWork_thenRun_andRemainUnparked() throws Exception {
        partitionQueue.head.set(UNPARKED);
        DummyOperation op = new DummyOperation();

        beforeTest();
        partitionQueue.runOrAdd(op);

        assertExecutedByCurrentThread(op, op);

        // todo: we should check if it has been flipped to stolen during execution
        assertHead(UNPARKED);
        assertNoNewUnparks();
    }

//    @Test
//    public void whenEmptyScheduled() throws Exception {
//        partitionQueue.node.set(PartitionQueue.EMPTY_SCHEDULED);
//
//        PartitionQueue.Node node1 = partitionQueue.getNode();
//
//        MockPartitionOperation op = new MockPartitionOperation();
//        partitionQueue.add(op);
//
//        assertEquals(0, op.counter.get());
//
//        PartitionQueue.Node node = partitionQueue.node.get();
//        assertNotNull(node);
//        assertEquals(1, node.normalSize);
//        assertNull(node.prev);
//        assertSame(op, node.task);
//        assertEquals(PartitionQueueState.Unparked, node.headUnparked);
//        assertNull(scheduleQueue.headUnparked.get());//since it was already scheduled, it should not be scheduled again
//        assertSame(partitionThread, partitionQueue.getOwnerThread());
//    }

    @Test
    public void whenPriorityUnparked_thenEverythingProcessed_andReturnToParked() {
        DummyOperation op1 = new DummyOperation();
        partitionQueue.priorityAdd(op1);
        DummyOperation op2 = new DummyOperation();

        beforeTest();
        partitionQueue.runOrAdd(op2);

        assertExecutedByCurrentThread(op1, op2);
        // todo: we should check if it has been flipped to stolen during execution
        assertHead(Node.PARKED);
        assertNoNewUnparks();
    }

    @Test
    public void whenPriorityExecuting_thenTaskAdded() {
        partitionQueue.head.set(Node.EXECUTING_PRIORITY);
        MockPartitionOperation op = new MockPartitionOperation();

        beforeTest();
        partitionQueue.runOrAdd(op);

        assertEquals(0, op.counter.get());
        assertNodeAdded(Executing, op);
        assertUnparkNodeAdded();
    }

    @Test
    public void whenExecuting_thenTaskAdded() throws Exception {
        partitionQueue.head.set(Node.EXECUTING);
        MockPartitionOperation op = new MockPartitionOperation();

        beforeTest();
        partitionQueue.runOrAdd(op);

        assertEquals(0, op.counter.get());
        assertNodeAdded(Executing, op);
        assertNoNewUnparks();
    }

    @Test
    public void whenStolen_thenTaskAdded() throws Exception {
        partitionQueue.head.set(Node.STOLEN);
        MockPartitionOperation op = new MockPartitionOperation();

        beforeTest();
        partitionQueue.runOrAdd(op);

        assertEquals(0, op.counter.get());
        assertNodeAdded(Stolen, op);
        assertNoNewUnparks();
    }

    @Test
    public void whenStolenUnparked_thenTaskAdded() {
        partitionQueue.head.set(Node.STOLEN_UNPARKED);
        MockPartitionOperation op = new MockPartitionOperation();

        beforeTest();
        partitionQueue.runOrAdd(op);

        assertEquals(0, op.counter.get());
        assertNodeAdded(StolenUnparked, op);
        assertNoNewUnparks();
    }
}
