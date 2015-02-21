package com.hazelcast.spi.impl.operationexecutor.progressive;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.spi.impl.operationexecutor.progressive.Node.EXECUTING;
import static com.hazelcast.spi.impl.operationexecutor.progressive.Node.EXECUTING_PRIORITY;
import static com.hazelcast.spi.impl.operationexecutor.progressive.Node.PARKED;
import static com.hazelcast.spi.impl.operationexecutor.progressive.Node.STOLEN;
import static com.hazelcast.spi.impl.operationexecutor.progressive.Node.STOLEN_UNPARKED;
import static com.hazelcast.spi.impl.operationexecutor.progressive.Node.UNPARKED;
import static com.hazelcast.spi.impl.operationexecutor.progressive.PartitionQueueState.Executing;
import static com.hazelcast.spi.impl.operationexecutor.progressive.PartitionQueueState.ExecutingPriority;
import static com.hazelcast.spi.impl.operationexecutor.progressive.PartitionQueueState.UnparkedPriority;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class PartitionQueue_parkTest extends PartitionQueueAbstractTest {

    @Test
    public void whenParked_thenIllegalStateException() {
        whenIllegalState_thenIllegalStateException(PARKED);
    }

    @Test
    public void whenUnparked_thenIllegalStateException() {
        whenIllegalState_thenIllegalStateException(UNPARKED);
    }

    @Test
    public void whenPriorityUnpark_thenIllegalStateException() {
        whenIllegalState_thenIllegalStateException(new Node(UnparkedPriority));
    }

    @Test
    public void whenStolen_thenIllegalStateException() {
        whenIllegalState_thenIllegalStateException(STOLEN);
    }

    @Test
    public void whenStolenUnparked_thenIllegalStateException() {
        whenIllegalState_thenIllegalStateException(STOLEN_UNPARKED);
    }

    @Test
    public void whenPriorityExecuting_thenIllegalStateException() {
        whenIllegalState_thenIllegalStateException(Node.EXECUTING_PRIORITY);
    }

    private void whenIllegalState_thenIllegalStateException(Node node) {
        partitionQueue.head.set(node);

        try {
            partitionQueue.park();
            fail();
        } catch (IllegalStateException expected) {

        }

        assertHead(node);
        assertNoNewUnparks();
        assertBuffersUnchanged();
    }

    @Test
    public void whenExecuting_andNoPendingWork_thenParkedSuccess() {
        partitionQueue.head.set(EXECUTING);

        beforeTest();
        boolean result = partitionQueue.park();

        assertTrue(result);
        assertHead(PARKED);
        assertNoNewUnparks();
        assertBuffersUnchanged();
    }

    @Test
    public void whenExecuting_andBufferedLowPriorityWork_thenParkFails() {
        Node node = new Node();
        node.init("", Executing, null);

        partitionQueue.appendToBuffers(node);
        partitionQueue.head.set(EXECUTING);

        beforeTest();
        boolean result = partitionQueue.park();

        assertFalse(result);
        assertHead(EXECUTING);
        assertNoNewUnparks();
        assertBuffersUnchanged();
    }

     @Test
    public void whenExecuting_andPendingNormalWork_thenParkFails() {
        Node node = new Node();
        node.init("", Executing, null);

        partitionQueue.head.set(node);

        beforeTest();
        boolean result = partitionQueue.park();

        assertFalse(result);
        assertHead(node);
        assertNoNewUnparks();
    }

    @Test
    public void whenExecuting_andBufferedPriorityWork_thenParkSuccess() {
        Object task = "sometask";
        Node node = new Node();
        node.priorityInit(task, Executing, null);

        partitionQueue.appendToBuffers(node);
        partitionQueue.head.set(EXECUTING);

        beforeTest();
        boolean result = partitionQueue.park();

        assertTrue(result);
        assertHead(Node.UNPARKED_PRIORITY);
        assertNoNewUnparks();
        assertBuffersUnchanged();
    }

    @Test
    public void whenExecuting_andPendingPriorityWork_thenParkSuccess() {
        Object task = "sometask";
        Node node = new Node();
        node.priorityInit(task, Executing, null);

        partitionQueue.head.set(node);

        beforeTest();
        boolean result = partitionQueue.park();

        assertTrue(result);
        assertPriorityNodeAdded(UnparkedPriority, task);
        assertNoNewUnparks();
        assertBuffersUnchanged();
    }
}
