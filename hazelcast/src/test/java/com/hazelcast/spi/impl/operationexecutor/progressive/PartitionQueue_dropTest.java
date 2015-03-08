package com.hazelcast.spi.impl.operationexecutor.progressive;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.spi.impl.operationexecutor.progressive.PartitionQueueState.*;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class PartitionQueue_dropTest extends PartitionQueueAbstractTest {

    @Test
    public void whenParked_thenIllegalStateException() {
        whenIllegalState_thenIllegalStateException(Node.PARKED);
    }

    @Test
    public void whenUnparked_thenIllegalStateException() {
        whenIllegalState_thenIllegalStateException(Node.UNPARKED);
    }

    @Test
    public void whenPriorityUnparked_thenIllegalStateException() {
        whenIllegalState_thenIllegalStateException(new Node(UnparkedPriority));
    }

    @Test
    public void whenExecuting_thenIllegalStateException() {
        whenIllegalState_thenIllegalStateException(Node.EXECUTING);
    }

    @Test
    public void whenPriorityExecuting_thenIllegalStateException() {
        whenIllegalState_thenIllegalStateException(Node.EXECUTING_PRIORITY);
    }

    public void whenIllegalState_thenIllegalStateException(Node node) {
        partitionQueue.head.set(node);

        try {
            partitionQueue.drop();
            fail();
        } catch (IllegalStateException expected) {
        }

        assertHead(node);
        assertNoNewUnparks();
    }

    @Test
    public void whenStolen_noMoreWork_thenPark() {
        partitionQueue.head.set(Node.STOLEN);

        beforeTest();
        boolean result = partitionQueue.drop();

        assertTrue(result);
        assertHead(Node.PARKED);
        assertNoNewUnparks();
    }

    @Test
    public void whenStolen_andMoreWork_thenUnparked() {
        Node node = new Node();
        node.normalSize = 1;
        node.state = Stolen;

        partitionQueue.head.set(node);

        beforeTest();
        boolean result = partitionQueue.drop();

        assertTrue(result);
        assertHeadStateChanged(Unparked);
        assertUnparkNodeAdded();
    }

    @Test
    public void whenStolen_andMoreNormalBufferedWork_thenUnpark() {
        DummyOperation op = new DummyOperation();
        Node node = new Node();
        node.task = op;
        node.normalSize = 1;
        partitionQueue.appendToBuffers(node);
        partitionQueue.head.set(Node.STOLEN);

        beforeTest();
        boolean result = partitionQueue.drop();

        assertTrue(result);
        assertNodeAdded(Unparked, null);
        assertUnparkNodeAdded();
    }

    @Test
    public void whenStolenParked_andNoMoreWork_thenUnparked() {
        partitionQueue.head.set(Node.STOLEN_UNPARKED);

        beforeTest();
        boolean result = partitionQueue.drop();

        assertTrue(result);
        assertHead(Node.UNPARKED);
        assertNoNewUnparks();
    }

    @Test
    public void whenStolenUnparked_andNormalPendingWork_thenUnparked() {
        Node node = new Node();
        node.normalSize = 1;
        node.state = StolenUnparked;

        partitionQueue.head.set(node);

        beforeTest();
        boolean result = partitionQueue.drop();

        assertTrue(result);
        assertHeadStateChanged(Unparked);
        assertNoNewUnparks();
    }

    @Test
    public void whenStolenUnparked_andNormalBufferedWork_thenUnpark() {
        Node node = new Node();
        node.init(new DummyOperation(), StolenUnparked, null);
        partitionQueue.appendToBuffers(node);
        partitionQueue.head.set(Node.STOLEN_UNPARKED);

        beforeTest();
        boolean result = partitionQueue.drop();

        assertTrue(result);
        assertNodeAdded(Unparked, null);
        assertNoNewUnparks();
    }
}
