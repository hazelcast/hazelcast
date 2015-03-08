package com.hazelcast.spi.impl.operationexecutor.progressive;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.spi.impl.operationexecutor.progressive.Node.*;
import static com.hazelcast.spi.impl.operationexecutor.progressive.PartitionQueueState.*;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class PartitionQueue_executeTest extends PartitionQueueAbstractTest {

    @Test
    public void whenParked_thenFails() {
        partitionQueue.head.set(PARKED);

        beforeTest();
        boolean result = partitionQueue.execute();

        assertFalse(result);
        assertHead(PARKED);
        assertBuffersUnchanged();
        assertNoNewUnparks();
    }

    @Test
    public void whenStolen_thenFails() {
        partitionQueue.head.set(STOLEN);

        beforeTest();
        boolean result = partitionQueue.execute();

        assertFalse(result);
        assertHead(STOLEN);
        assertBuffersUnchanged();
        assertNoNewUnparks();
    }

    @Test
    public void whenExecutingPriority_thenIllegalStateException() {
        whenIllegalState_thenIllegalStateException(EXECUTING_PRIORITY);
    }

    @Test
    public void whenExecuting_thenRemainExecuting() {
        whenIllegalState_thenIllegalStateException(EXECUTING);
    }

    public void whenIllegalState_thenIllegalStateException(Node node) {
        partitionQueue.head.set(node);
        beforeTest();

        try {
            partitionQueue.execute();
            fail();
        } catch (IllegalStateException expected) {

        }

        assertHead(node);
        assertBuffersUnchanged();
        assertNoNewUnparks();
    }

    @Test
    public void whenUnparked_andNoWork_thenSuccess() {
        partitionQueue.head.set(Node.UNPARKED);

        beforeTest();
        boolean result = partitionQueue.execute();

        assertTrue(result);
        assertHeadStateChanged(Executing);
        assertBuffersUnchanged();
        assertNoNewUnparks();
    }

    @Test
    public void whenUnparked_andWork_thenSuccess() {
        MockPartitionOperation op = new MockPartitionOperation();
        partitionQueue.add(op);

        beforeTest();
        boolean result = partitionQueue.execute();

        assertTrue(result);
        assertHead(EXECUTING);
//        assertBuffersUnchanged();
        assertNoNewUnparks();
    }

    @Test
    public void whenPriorityUnparked_thenPriorityExecuting() {
        MockPartitionOperation op = new MockPartitionOperation();
        partitionQueue.priorityAdd(op);

        beforeTest();
        boolean result = partitionQueue.execute();

        assertTrue(result);
        assertHead(EXECUTING_PRIORITY);
        //assertBuffersUnchanged();
    }

    //todo: we should also test with non empty states.

    @Test
    public void whenStolenUnparked_thenRevertToStolen() {
        partitionQueue.head.set(Node.STOLEN_UNPARKED);

        beforeTest();
        boolean result = partitionQueue.execute();

        assertFalse(result);
        assertHead(STOLEN_UNPARKED);
        assertBuffersUnchanged();
        assertNoNewUnparks();
    }
}
