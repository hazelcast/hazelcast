package com.hazelcast.spi.impl.operationexecutor.progressive;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.spi.impl.operationexecutor.progressive.PartitionQueueState.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class PartitionQueue_pullPriorityTest extends PartitionQueueAbstractTest {

    @Test
    public void whenParked_thenIllegalStateException() {
        whenIllegalState(Node.PARKED);
    }

    @Test
    public void whenUnparked_thenIllegalStateException() {
        whenIllegalState(Node.UNPARKED);
    }

    @Test
    public void whenPriorityUnparked_thenIllegalStateException() {
        Node node = new Node();
        node.state = UnparkedPriority;
        whenIllegalState(node);
    }

    public void whenIllegalState(Node initialState) {
        partitionQueue.head.set(initialState);
        beforeTest();

        try {
            partitionQueue.pull(true);
            fail();
        } catch (IllegalStateException expected) {

        }

        assertHead(initialState);
        assertNoNewUnparks();
    }

    // ================ executing ===================

    @Test
    public void whenExecuting_andNoPendingWork_thenNothingHappens() {
        whenNoPendingWork(Node.EXECUTING);
    }

    @Test
    public void whenExecuting_onlyNormalPendingWork_thenNothingHappens() {
        whenOnlyNormalPendingWork(Executing);
    }

    @Test
    public void whenExecuting_onlyPriorityPendingWork() {
        whenOnlyPriorityPendingWork(Executing, Node.EXECUTING);
    }

    @Test
    public void whenExecuting_priorityAndNormalPendingWork() {
        whenPriorityAndNormalWorkPendingWork(Executing, Node.EXECUTING);
    }

    // ================ priorityExecuting ===================


    @Test
    public void whenPriorityExecuting_andNoPendingWork_thenNothingHappens() {
        whenNoPendingWork(Node.EXECUTING_PRIORITY);
    }

    @Test
    public void whenPriorityExecuting_andOnlyNormalPendingWork_thenNothingHappens() {
        whenOnlyNormalPendingWork(ExecutingPriority);
    }

    @Test
    public void whenPriorityExecuting_andOnlyPriorityPendingWork_thenWorkPulledIn() {
        whenOnlyPriorityPendingWork(ExecutingPriority, Node.EXECUTING_PRIORITY);
    }

    @Test
    public void whenPriorityExecuting_priorityAndNormalPendingWork() {
        whenPriorityAndNormalWorkPendingWork(ExecutingPriority, Node.EXECUTING_PRIORITY);
    }

    // ================= stolen ==============

    @Test
    public void whenStolen_andNoPendingWork_thenNothingHappens() {
        whenNoPendingWork(Node.STOLEN);
    }

    @Test
    public void whenStolen_andOnlyNormalPendingWork_thenNothingHappens() {
        whenOnlyNormalPendingWork(Stolen);
    }

    @Test
    public void whenStolen_andOnlyPriorityPendingWork() {
        whenOnlyPriorityPendingWork(Stolen, Node.STOLEN);
    }

    @Test
    public void whenStolen_priorityAndNormalPendingWork() {
        whenPriorityAndNormalWorkPendingWork(Stolen, Node.STOLEN);
    }

    // ================= stolenUnparked ==============


    @Test
    public void whenStolenUnparked_andNoPendingWork_thenNothingHappens() {
        whenNoPendingWork(Node.STOLEN_UNPARKED);
    }

    @Test
    public void whenStolenUnparked_andOnlyNormalPendingWork_thenNothingHappens() {
        whenOnlyNormalPendingWork(StolenUnparked);
    }

    @Test
    public void whenStolenUnparked_onlyPriorityPendingWork() {
        whenOnlyPriorityPendingWork(StolenUnparked, Node.STOLEN_UNPARKED);
    }

    @Test
    public void whenStolenUnparked_priorityAndNormalPendingWork() {
        whenPriorityAndNormalWorkPendingWork(StolenUnparked, Node.STOLEN_UNPARKED);
    }

    // ==========================================

    public void whenNoPendingWork(Node node) {
        partitionQueue.head.set(node);

        int oldSize = partitionQueue.bufferSize;
        int oldPrioritySize = partitionQueue.priorityBufferSize;

        beforeTest();
        partitionQueue.pull(true);

        assertHead(node);
        assertEquals(oldSize, partitionQueue.bufferSize);
        assertEquals(oldPrioritySize, partitionQueue.priorityBufferSize);
        assertNoNewUnparks();
    }

    public void whenOnlyNormalPendingWork(PartitionQueueState state) {
        Node node = new Node();
        node.normalSize = 0;
        node.task = "";
        node.state = state;

        partitionQueue.head.set(node);

        int oldSize = partitionQueue.bufferSize;
        int oldPrioritySize = partitionQueue.priorityBufferSize;

        beforeTest();
        partitionQueue.pull(true);

        assertHead(node);
        assertEquals(oldSize, partitionQueue.bufferSize);
        assertEquals(oldPrioritySize, partitionQueue.priorityBufferSize);
        assertNoNewUnparks();
    }

    public void whenOnlyPriorityPendingWork(PartitionQueueState state, Node expectedNext) {
        for (int k = 0; k < 10; k++) {
            Node node = new Node();
            node.prioritySize = 1;
            node.task = "";
            node.state = state;
            node.hasPriority = true;

            partitionQueue.head.set(node);

            int oldSize = partitionQueue.bufferSize;
            int oldPrioritySize = partitionQueue.priorityBufferSize;

            beforeTest();
            partitionQueue.pull(true);

            assertHead(expectedNext);
            assertEquals(oldSize, partitionQueue.bufferSize);
            assertEquals(oldPrioritySize + 1, partitionQueue.priorityBufferSize);
            assertNoNewUnparks();
        }
    }

    public void whenPriorityAndNormalWorkPendingWork(PartitionQueueState state, Node expectedNext) {
        for (int k = 0; k < 10; k++) {
            Node priorityNode = new Node();
            priorityNode.priorityInit("", state, null);

            Node normalNode = new Node();
            normalNode.init("", state, priorityNode);

            partitionQueue.head.set(normalNode);

            int oldSize = partitionQueue.bufferSize;
            int oldPrioritySize = partitionQueue.priorityBufferSize;

            beforeTest();
            partitionQueue.pull(true);

            assertHead(expectedNext);
            assertEquals(oldSize + 1, partitionQueue.bufferSize);
            assertEquals(oldPrioritySize + 1, partitionQueue.priorityBufferSize);
            assertNoNewUnparks();
        }
    }
}
