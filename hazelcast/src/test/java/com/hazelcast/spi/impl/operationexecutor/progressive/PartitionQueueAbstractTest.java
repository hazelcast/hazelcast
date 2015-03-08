package com.hazelcast.spi.impl.operationexecutor.progressive;

import org.junit.Before;

import static org.junit.Assert.*;

public abstract class PartitionQueueAbstractTest {

    protected PartitionQueue partitionQueue;
    protected MockOperationRunner operationRunner;
    protected ProgressiveScheduleQueue scheduleQueue;
    protected Thread partitionThread;
    protected UnparkNode previousUnparkNode;
    protected Node previousNode;
    private int previousBufferPos;
    private int previousBufferSize;
    private int previousPriorityBufferPos;
    private int previousPriorityBufferSize;

    @Before
    public void setup() {
        partitionThread = Thread.currentThread();
        operationRunner = new MockOperationRunner(0);
        scheduleQueue = new ProgressiveScheduleQueue(partitionThread);
        partitionQueue = new PartitionQueue(0, scheduleQueue, operationRunner);
        beforeTest();
    }

    public void beforeTest() {
        previousNode = partitionQueue.head.get();
        previousUnparkNode = scheduleQueue.head.get();
        previousBufferPos = partitionQueue.bufferPos;
        previousBufferSize = partitionQueue.bufferSize;
        previousPriorityBufferPos = partitionQueue.priorityBufferPos;
        previousPriorityBufferSize = partitionQueue.priorityBufferSize;
    }

    protected void assertBuffersUnchanged() {
        assertEquals("bufferPos has changed", previousBufferPos, partitionQueue.bufferPos);
        assertEquals("bufferSize has changed", previousBufferSize, partitionQueue.bufferSize);
        assertEquals("priorityBufferPos changed", previousPriorityBufferPos, partitionQueue.priorityBufferPos);
        assertEquals("priorityBufferSize has changed", previousPriorityBufferSize, partitionQueue.priorityBufferSize);
    }

    public void assertHead(Node expected) {
        Node actual = partitionQueue.head.get();
        assertSame("head is not the same", expected, actual);
    }

    public void assertPriorityUnparkNodeAdded() {
        UnparkNode unparkNode = scheduleQueue.head.get();
        assertNotNull(unparkNode);
        assertSame(previousUnparkNode, unparkNode.prev);

        assertTrue("was expecting priority", unparkNode.hasPriority);
        assertSame(partitionQueue, unparkNode.partitionQueue);

        int expectedNormalSize = previousUnparkNode == null ? 0 : previousUnparkNode.normalSize;
        assertEquals(expectedNormalSize, unparkNode.normalSize);

        int expectedPrioritySize = previousUnparkNode == null ? 1 : previousUnparkNode.prioritySize + 1;
        assertEquals(expectedPrioritySize, unparkNode.prioritySize);
    }

    public void assertNodeAdded(PartitionQueueState expected, Object task) {
        Node node = partitionQueue.head.get();
        assertNotNull(node);

        assertEquals("node.state doesn't match", expected, node.state);
        assertSame("node.task doesn't match", task, node.task);
        assertFalse("node should not have priority set", node.hasPriority);
        int taskSize = task == null ? 0 : 1;
        assertEquals("normal size doesn't match", previousNode.normalSize + taskSize, node.normalSize);
        assertEquals("node prioritySize doesn't match", previousNode.prioritySize, node.prioritySize);

        if (previousNode.size() == 0) {
            assertNull("node.prev should be null", node.prev);
        } else {
            assertSame("node.prev doesn't match", previousNode, node.prev);
        }
    }

    public void assertPriorityNodeAdded(PartitionQueueState expected, Object task) {
        Node node = partitionQueue.head.get();
        assertNotNull(node);

        assertEquals("node.state doesn't match", expected, node.state);
        assertSame("node.task doesn't match", task, node.task);
        assertTrue("node.hasPriority doesn't match", node.hasPriority);
        assertEquals("node.normalSize doesn't match", previousNode.normalSize, node.normalSize);

        int taskSize = task == null ? 0 : 1;
        assertEquals("node.prioritySize doesn't match", previousNode.prioritySize + taskSize, node.prioritySize);

        if (previousNode.size() == 0) {
            assertNull("node.prev should be null", node.prev);
        } else {
            assertSame("node.prev doesn't match", previousNode, node.prev);
        }
    }

    public void assertUnparkNodeAdded() {
        UnparkNode unparkNode = scheduleQueue.head.get();
        assertNotNull(unparkNode);
        assertSame(previousUnparkNode, unparkNode.prev);

        assertFalse("was not expecting priority", unparkNode.hasPriority);
        assertSame(partitionQueue, unparkNode.partitionQueue);

        int expectedNormalSize = previousUnparkNode == null ? 1 : previousUnparkNode.normalSize + 1;
        assertEquals(expectedNormalSize, unparkNode.normalSize);

        int expectedPrioritySize = previousUnparkNode == null ? 0 : previousUnparkNode.prioritySize + 0;
        assertEquals(expectedPrioritySize, unparkNode.prioritySize);
    }

    public void assertHeadStateChanged(PartitionQueueState expectedState) {
        Node node = partitionQueue.head.get();
        assertNotSame(node, previousNode);
        assertEquals(previousNode.hasPriority, node.hasPriority);
        assertEquals(previousNode.normalSize, node.normalSize);
        assertEquals(previousNode.prioritySize, node.prioritySize);
        assertEquals(previousNode.task, node.task);
        assertEquals(previousNode.prev, node.prev);
        assertEquals(expectedState, node.state);
    }

    public void assertNoNewUnparks() {
        assertSame(previousUnparkNode, scheduleQueue.head.get());
    }

    public void assertNoNewNodes() {
        assertSame(previousNode, partitionQueue.head.get());
    }
}
