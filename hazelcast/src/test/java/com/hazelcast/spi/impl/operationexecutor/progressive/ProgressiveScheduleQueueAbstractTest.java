package com.hazelcast.spi.impl.operationexecutor.progressive;

import org.junit.Before;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public abstract class ProgressiveScheduleQueueAbstractTest {
    protected MockOperationRunner operationRunner;
    protected ProgressiveScheduleQueue scheduleQueue;
    protected UnparkNode previousHead;
    protected int previousBufferPos;
    protected int previousBufferSize;
    protected int previousPriorityBufferPos;
    protected int previousPriorityBufferSize;

    @Before
    public void setup() {
        operationRunner = new MockOperationRunner(0);
        scheduleQueue = new ProgressiveScheduleQueue(Thread.currentThread());
    }

    public void beforeTest() {
        previousHead = scheduleQueue.head.get();
        previousBufferPos = scheduleQueue.bufferPos;
        previousBufferSize = scheduleQueue.bufferSize;
        previousPriorityBufferPos = scheduleQueue.priorityBufferPos;
        previousPriorityBufferSize = scheduleQueue.priorityBufferSize;
    }

    protected void assertNoBuffersChanges() {
        assertNoLowPriorityBufferChanges();
        assertNoHighPriorityBufferChanges();
    }

    protected void assertNoHighPriorityBufferChanges() {
        assertEquals("priorityBufferPos changed", previousPriorityBufferPos, scheduleQueue.priorityBufferPos);
        assertEquals("priorityBufferSize has changed", previousPriorityBufferSize, scheduleQueue.priorityBufferSize);
    }

    protected void assertNoLowPriorityBufferChanges() {
        assertEquals("bufferPos has changed", previousBufferPos, scheduleQueue.bufferPos);
        assertEquals("bufferSize has changed", previousBufferSize, scheduleQueue.bufferSize);
    }

    public void assertNoHeadChange(){
        assertHead(previousHead);
    }

    public void assertHead(UnparkNode expected) {
        UnparkNode actual = scheduleQueue.head.get();
        assertSame("head is not the same", expected, actual);
    }
}
