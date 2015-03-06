package com.hazelcast.spi.impl.operationexecutor.progressive;

import com.hazelcast.spi.impl.operationexecutor.classic.ScheduleQueue;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.spi.impl.operationexecutor.progressive.PartitionQueueState.Executing;
import static com.hazelcast.util.ValidationUtil.checkNotNull;
import static java.lang.System.arraycopy;

/**
 * Multiple PartitionQueue's schedule themselves at a single work-buffer so that their work will be processed by the
 * partition-thread.
 * <p/>
 * A partition-buffer can be scheduled at a work-buffer, but it can be that a different thread stole the partition-buffer
 * and processed some/all of the work. So the work-buffer could end up with an empty partition-buffer; which is fine.
 * <p/>
 * A WorkQueue could be seen as a stack of partition-queues.
 * <p/>
 * <p/>
 * <p/>
 * todo:
 * - make handling the partition queues fifo instead of lifo. So copy the unpark-nodes in a local array.
 * <p/>
 * <p/>
 * done
 * - the partition buffer should not work as node in the unparked-list in the schedule-buffer.
 */
public final class ProgressiveScheduleQueue implements ScheduleQueue {

    public volatile boolean error = false;

    static final UnparkNode BLOCKED = new UnparkNode(null, false);
    static final int INITIAL_BUFFER_CAPACITY = 100;

    final AtomicReference<UnparkNode> head = new AtomicReference<UnparkNode>();
    private Thread partitionThread;

    // ===================================================================
    // The fields bellow will only be touched by the partition-thread.
    // ===================================================================

    PartitionQueue[] buffer = new PartitionQueue[INITIAL_BUFFER_CAPACITY];
    int bufferPos;
    int bufferSize;

    PartitionQueue[] priorityBuffer = new PartitionQueue[INITIAL_BUFFER_CAPACITY];
    int priorityBufferPos;
    int priorityBufferSize;

    PartitionQueue activeQueue;
    boolean activeQueueHasPriority;

    public ProgressiveScheduleQueue() {
    }

    public ProgressiveScheduleQueue(Thread partitionThread) {
        this.partitionThread = checkNotNull(partitionThread, "partitionThread");
    }

    public void setOwnerThread(Thread partitionThread) {
        this.partitionThread = partitionThread;
    }

    public Thread getPartitionThread() {
        return partitionThread;
    }

    @Override
    public int normalSize() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int prioritySize() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void add(Object task) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addUrgent(Object task) {
        throw new UnsupportedOperationException();
    }

    private PartitionQueue suspendedQueue;

    @Override
    public Object take() throws InterruptedException {
        assertCallingThreadIsOwner();

        for (; ; ) {
            Object highPriorityTask = highPriority();
            if (highPriorityTask != null) {
                return highPriorityTask;
            }

            assert priorityBufferSize == 0;
            assert priorityBufferPos == 0;

            Object lowPriorityTask = lowPriority();
            if (lowPriorityTask != null) {
                return lowPriorityTask;
            }
        }
    }

    private Object highPriority() {
        for (; ; ) {
            PartitionQueue priorityQueue = pollNextPriorityQueue();
            if (priorityQueue == null) {
                // no queue with priority tasks has been found.
                return null;
            }

            // this approach is not very efficient, because we are still going to call suspend/execute if
            // we get a priority message from the the same queue we are using in a low priority approach

            if (activeQueue != null) {
                activeQueue.suspend();

                if (!activeQueueHasPriority) {
                    assert suspendedQueue == null;
                    suspendedQueue = activeQueue;
                }

                activeQueue = null;
                activeQueueHasPriority = false;
            }

            if (!priorityQueue.execute()) {
                continue;
            }

            activeQueue = priorityQueue;
            activeQueueHasPriority = true;

            Object item = pollActiveQueue(true);
            if (item != null) {
                activeQueue.suspend();
                activeQueue = null;
                return item;
            }
        }
    }

    private Object lowPriority() throws InterruptedException {
        // we iterate over all low priority partition-queues.
        for (; ; ) {
            Object item = pollActiveQueue(false);
            if (item != null) {
                return item;
            }

            PartitionQueue partitionQueue = next();
            if (partitionQueue == null) {
                // this call blocks till any work comes available
                UnparkNode node = takeHead();
                appendToBuffers(node);
                return null;
            }

            for (; ; ) {
                if (partitionQueue.execute()) {
                    assert partitionQueue.state() == Executing : "expecting Executing, but found " + partitionQueue.state();
                    activeQueue = partitionQueue;
                    activeQueueHasPriority = false;
                    break;
                }

                if (partitionQueue.park()) {
                    break;
                }
            }
        }
    }

    /**
     * Polls a low priority item from the active queue.
     * <p/>
     * If no activeQueue is available, null is returned.
     * <p/>
     * If the activeQueue has no more items, it is parked, null is returned and the activeQueue is set to null.
     *
     * @return
     */
    Object pollActiveQueue(boolean priorityTasks) {
        if (activeQueue == null) {
            return null;
        }

        Object item = activeQueue.poll(priorityTasks);

        if (item != null) {
            return item;
        }

        if (activeQueueHasPriority) {
            activeQueue.suspend();
        } else {
            if (priorityTasks) {
                // so we asked for a priority task, but the activeQueue has nothing to offer
                // so we we suspend this queue
                activeQueue.suspend();
                assert suspendedQueue == null;
                suspendedQueue = activeQueue;
            } else if (!activeQueue.park()) {
                //we did not manage to park this queue because low priority work has been found.
                // so lets return that.
                return activeQueue.poll(false);
            }
        }

        // we successfully managed to park the queue. This mans that the
        // queue is now returned to its initial state.
        activeQueue = null;
        activeQueueHasPriority = false;
        return null;
    }

    /**
     * Polls the priority PartitionQueue.
     * <p/>
     * So it will first check if there is an item in the local priority buffer.
     * If none is found, the head is checked. If there is a priority item, the head is
     * taken, put in the buffers and the priority item taken.
     *
     * @return the found priority PartitionQueue, null if none is found.
     */
    PartitionQueue pollNextPriorityQueue() {
        PartitionQueue queue = priorityNext();
        if (queue != null) {
            return queue;
        }

        // nothing was found in the priority buffers, lets check if there is perhaps work in the head
        // with a priority.
        for (; ; ) {
            UnparkNode prev = head.get();
            if (prev == null || prev.prioritySize == 0) {
                return null;
            }

            if (!head.compareAndSet(prev, null)) {
                // we failed to swap the head, so lets try again.
                continue;
            }

            // we found work, so lets append it to the buffer and lets try the outer loop again so we get
            // the priority work.
            appendToBuffers(prev);
            break;
        }

        return priorityNext();
    }

    public void assertCallingThreadIsOwner() {
        Thread currentThread = Thread.currentThread();
        if (currentThread != partitionThread) {
            throw new IllegalStateException("Take can only be done by the schedule-queue owner, but is done by: "
                    + currentThread);
        }
    }


    /**
     * Takes the head or parks if there is no head.
     *
     * @return the PartitionQueues.
     * @throws InterruptedException if the calling thread is interrupted while waiting.
     */
    private UnparkNode takeHead() throws InterruptedException {
        if (priorityBufferPos != priorityBufferSize) {
            throw new IllegalStateException();
        }

        if (bufferPos != bufferSize) {
            throw new IllegalStateException();
        }

        final AtomicReference<UnparkNode> head = this.head;
        for (; ; ) {
            final UnparkNode prev = head.get();

            if (prev == null || prev == BLOCKED) {
                // there currently is nothing to do

                if (!head.compareAndSet(prev, BLOCKED)) {
                    // we did not manage to register as blocked because a partition-queue just parked.
                    // so lets try again.
                    continue;
                }

                // we have successfully registers as blocked, so lets park.
                LockSupport.park();

                // A thread that parks can be interrupted and it will end the park, but we need to check
                // if the interrupt-flag is set
                if (partitionThread.isInterrupted()) {
                    throw new InterruptedException();
                }

                // we just wakeup up from a park, lets try again.
                continue;
            }

            // lets try to check out the pending work.
            if (!head.compareAndSet(prev, null)) {
                // we failed claim the partition-buffer, lets try again.
                continue;
            }

            // we successfully managed to remove all pending UnparkNodes.
            return prev;
        }
    }

    /**
     * Unparks a PartitionQueue. It means that this PartitionQueue is registered to be picked up by the
     * partition-thread at some point in time. So eventually this thread will process the partition-buffer,
     * or a thief steals it.
     * <p/>
     * When a thief steals a partition-buffer, it won't change anything regarding being parked. The partition-thread
     * is still going to try to claim ownership and execute it.
     *
     * @param partitionQueue
     */
    public void unpark(PartitionQueue partitionQueue) {
        assertCanUnpark(partitionQueue);

        final UnparkNode next = partitionQueue.unparkNode;
        final AtomicReference<UnparkNode> head = this.head;
        for (; ; ) {
            final UnparkNode prev = head.get();

            if (prev == BLOCKED || prev == null) {
                next.prev = null;
                next.normalSize = 1;
                next.prioritySize = 0;
            } else {
                next.prev = prev;
                next.normalSize = prev.normalSize + 1;
                next.prioritySize = prev.prioritySize;
            }

            if (!head.compareAndSet(prev, next)) {
                // we did not manage to add the partition-buffer, so lets try again.
                continue;
            }

            // we successfully managed to add the partition-buffer.
            if (prev == BLOCKED) {
                // the partition-thread wants to be signalled since it was blocking
                LockSupport.unpark(partitionThread);
            }

            return;
        }
    }

    private void assertCanUnpark(PartitionQueue partitionQueue) {
        if (partitionQueue == null) {
            throw new NullPointerException("partitionQueue can't be null");
        }

        Node node = partitionQueue.head.get();
        switch (node.state) {
            case Executing://registered call
            case Unparked://registered call
            case StolenUnparked://registered call
                break;
            case ExecutingPriority://unregistered call
            case UnparkedPriority://unregistered call
            case Stolen://unregistered call
            case Parked://unregistered call
                throw new IllegalStateException(node.state + " Unexpected state: " + partitionQueue);
        }

        UnparkNode unparkNode = partitionQueue.unparkNode;
        UnparkNode prev = unparkNode.prev;
        if (prev != null) {
            throw new IllegalArgumentException("partitionQueue.unparkNode.prev should be null, but was: " + prev);
        }
    }

    public void priorityUnpark(PartitionQueue partitionQueue) {
        if (partitionQueue == null) {
            throw new NullPointerException("partitionQueue can't be null");
        }

        final UnparkNode next = new UnparkNode(partitionQueue, true);
        final AtomicReference<UnparkNode> head = this.head;
        for (; ; ) {
            final UnparkNode prev = head.get();

            if (prev == BLOCKED || prev == null) {
                next.prev = null;
                next.normalSize = 0;
                next.prioritySize = 1;
            } else {
                next.prev = prev;
                next.normalSize = prev.normalSize;
                next.prioritySize = prev.prioritySize + 1;
            }

            if (!head.compareAndSet(prev, next)) {
                // we did not manage to add the partition-buffer, so lets try again.
                continue;
            }

            // we successfully managed to add the partition-buffer.
            if (prev == BLOCKED) {
                // the partition-thread wants to be signalled since it was blocking
                LockSupport.unpark(partitionThread);
            }
            return;
        }
    }

    PartitionQueue priorityNext() {
        // first we check if we can find something in the priority buffer.
        if (priorityBufferPos == priorityBufferSize) {
            return null;
        }

        PartitionQueue partitionQueue = priorityBuffer[priorityBufferPos];
        priorityBuffer[priorityBufferPos] = null;
        priorityBufferPos++;
        if (priorityBufferPos == priorityBufferSize) {
            // we are at the end of the buffer. So lets clear it.
            priorityBufferPos = 0;
            priorityBufferSize = 0;
        }

        return partitionQueue;
    }

    PartitionQueue next() {
        if (suspendedQueue != null) {
            PartitionQueue tmp = suspendedQueue;
            suspendedQueue = null;
            return tmp;
        }

        // first we check if we can find something in the priority buffer.
        if (bufferPos == bufferSize) {
            return null;
        }

        PartitionQueue partitionQueue = buffer[bufferPos];
        buffer[bufferPos] = null;
        bufferPos++;
        if (bufferPos == bufferSize) {
            // we are at the end of the buffer. So lets clear it.
            bufferPos = 0;
            bufferSize = 0;
        }

        return partitionQueue;
    }

    void appendToBuffers(UnparkNode node) {
        assert node != null;

        // doing a capacity check for normal nodes.
        bufferSize += node.normalSize;
        if (bufferSize > buffer.length) {
            PartitionQueue[] newBuffer = new PartitionQueue[bufferSize * 2];
            arraycopy(buffer, 0, newBuffer, 0, buffer.length);
            this.buffer = newBuffer;
        }

        // doing a capacity check for priority nodes.
        priorityBufferSize += node.prioritySize;
        if (priorityBufferSize > priorityBuffer.length) {
            PartitionQueue[] newBuffer = new PartitionQueue[priorityBufferSize * 2];
            arraycopy(priorityBuffer, 0, newBuffer, 0, priorityBuffer.length);
            this.priorityBuffer = newBuffer;
        }

        int items = node.normalSize + node.prioritySize;
        int insertIndex = bufferSize - 1;
        int priorityInsertIndex = priorityBufferSize - 1;

        // we need to begin at the end of the buffer's and then walk to the beginning
        // to get the items in the right order.
        for (int k = 0; k < items; k++) {
            if (node.hasPriority) {
                priorityBuffer[priorityInsertIndex] = node.partitionQueue;
                priorityInsertIndex--;
            } else {
                buffer[insertIndex] = node.partitionQueue;
                insertIndex--;
            }

            UnparkNode next = node.prev;
            node.prev = null;
            node = next;
        }
    }

    @Override
    public String toString() {
        UnparkNode head = this.head.get();

        String headString;
        if (head == null) {
            headString = "null";
        } else if (head == BLOCKED) {
            headString = "Blocked";
        } else {
            headString = head.toString();
        }

        return "ProgressiveScheduleQueue{" +
                "  bufferPos=" + bufferPos +
                ", bufferSize=" + bufferSize +
                ", priorityBufferPos=" + priorityBufferPos +
                ", priorityBufferSize=" + priorityBufferSize +
                ", activeQueue=" + activeQueue +
                ", head=" + headString +
                '}';
    }
}
