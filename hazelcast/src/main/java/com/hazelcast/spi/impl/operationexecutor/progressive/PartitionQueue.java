package com.hazelcast.spi.impl.operationexecutor.progressive;

import com.hazelcast.nio.Packet;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.operationexecutor.OperationRunner;

import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.spi.impl.operationexecutor.progressive.Node.*;
import static com.hazelcast.spi.impl.operationexecutor.progressive.OperationRunnerThreadLocal.getThreadLocalOperationRunnerHolder;
import static com.hazelcast.spi.impl.operationexecutor.progressive.PartitionQueueState.*;
import static java.lang.System.arraycopy;

/**
 * A buffer that contains all tasks pending for a particular partition. So if there are 271 partitions,
 * than each member will have 271 partition-queues.
 * <p/>
 * when an item is added to a partition buffer and the partition-buffer isn't executing, then the whole
 * partition-buffer can be stolen and used to execute the operation. It can even help out processing
 * operations that are pending before the operation was added. This is called the caller-runs optimization
 * and makes better performance possible.
 * <p/>
 * <p/>
 * todo:
 * the current calling of the OperationRunner is not safe. The OperationRunner is now allowed to throw exceptions
 * but this should be fully taken care of by the OperationHandler.
 */
public class PartitionQueue {

    final UnparkNode unparkNode = new UnparkNode(this, false);

    final AtomicReference<Node> head = new AtomicReference<Node>(PARKED);

    private final OperationRunner operationRunner;

    private final int partitionId;
    private final ProgressiveScheduleQueue scheduleQueue;

    static final int INITIAL_BUFFER_SIZE = 16;
    // the buffer contains all the tasks in the right order for a partition to process.
    // the buffer is only touched by the thread that puts the partition-buffer in 'executing' head.
    Object[] buffer = new Object[INITIAL_BUFFER_SIZE];
    //points to the prev item to read.
    int bufferPos;
    int bufferSize;

    Object[] priorityBuffer = new Object[INITIAL_BUFFER_SIZE];
    //points to the prev item to read.
    int priorityBufferPos;
    int priorityBufferSize;

    public PartitionQueue(int partitionId, ProgressiveScheduleQueue scheduleQueue, OperationRunner operationRunner) {
        this.partitionId = partitionId;
        this.scheduleQueue = scheduleQueue;
        this.operationRunner = operationRunner;
    }

    public ProgressiveScheduleQueue getScheduleQueue() {
        return scheduleQueue;
    }

    Node getHead() {
        return head.get();
    }

    public int getPartitionId() {
        return partitionId;
    }

    PartitionQueueState state() {
        return head.get().state;
    }

    public void errorCheck() {
        if (scheduleQueue.error) {
            throw new RuntimeException(this.toString());
        }
    }

    /**
     * Adds an task to this PartitionQueue.
     *
     * @param task the task to add.
     * @throws java.lang.NullPointerException if task is null.
     */
    public void add(final Object task) {
        if (task == null) {
            throw new NullPointerException("task can't be null");
        }

        final Node next = new Node();
        for (; ; ) {
            final Node prev = head.get();

            boolean unpark;
            PartitionQueueState nextState;
            switch (prev.state) {
                case Parked:
                    unpark = true;
                    nextState = Unparked;
                    break;
                case Unparked:
                    unpark = false;
                    nextState = Unparked;
                    break;
                case ExecutingPriority:
                    unpark = true;
                    nextState = Executing;
                    break;
                case Executing:
                    unpark = false;
                    nextState = Executing;
                    break;
                case Stolen:
                    // the drop will take care of unparking if needed.
                    unpark = false;
                    nextState = Stolen;
                    break;
                case StolenUnparked:
                    unpark = false;
                    nextState = StolenUnparked;
                    break;
                default:
                    throw new IllegalStateException("Unrecognized state: " + prev.state);
            }

            next.init(task, nextState, prev);

            if (!cas(prev, next)) {
                continue;
            }

            if (unpark) {
                scheduleQueue.unpark(this);
            }
            return;
        }
    }

    /**
     * Adds a task with priority. priority add means that this task will be processed with a higher priority than regular
     * tasks. This can be used for system operations like migration.
     * <p/>
     * Any thread can call this method.
     *
     * @param task the task to add
     * @throws NullPointerException if task is null
     */
    public void priorityAdd(final Object task) {
        if (task == null) {
            throw new NullPointerException("task can't be null");
        }

        final Node next = new Node();
        for (; ; ) {
            final Node prev = head.get();

            next.priorityInit(task, prev.state, prev);

            if (!cas(prev, next)) {
                // we did not manage to move to the prev, so lets try again.
                continue;
            }

            // for priorityAdd we always do an unpark.
            scheduleQueue.priorityUnpark(this);
            return;
        }
    }

    /**
     * Tries to run a task if possible on the calling thread, otherwise it will be added to the queue.
     * <p/>
     * Any thread can call this method.
     * <p/>
     * If partition can be stolen successfully, this method will keep on processing:
     * - till there is no more priority tasks
     * - till all pending normal work in front of the operation has been processed.
     *
     * @param task the task to add.
     * @throws java.lang.NullPointerException if task is null.
     */
    public void runOrAdd(final Object task) {
        if (task == null) {
            throw new NullPointerException("task can't be null");
        }

        Node newNode = null;
        for (; ; ) {
            Node prev = head.get();
            switch (prev.state) {
                case Parked: {
                    if (!cas(prev, STOLEN)) {
                        continue;
                    }
                    runAndDrop(prev, task);
                    return;
                }
                case Unparked: {
                    if (!cas(prev, STOLEN_UNPARKED)) {
                        continue;
                    }
                    runAndDrop(prev, task);
                    return;
                }
                case Stolen:
                case StolenUnparked:
                case Executing: {
                    if (newNode == null) {
                        newNode = new Node();
                    }
                    Node next = newNode;
                    next.init(task, prev.state, prev);
                    if (!cas(prev, next)) {
                        continue;
                    }
                    return;
                }
                case ExecutingPriority: {
                    assert prev.normalSize == 0;

                    // When a task is added to a priority executing, we need to upgrade to 'executing' since
                    // only priority tasks have been registered before.
                    if (newNode == null) {
                        newNode = new Node();
                    }
                    Node next = newNode;
                    next.init(task, Executing, prev);
                    if (!cas(prev, next)) {
                        continue;
                    }

                    // since we just converted to a normal 'Executing' we need to register the unpark.
                    scheduleQueue.unpark(this);
                    return;
                }
                default:
                    throw new IllegalStateException("Unrecognized state: " + prev.state);
            }
        }
    }

    /**
     * Process all the work in pending and then drops this PartitonQueue so that its 'available' again
     * for other thiefs and the partition-thread.
     * <p/>
     * The runAndDrop keeps processing all priority operations that are inserted till the drop is a success.
     * It will also process all pending work that was added before the task was added. It will not process
     * any work that has been processed after the task is added. So there won't be a starvation where a thief
     * keeps processing work until the end of time. This can only happen with priority operations.
     * <p/>
     * This method should be called only by a thief that has successfully stolen a partition.
     *
     * @param preceding
     */
    void runAndDrop(Node preceding, Object stolenTask) {
        assertStolen(head.get());

        OperationRunnerThreadLocal.Holder holder = getThreadLocalOperationRunnerHolder();
        holder.runner = operationRunner;

        appendToBuffers(preceding);
        int endPos = bufferSize - 1;

        // first we process everything in front. Also we process all priority operations.
        for (; ; ) {
            // first we process all the priority tasks.
            Object priorityTask = poll(true);
            if (priorityTask != null) {
                process(priorityTask);
                // we try the loop again if we find a priority task; perhaps we can find another one
                continue;
            }

            if (endPos == -1) {
                // no items in front of us.
                break;
            }

            // are we at the end; meaning the next item we are going to get is the item in front of our stolenTask
            boolean end = bufferPos == endPos;
            Object task = next();
            process(task);
            if (end) {
                break;
            }
        }

        process(stolenTask);

        // now we need to keep processing till we have processed all priority work and then we do a drop.
        for (; ; ) {
            Object priorityTask = poll(true);
            if (priorityTask != null) {
                assertStolen(head.get());
                process(priorityTask);
                continue;
            }

            holder.runner = null;
            if (drop()) {
                return;
            }
            holder.runner = operationRunner;
        }
    }

    /**
     * Drops this PartitionQueue so it can be executed by others.
     * <p/>
     * This method should only be called by the thief that stole it.
     * <p/>
     *
     * @return true if the park was a success, false if high priority work has been found. If only low priority work
     * is found and the state is stolen (so it isn't unparked), then unpark is called and the state transforms to StolenUnparked.
     * @throws java.lang.IllegalStateException if the state is not Stolen/StolenUnparked.
     */
    boolean drop() {
        Node newNode = null;
        for (; ; ) {
            Node prev = head.get();
            Node next;

            // as long as there is priority work, you can't drop.
            boolean hasPriorityWork = priorityBufferPos != priorityBufferSize || prev.prioritySize > 0;

            switch (prev.state) {
                case Stolen: {
                    if (hasPriorityWork) {
                        return false;
                    }

                    boolean unpark = false;
                    if (prev.normalSize > 0) {
                        if (newNode == null) {
                            newNode = new Node();
                        }
                        next = newNode;
                        next.withNewState(prev, Unparked);
                        unpark = true;
                    } else if (bufferPos != bufferSize) {
                        next = UNPARKED;
                        unpark = true;
                    } else {
                        next = PARKED;
                    }

                    if (!cas(prev, next)) {
                        continue;
                    }

                    if (unpark) {
                        scheduleQueue.unpark(this);
                    }

                    return true;
                }
                case StolenUnparked: {
                    // as long as there is priority work, you can't drop.
                    if (hasPriorityWork) {
                        return false;
                    }

                    if (prev.normalSize > 0) {
                        if (newNode == null) {
                            newNode = new Node();
                        }
                        next = newNode;
                        next.withNewState(prev, Unparked);
                    } else {
                        next = UNPARKED;
                    }

                    if (!cas(prev, next)) {
                        continue;
                    }

                    return true;
                }
                default:
                    throw new IllegalStateException("Illegal state for drop, expected "
                            + Stolen + " or " + StolenUnparked + " but found: " + prev);
            }
        }
    }

    private void process(Object task) {
        try {
            if (task instanceof Operation) {
                Operation operation = (Operation) task;
                operationRunner.run(operation);
            } else if (task instanceof Packet) {
                Packet packet = (Packet) task;
                operationRunner.run(packet);
            } else if (task instanceof Runnable) {
                Runnable runnable = (Runnable) task;
                runnable.run();
            } else {
                throw new RuntimeException("Unhandled item:" + task);
            }
        } catch (Throwable t) {
            //todo
            t.printStackTrace();
        }
    }

    /**
     * Suspends this PartitionQueue by reverting from Executing/ExecutingPriority to Unparked/UnparkedPriority.
     * <p/>
     * <p/>
     * This method should only be called by the partition owner.
     *
     * @throws IllegalStateException if the caller is not the partition owner, or if the partitionqueue isn't
     *                               in executing mode.
     */
    public void suspend() {
        scheduleQueue.assertCallingThreadIsOwner();

        Node newNode = null;
        for (; ; ) {
            Node prev = head.get();
            PartitionQueueState nextState;
            switch (prev.state) {
                case Executing:
                    nextState = Unparked;
                    break;
                case ExecutingPriority:
                    nextState = Parked;
                    break;
                default:
                    throw new IllegalStateException("Unexpected state for suspend: " + prev);
            }

            if (newNode == null) {
                newNode = new Node();
            }
            Node next = newNode;
            next.withNewState(prev, nextState);

            if (!cas(prev, next)) {
                continue;
            }

            return;
        }
    }


    /**
     * Makes sure this PartitionQueue is currently in the 'Executing' or 'Executing_Priority' state.
     * <p/>
     * This method is only called by the partition-thread.
     * <p/>
     *
     * @return true if it can be put in the executing mode, false otherwise.
     */
    boolean execute() {
        for (; ; ) {
            final Node prev = head.get();
            switch (prev.state) {
                //case Parked:
                case Stolen:
                case StolenUnparked:
                    return false;
                case Executing:
                case ExecutingPriority:
                    throw new IllegalStateException("Unexpected state:" + prev.state);
                case Unparked:
                    if (!cas(prev, EXECUTING)) {
                        continue;
                    }

                    appendToBuffers(prev);
                    return true;
                case Parked:
                    if (!cas(prev, EXECUTING_PRIORITY)) {
                        continue;
                    }

                    appendToBuffers(prev);
                    return true;
                default:
                    throw new IllegalStateException("Illegal state: " + prev.state);
            }
        }
    }

    /**
     * Tries to park this PartitionQueue. This method should only be called after the partition-thread has processed
     * all normal priority tasks of this PartitionQueue.
     * <p/>
     * This method should only be called by the partition-thread.
     *
     * @return true if the park was a success, false if the parked failed because additional low priority work has been found.
     * @throws java.lang.IllegalStateException if the PartitionQueue was not in executing state or if the calling
     *                                         thread is not the partition-thread.
     */
    boolean park() {
        scheduleQueue.assertCallingThreadIsOwner();

        Node newNode = null;
        for (; ; ) {
            final Node prev = head.get();
            switch (prev.state) {
                case Stolen:
                    //                  throw new IllegalStateException("Unexpected state: " + prev);
                    //case UnparkedPriority:
                case ExecutingPriority:
                case Parked:
                    return true;
                case Unparked:
                    return false;
                case Executing: {
                    // if there is any normal work, the park fails.
                    if (bufferPos != bufferSize || prev.normalSize > 0) {
                        return false;
                    }

                    Node next;
                    if (prev.prioritySize > 0) {
                        if (newNode == null) {
                            newNode = new Node();
                        }
                        next = newNode;
                        next.withNewState(prev, Parked);
                    } else {
                        next = PARKED;
                    }

                    if (!cas(prev, next)) {
                        continue;
                    }

                    return true;
                }
                case StolenUnparked: {
                    // a thief has stolen the unparked partition-buffer and is currently processing it.
                    // Currently the head is 'stolen unparked' so a stealer should not call unpark
                    // again. The problem is that the executing thread just has removed the partition-buffer
                    // from its unparked set. So we are going to revert from StolenUnparked to Stolen to indicate
                    // that an unpark is needed again. If you don't do this, you will end up with work in a partition-buffer
                    // which never gets picked by a partition-thread.
                    // When the stealing thread is going to drop the partition-buffer and it sees that the state has
                    // changed to Stolen, it knows that it can revert the partition buffer to Parked.

                    Node next;
                    if (prev.size() == 0) {
                        next = STOLEN;
                    } else {
                        if (newNode == null) {
                            newNode = new Node();
                        }
                        next = newNode;
                        next.withNewState(prev, Stolen);
                    }

                    if (!cas(prev, next)) {
                        // we failed to move to the prev headUnparked to STOLEN, lets try again.
                        continue;
                    }

                    return true;
                }
                default:
                    throw new IllegalStateException("Unexpected state: " + prev);
            }

        }
    }

    private boolean cas(Node expected, Node update) {
        errorCheck();
        boolean success = head.compareAndSet(expected, update);

        if(success) {
//            if (expected.state == ExecutingPriority && update.state == Parked) {
//                    throw new RuntimeException();
//            }
        }
        return success;
    }

    // ================ buffers ===========================================

    /**
     * Polls a low priority item from this PartitionQueue. If there is any pending low priority work, it will
     * be pulled in.
     * <p/>
     * Otherwise pending normal work is returned.
     *
     * @return
     * @throws java.lang.IllegalStateException if the PartitionQueue is not in a stolen/executing state.
     */
    Object poll(boolean priority) {
        Node prev = head.get();
        switch (prev.state) {
            case Executing:
            case ExecutingPriority:
            case StolenUnparked:
            case Stolen:
                break;
            default:
                throw new IllegalStateException("Unexpected state: " + prev);
        }

        // no priority work was found, lets check if we have normal work
        // first we check the buffer.
        Object task = priority ? priorityNext() : next();
        if (task != null) {
            return task;
        }

        // no work was found in the buffer
        if (!pull(priority)) {
            return null;
        }

        return priority ? priorityNext() : next();
    }

    /**
     * Gets the next item in the buffer.
     *
     * @return the item or null if no item was found.
     */
    Object next() {
        if (bufferPos == bufferSize) {
            return null;
        }

        // there is something in the buffers.
        Object item = buffer[bufferPos];
        buffer[bufferPos] = null;
        bufferPos++;

        if (bufferPos == bufferSize) {
            bufferPos = 0;
            bufferSize = 0;
        }

        return item;
    }

    /**
     * Gets the next item in the priority buffer.
     *
     * @return the next item or null if no item was found.
     */
    Object priorityNext() {
        if (priorityBufferPos == priorityBufferSize) {
            return null;
        }

        Object item = priorityBuffer[priorityBufferPos];
        priorityBuffer[priorityBufferPos] = null;
        priorityBufferPos++;
        if (priorityBufferPos == priorityBufferSize) {
            // we are at the end of the buffer. So lets clear it.
            priorityBufferPos = 0;
            priorityBufferSize = 0;
        }

        return item;
    }

    private void assertStolen(Node node) {
        switch (node.state) {
            case Stolen:
            case StolenUnparked:
                return;
            default:
                throw new IllegalStateException("Executing/Stolen expected but found: " + node);
        }
    }

    void appendToBuffers(Node node) {
        if (node.size() == 0) {
            return;
        }

        // doing a capacity check for normal nodes.
        bufferSize += node.normalSize;
        if (bufferSize > buffer.length) {
            Object[] newBuffer = new Object[bufferSize * 2];
            arraycopy(buffer, 0, newBuffer, 0, buffer.length);
            this.buffer = newBuffer;
        }

        // doing a capacity check for priority nodes.
        priorityBufferSize += node.prioritySize;
        if (priorityBufferSize > priorityBuffer.length) {
            Object[] newBuffer = new Object[priorityBufferSize * 2];
            arraycopy(priorityBuffer, 0, newBuffer, 0, priorityBuffer.length);
            this.priorityBuffer = newBuffer;
        }

        int items = node.normalSize + node.prioritySize;
        int insertIndex = bufferSize - 1;
        int priorityInsertIndex = priorityBufferSize - 1;

        // we need to begin at the end of the buffer's and then walk to the beginning
        // to get the items in the right order.
        for (int k = 0; k < items; k++) {
            // we need to filter out empty nodes
            for (; ; ) {
                if (node.task != null) {
                    break;
                }
                node = node.prev;
            }

            if (node.hasPriority) {
                priorityBuffer[priorityInsertIndex] = node.task;
                priorityInsertIndex--;
            } else {
                buffer[insertIndex] = node.task;
                insertIndex--;
            }

            Node next = node.prev;
            node.prev = null;
            node = next;
        }
    }

    /**
     * Pull in work
     *
     * @param priority true if priority work should be pulled on, false if non priority work should be pulled in
     * @return true if any work was pulled in, false if no work was pulled on.
     * @throws IllegalStateException if this PartitionQueue isn't stolen/executing.
     */
    boolean pull(boolean priority) {
        for (; ; ) {
            Node prev = head.get();
            Node next;
            switch (prev.state) {
                case Executing:
                    next = EXECUTING;
                    break;
                case ExecutingPriority:
                    next = EXECUTING_PRIORITY;
                    break;
                case Stolen:
                    next = STOLEN;
                    break;
                case StolenUnparked:
                    next = STOLEN_UNPARKED;
                    break;
                default:
                    throw new IllegalStateException("Found " + prev);
            }

            int size = priority ? prev.prioritySize : prev.normalSize;
            if (size == 0) {
                return false;
            }

            if (!cas(prev, next)) {
                continue;
            }

            appendToBuffers(prev);
            return true;
        }
    }

    // ===========================================================

    @Override
    public String toString() {
        return "PartitionQueue{" +
                "partitionId=" + partitionId +
                ", bufferPos=" + bufferPos +
                ", bufferSize=" + bufferSize +
                ", priorityBufferPos=" + priorityBufferPos +
                ", priorityBufferSize=" + priorityBufferSize +
                ", head=" + head +
                '}';
    }
}
