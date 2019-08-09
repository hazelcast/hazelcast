package com.hazelcast.internal.networking.nio;

import com.hazelcast.internal.networking.OutboundFrame;
import com.hazelcast.internal.util.counters.SwCounter;

import java.util.function.Supplier;

import static com.hazelcast.internal.networking.nio.SendQueue.State.BLOCKED;
import static com.hazelcast.internal.networking.nio.SendQueue.State.SCHEDULED_DATA_ONLY;
import static com.hazelcast.internal.networking.nio.SendQueue.State.SCHEDULED_WITH_TASK;
import static com.hazelcast.internal.networking.nio.SendQueue.State.UNSCHEDULED;

// todo: priority

/**
 * The SendQueue is build using 2 stacks:
 * - the putStack where new items are added to
 * - the takeStack where items are taken from.
 * If an item needs to be taken and the takeStack is empty, the putStack is taken
 * and reversed so we get back the original FIFO order.
 *
 * The idea behind the SendQueue is that offering an item, setting the scheduled flag and
 * other actions (like determining the number of bytes pending) is done atomically.
 *
 * The SendQueue doesn't wake up the NioThread. This is done for a reason; make the SendQueue easy
 * to test in isolation. So the return value of the methods need to be checked if the IOThread needs
 * to be notified.
 */
public class SendQueue implements Supplier<OutboundFrame> {

    public enum Owner{
        TRUE,FALSE
    }

    public enum State {

        /**
         * The pipeline isn't scheduled. It isn't owned by any thread at the moment.
         */
        UNSCHEDULED,

        /**
         * The pipeline is scheduled; meaning it is owned by one thread.
         *
         * THis flag is used when data is pending but no tasks.
         */
        SCHEDULED_DATA_ONLY,

        /**
         * The pipeline is scheduled, meaning it is owned by one thread.
         *
         * This flag is used when there is at least one task pending and potentially some data.
         */
        SCHEDULED_WITH_TASK,

        /**
         * If the pipeline is blocked. So it will not schedule itself if frames are offered; only
         * when tasks are executed.
         */
        BLOCKED
    }

    // Will only be accessed by the thread that is processing the pipeline.
    // Therefor no synchronization is needed.
    private Node takeStackOldest;
    private Node takeStackYoungest;
    private long lowWaterMark;
    private long highWatermark;
    private final SwCounter bytesWritten;

    final PaddedAtomicReference<Node> putStack = new PaddedAtomicReference<>(new Node(BLOCKED));

    public SendQueue(SwCounter bytesWritten) {
        this.bytesWritten = bytesWritten;
    }

    public long bytesPending() {
        return 0;
    }

    public long priorityBytesPending() {
        return 0;
    }

    public int totalFramesPending() {
        return 0;
    }

    public int priorityFramesPending() {
        return 0;
    }

    /**
     * Returns the current State.
     *
     * @return the current state.
     */
    public State state() {
        return putStack.get().state;
    }

    /**
     * Offers a frame to be written.
     *
     * This method can be used in combination with the write-through. When an frame is added,
     * it atomically tries to set the scheduled flag. If this is successfully set, it doesn't
     * matter which thread is actually doing to socket write, the main thing is that only 1
     * thread is granted access to writing (needed for write through vs IO thread)
     *
     * This method is thread-safe.
     *
     * @param frame the frame to write.
     * @return return true if the NioThread needs to be woken up.
     */
    public boolean offer(OutboundFrame frame) {
        Node next = new Node(frame);

        //System.out.println(this+" schedule:"+next);
        for (; ; ) {
            Node prev = putStack.get();
            next.prev = prev;
            next.bytesOffered = prev.bytesOffered + frame.getFrameLength();
            long bytesPending = next.bytesOffered - bytesWritten.get();
// todo: be careful with backing of a fat packet because it could be the queue is empty.
            //            if(bytesPending>10mb){
//                //do backoff.
//                ...
//                /// trye again
//                continue;
//            }
            switch (prev.state) {
                case BLOCKED:
                    next.state = BLOCKED;
                    if (putStack.compareAndSet(prev, next)) {
                        // we don't need to schedule since it is blocked and a blocked pipeline
                        // should not woken up when data gets offered.
                        return false;
                    }
                    break;
                case SCHEDULED_DATA_ONLY:
                    next.state = SCHEDULED_DATA_ONLY;
                    if (putStack.compareAndSet(prev, next)) {
                        // we don't need to schedule, since it is already scheduled.
                        return false;
                    }
                    break;
                case SCHEDULED_WITH_TASK:
                    next.state = SCHEDULED_WITH_TASK;
                    if (putStack.compareAndSet(prev, next)) {
                        // we don't need to schedule, since it is already scheduled.
                        return false;
                    }
                    break;
                case UNSCHEDULED:
                    next.state = SCHEDULED_DATA_ONLY;
                    if (putStack.compareAndSet(prev, next)) {
                        // we need to schedule since it was unscheduled.
                        return true;
                    }
                    break;
                default:
                    throw new IllegalStateException();
            }
        }
    }

    /**
     * Executes a task by offering it to the SendQueue. Wakes up the SendQueue if needed.
     *
     * The task will not immediately be executed, it will be executed when the {@link #prepare()} is called by
     * the thread that is 'owning' the pipeline.
     *
     * If the SendQueue is blocked or unscheduled, it will be woken up.
     * todo: guarantee if wakeup always gets processed.
     *
     * There is no order defined between tasks and frames. Only a total order between frames and a total
     * order between tasks.
     *
     * @param task
     * @return return true if the NioThread needs to be woken up.
     */
    public boolean execute(Runnable task) {
        Node next = new Node(task);

        for (; ; ) {
            Node prev = putStack.get();
            next.state = SCHEDULED_WITH_TASK;
            next.prev = prev;
            next.bytesOffered = prev.bytesOffered;
            if (putStack.compareAndSet(prev, next)) {
                //System.out.println(NioOutboundPipeline.this + " wakeup " + prev.state + "->" + next.state);
                return prev.state == BLOCKED || prev.state == UNSCHEDULED;
            }
        }
    }

    /**
     * Tries to put this Queue in blocking mode.
     *
     * This will prevent the queue from scheduling if other frames are offered. This is useful if the pipeline
     * is blocked for some external event and we don't want to cause the pipeline to get processed if new frames
     * are offered.
     *
     * Should only be made by the thread that owns the Pipeline (so the one calling process).
     *
     * This method guarantees that it won't block if there is a task pending.
     * - there should not be a task on the putStack (then true is returned)
     * - there should not be any tasks within the takeStack (should have been processed)
     *
     * @return true if this SendQueue needs to be rescheduled, false if the pipeline is blocked.
     */
    public boolean block() {
        Node next = null;
        for (; ; ) {
            Node prev = putStack.get();
            switch (prev.state) {
                case UNSCHEDULED:
                    throw new IllegalStateException("Can't block an unscheduled pipeline");
                case BLOCKED:
                    // ignore since it already blocked. Return false to indicate that
                    // rescheduling isn't needed.
                    return false;
                case SCHEDULED_DATA_ONLY:
                    // there are only frames pending; so we can block.
                    if (next == null) {
                        next = new Node(BLOCKED);
                    }
                    next.prev = prev;
                    next.bytesOffered = prev.bytesOffered;
                    if (putStack.compareAndSet(prev, next)) {
                        // we return false to indicate that rescheduling isn't needed.
                        return false;
                    }
                    break;
                case SCHEDULED_WITH_TASK:
                    // todo: why don't we process the task?

                    // we can't block, there is a task pending.
                    // return true to indicate that rescheduling is needed to get the task processed.
                    return true;
                default:
                    throw new IllegalStateException();
            }
        }
    }

    /**
     * Tries to unschedule the SendQueue. This should be done by the Thread that owns the pipeline and
     * determines that nothing is left to be done.
     *
     * The reason that try is returned if scheduling false is that other methods like offer also return
     * true in case of scheduling needed.
     *
     * This call will be made if the IO thread has processed all work from both the put/take stacks. However
     * it could be that new work on the putStack was offered. In that case we are not ready and the pipeline
     * needs to be rescheduled.
     *
     * This method will only be called if no tasks are pending. Because any task on the putStack will be detected
     * and takestack should be empty.
     *
     * @return true if the pipeline is dirty and needs to be offered for processing. False if the pipeline
     * is clean.
     */
    public boolean tryUnschedule() {
        if (takeStackOldest != null) {
            throw new IllegalStateException("Can't call tryUnschedule if the takeStack isn't empty");
        }

        Node prev = putStack.get();
        assert prev.task == null;

        if (prev.frame != null) {
            // we can't unschedule since the putStack isn't empty.
            return true;
        }

        /// the put stack is empty, lets try to cas it to unscheduled.
        // todo: litter.
        Node next = new Node(UNSCHEDULED);
        next.bytesOffered = prev.bytesOffered;
        return !putStack.compareAndSet(prev, next);
    }

    /**
     * Prepares the SendQueue for taking messages (so the get method).
     *
     * The reason this method exists next to get is that even though a pipeline process method is called,
     * it doesn't mean that the {@link #get()} is called; e.g. because the pipeline is trying to get write a
     * large packet that needs many writes.
     *
     * If such a pipeline would have tasks pending, these tasks wouldn't get processed.
     *
     * After this method is called, no tasks are pending to be processed.
     */
    public void prepare() {
        // we read the putStack head. Only in case of a SCHEDULED_WITH_TASK
        // we swap the node.
        Node putStackHead;
        for (; ; ) {
            putStackHead = putStack.get();
            if (putStackHead.state != SCHEDULED_WITH_TASK) {
                break;
            }

            Node next = new Node(SCHEDULED_DATA_ONLY);
            next.bytesOffered = putStackHead.bytesOffered;
            if (putStack.compareAndSet(putStackHead, next)) {
                // we are done, we successfully managed to take the head
                break;
            }
        }

        // iterate over all items. As a result we get 2 stacks: one stack with tasks and one stack with frames.
        Node oldestTaskNode = null;
        Node oldestFrameNode = null;
        Node youngestFrameNode = null;
        Node current = putStackHead;
        while (current != null) {
            Node next = current.prev;
            if (current.task != null) {
                Node tmp = oldestTaskNode;
                oldestTaskNode = current;
                oldestTaskNode.prev = tmp;
            } else if (current.frame != null) {
                if (youngestFrameNode == null) {
                    youngestFrameNode = current;
                }
                Node tmp = oldestFrameNode;
                oldestFrameNode = current;
                oldestFrameNode.prev = tmp;
            }
            current = next;
        }

        // process all tasks in order.
        while (oldestTaskNode != null) {
            oldestTaskNode.task.run();
            oldestTaskNode = oldestTaskNode.prev;
        }

        // append the stack after the takeStack so we can take the items in the correct order.
        if (takeStackYoungest != null) {
            takeStackYoungest.prev = youngestFrameNode;
        } else {
            takeStackOldest = oldestFrameNode;
        }
        takeStackYoungest = youngestFrameNode;
    }

    /**
     * Gets an OutboundFrame from the sendQueue.
     *
     * If this call returns null, the putStack and takeStack have been drained. Although new items
     * could have been placed. That is why we need the {@link #tryUnschedule()}.
     *
     * @return the OutboundFrame or null if none are available.
     */
    @Override
    public OutboundFrame get() {
        for (; ; ) {
            if (takeStackOldest == null) {
                return null;
            }

            Node node = takeStackOldest;
            takeStackOldest = takeStackOldest.prev;
            if (takeStackOldest == null) {
                takeStackYoungest = null;
            }
            OutboundFrame frame = node.frame;
            node.frame = null;
            // normalFramesWritten.inc();
            return frame;
        }
    }

    /**
     * Clears the SendQueue. Can only be made by the thread owning the pipeline.
     */
    public void clear() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    static class Node {
        State state;
        Node prev;
        OutboundFrame frame;
        Runnable task;
        long bytesOffered;

        Node(OutboundFrame frame) {
            this.frame = frame;
        }

        Node(Runnable task) {
            this.task = task;
        }

        Node(State state) {
            this.state = state;
        }

        @Override
        public String toString() {
            return "Node{" +
                    "state=" + state +
                    ", frame=" + frame +
                    ", task=" + task +
                    ", bytesOffered=" + bytesOffered +
                    ", prev=" + prev +
                    '}';
        }
    }
}
