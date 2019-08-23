package com.hazelcast.internal.networking.nio;

import com.hazelcast.internal.networking.OutboundFrame;
import com.hazelcast.internal.util.counters.SwCounter;

import java.util.function.Supplier;

import static com.hazelcast.internal.networking.nio.SendQueue.State.BLOCKED;
import static com.hazelcast.internal.networking.nio.SendQueue.State.SCHEDULED_DATA_ONLY;
import static com.hazelcast.internal.networking.nio.SendQueue.State.SCHEDULED_WITH_TASK;
import static com.hazelcast.internal.networking.nio.SendQueue.State.UNSCHEDULED;
import static com.hazelcast.util.QuickMath.modPowerOfTwo;
import static com.hazelcast.util.QuickMath.nextPowerOfTwo;

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
 *
 * Could it be we end up in a situation whereby we run out of the frames array because we don't move
 * the unprocessed items to the beginning? So imagine a pipeline with more frames it can handle
 * and therefor it needs to reschedule itself and the next round frames again are added; but
 * this isn't done at the end of the array.
 */
public class SendQueue implements Supplier<OutboundFrame> {

    public enum Owner {
        TRUE, FALSE
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

    private Runnable[] tasks = new Runnable[10000];
    private final SwCounter bytesWritten;
    private long frameNr;
    private long priorityFrameNr;
    public long nodeNr;
    public final Queue queue = new Queue();
    public final Queue priorityQueue = new Queue();
    public NioChannel nioChannel;

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
    public boolean enque(OutboundFrame frame) {
        Node next = new Node(frame);

        for (; ; ) {
            Node prev = putStack.get();
            next.prev = prev;
            next.bytesOffered = prev.bytesOffered + frame.getFrameLength();
            if (frame.isUrgent()) {
                next.frameNr = prev.frameNr;
                next.priorityFrameNr = prev.priorityFrameNr + 1;
            } else {
                next.frameNr = prev.frameNr + 1;
                next.priorityFrameNr = prev.priorityFrameNr;
            }
            next.nodeNr = prev.nodeNr + 1;
            //long bytesPending = next.bytesOffered - bytesWritten.get();
            // todo: be careful with backing of a fat packet because it could be the queue is empty.
            //if(bytesPending>10mb){
            //      do backoff.
            //      ...
            //      //try again
            //      continue;
            //}
            switch (prev.state) {
                case UNSCHEDULED:
                    next.state = SCHEDULED_DATA_ONLY;
                    if (putStack.compareAndSet(prev, next)) {
                        // we need to schedule since it was unscheduled.
                        return true;
                    }
                    break;
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
    public boolean enque(Runnable task) {
        Node next = new Node(task);

        for (; ; ) {
            Node prev = putStack.get();
            next.state = SCHEDULED_WITH_TASK;
            next.prev = prev;
            next.bytesOffered = prev.bytesOffered;
            next.frameNr = prev.frameNr;
            next.priorityFrameNr = prev.priorityFrameNr;
            next.nodeNr = prev.nodeNr + 1;
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
     * Should only be made by the thread that owns the Pipeline (so the one calling the process method).
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
                    next.frameNr = prev.frameNr;
                    next.priorityFrameNr = prev.priorityFrameNr;
                    next.nodeNr = prev.nodeNr + 1;
                    if (putStack.compareAndSet(prev, next)) {
                        // we return false to indicate that rescheduling isn't needed.
                        return false;
                    }
                    break;
                case SCHEDULED_WITH_TASK:
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
        if (queue.hasItems() || priorityQueue.hasItems()) {
            // there are frames pending
            return true;
        }

        Node prev = putStack.get();
        if (prev.nodeNr != nodeNr) {
            // there are some frames on the putStack pending
            return true;
        }

        // No more frames are pending.
        // So we are going to try to unschedule.
        // We need a new node because we need to keep track of some important information.
        Node next = new Node(UNSCHEDULED);
        next.bytesOffered = prev.bytesOffered;
        next.frameNr = prev.frameNr;
        next.priorityFrameNr = prev.priorityFrameNr;
        next.nodeNr = prev.nodeNr + 1;
        return !putStack.compareAndSet(prev, next);
    }

    /**
     * Prepares the SendQueue for taking messages (so the get method).
     *
     * The reason this method exists next to get is that even though a pipeline process method is called,
     * it doesn't mean that the {@link #get()} is called; e.g. because the pipeline is trying to get write a
     * large packet that needs many writes. If such a pipeline would have tasks pending, these tasks wouldn't
     * get processed.
     *
     * After this method is called, no tasks are pending to be processed.
     */
    public void prepare() {
        for (; ; ) {
            Node prev = putStack.get();
            switch (prev.state) {
                case BLOCKED:
                    // fall through to SCHEDULED_DATA_ONLY
                case SCHEDULED_DATA_ONLY:
                    // this is the most frequent occurring case.
                    // we do not want to cas the node because this will cause contention with
                    // threads that want to write something to the pipeline and we already need to
                    // change the status when the pipeline wants to unschedule.
                    moveToTakeStack(prev);
                    return;
                case SCHEDULED_WITH_TASK:
                    Node next = new Node(SCHEDULED_DATA_ONLY);
                    next.nodeNr = prev.nodeNr + 1;
                    next.frameNr = prev.frameNr;
                    next.priorityFrameNr = prev.priorityFrameNr;
                    next.prev = prev;
                    if (putStack.compareAndSet(prev, next)) {
                        moveToTakeStack(next);
                        return;
                    }
                    break;
                default:
                    throw new IllegalStateException();
            }
        }
    }

    private void moveToTakeStack(final Node head) {
        if (head.nodeNr == nodeNr) {
            // we already processed up to this point, we are done.
            return;
        }

        int framesPending = (int) (head.frameNr - frameNr);
        queue.ensureExtraCapacity(framesPending);

        int priorityFramesPending = (int) (head.priorityFrameNr - priorityFrameNr);
        priorityQueue.ensureExtraCapacity(priorityFramesPending);

        int frameIndex = queue.toIndex(queue.tail + framesPending - 1);
        int priorityFrameIndex = priorityQueue.toIndex(priorityQueue.tail + priorityFramesPending - 1);

        // int taskIndex = -1;
        Node node = head;
        do {
            if (node.nodeNr == nodeNr) {
                // We are done; premature end of the stack detected.
                break;
            }

            Object item = node.item;
            if (item != null) {
                if (item instanceof Runnable) {
                    //todo: reverse order
                    ((Runnable) item).run();
                } else if (((OutboundFrame) item).isUrgent()) {
                    priorityQueue.array[priorityFrameIndex] = (OutboundFrame) item;
                    priorityFrameIndex = priorityQueue.toIndex(priorityFrameIndex - 1);
                } else {
                    queue.array[frameIndex] = (OutboundFrame) item;
                    frameIndex = queue.toIndex(frameIndex - 1);
                }
            }

            node = node.prev;
        } while (node != null);

//        // tasks are processed in reverse order to restore the FIFO property.
//        for (int k = 0; k <= taskIndex; k++) {
//            tasks[k].run();
//            tasks[k] = null;
//        }

        priorityQueue.tail = priorityQueue.toIndex(priorityQueue.tail + priorityFramesPending);
        queue.tail = queue.toIndex(queue.tail + framesPending);

        // since we already processed this node and whatever is following, we can get rid of it.
        head.prev = null;
        frameNr = head.frameNr;
        priorityFrameNr = head.priorityFrameNr;
        nodeNr = head.nodeNr;
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
        // todo: we always need to check if the putStack has priority frames so that the

        OutboundFrame frame = priorityQueue.deque();
        if (frame != null) {
            return frame;
        }

        // normalFramesWritten.inc();
        return queue.deque();
    }

    private static class Queue {

        private OutboundFrame[] array = new OutboundFrame[65536];
        private int capacity = array.length;
        private int head;
        private int tail;

        /**
         * Deques an item. If no item is found, null is returned.
         *
         * @return the dequeued item.
         */
        OutboundFrame deque() {
            if (head == tail)
                return null;
            OutboundFrame frame = array[head];
            array[head] = null;
            head = modPowerOfTwo(head + 1, capacity);
            return frame;
        }

        int toIndex(int i) {
            if (i < 0) {
                return i + capacity;
            } else if (i >= capacity) {
                return i - capacity;
            } else {
                return i;
            }
        }

        void clear() {
            while (deque() != null) ;
        }

        boolean hasItems() {
            return head != tail;
        }

        void resize(int newCapacity) {
            newCapacity = nextPowerOfTwo(newCapacity);
            OutboundFrame[] newQueue = new OutboundFrame[newCapacity];

            int size = size();
            for (int i = 0; i < size; i++)
                newQueue[i] = deque();

            this.array = newQueue;
            this.capacity = newCapacity;
            this.head = 0;
            this.tail = size;
        }

        int size() {
            int diff = tail - head;
            if (diff < 0)
                diff += capacity;
            return diff;
        }

        void ensureExtraCapacity(int extraItems) {
            if (extraItems == 0) {
                return;
            }

            int requiredCapacity = size() + extraItems;
            if (requiredCapacity > capacity) {
                resize(requiredCapacity);
            }
        }
    }

    /**
     * Clears the SendQueue. Can only be made by the thread owning the pipeline.
     */
    public void clear() {
//        Node node = new Node(UNSCHEDULED);
//        for (; ; ) {
//            Node prev = putStack.get();
//            node.bytesOffered = prev.bytesOffered;
//            node.frameNr = prev.frameNr;
//            node.priorityFrameNr = prev.priorityFrameNr;
//            node.nodeNr = prev.nodeNr + 1;
//            node.prev = prev;
//            if (putStack.compareAndSet(prev, node)) {
//                break;
//            }
//        }
//
//        queue.clear();
//        priorityQueue.clear();
    }

    static class Node {
        State state;
        Node prev;
        Object item;
        long bytesOffered;
        long priorityFrameNr;
        long frameNr;
        long nodeNr;

        Node(OutboundFrame item) {
            this.item = item;
        }

        Node(Runnable task) {
            this.item = task;
        }

        Node(State state) {
            this.state = state;
        }

        @Override
        public String toString() {
            return "Node{" +
                    "state=" + state +
                    ", prev=" + prev +
                    ", item=" + item +
                    ", bytesOffered=" + bytesOffered +
                    ", priorityFrameNr=" + priorityFrameNr +
                    ", frameNr=" + frameNr +
                    ", nodeNr=" + nodeNr +
                    '}';
        }
    }
}
