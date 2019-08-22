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
    private final Queue<Runnable> taskQueue = new Queue<>();
    private final Queue<OutboundFrame> priorityQueue = new Queue<>();
    private final Queue<OutboundFrame> queue = new Queue<>();

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
            next.node = prev;
            next.bytesOffered = prev.bytesOffered + frame.getFrameLength();
            next.urgentFrameCount = frame.isUrgent() ? prev.urgentFrameCount + 1 : prev.urgentFrameCount;

            //   next.prevNodeWithPriorityFrame =

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
    public boolean execute(Runnable task) {
        Node next = new Node(task);

        for (; ; ) {
            Node prev = putStack.get();
            next.state = SCHEDULED_WITH_TASK;
            next.node = prev;
            next.bytesOffered = prev.bytesOffered;
            next.urgentFrameCount = prev.urgentFrameCount;
            if (putStack.compareAndSet(prev, next)) {
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
                    next.node = prev;
                    next.urgentFrameCount = prev.urgentFrameCount;
                    next.bytesOffered = prev.bytesOffered;
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
     * determines that nothing is left to be done. So the
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
        if (taskQueue.hasItems() || priorityQueue.hasItems()) {
            throw new IllegalStateException("Can't call tryUnschedule if items are pending");
        }

        Node prev = putStack.get();

        if (prev.item != null) {
            //todo: what if the node is a marker node and node with data are behind?
            // we can't unschedule since the putStack isn't empty.
            return true;
        }

        /// the put stack is empty, lets try to cas it to unscheduled.
        // we need a new node because we need to keep track of the number of bytes offered.
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
            next.urgentFrameCount = putStackHead.urgentFrameCount;
            next.bytesOffered = putStackHead.bytesOffered;
            if (putStack.compareAndSet(putStackHead, next)) {
                // we are done, we successfully managed to take the head
                break;
            }
        }

        Node newestTaskNode = null;
        Node oldestTaskNode = null;
        Node newestFrameNode = null;
        Node oldestFrameNode = null;
        Node newestPriorityFrameNode = null;
        Node oldestPriorityFrameNode = null;


        // todo: how does this code deal with a part of the stack being processed more than once? because
        // we don't change the stack while taking items. Afaik this is broken

        Node current = putStackHead;
        while (current != null) {
            Node prev = current.node;
            Object item = current.item;
            if (item != null) {
                if (item instanceof Runnable) {
                    if (newestTaskNode == null) {
                        current.node = null;
                        newestTaskNode = current;
                        oldestTaskNode = current;
                    } else {
                        current.node = oldestTaskNode;
                        oldestTaskNode = current;
                    }
                } else if (((OutboundFrame) item).isUrgent()) {
                    if (newestPriorityFrameNode == null) {
                        current.node = null;
                        newestPriorityFrameNode = current;
                        oldestPriorityFrameNode = current;
                    } else {
                        current.node = oldestPriorityFrameNode;
                        oldestPriorityFrameNode = current;
                    }
                } else {
                    if (newestFrameNode == null) {
                        current.node = null;
                        newestFrameNode = current;
                        oldestFrameNode = current;
                    } else {
                        current.node = oldestFrameNode;
                        oldestFrameNode = current;
                    }
                }
            }
            current = prev;
        }

        priorityQueue.enque(oldestPriorityFrameNode, newestPriorityFrameNode);
        queue.enque(oldestFrameNode, newestFrameNode);
        taskQueue.enque(oldestTaskNode, newestTaskNode);

        // we process all tasks;
        while (taskQueue.hasItems()) {
            taskQueue.deque().run();
        }
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

    /**
     * Clears the SendQueue. Can only be made by the thread owning the pipeline.
     */
    public void clear() {
        putStack.set(new Node(BLOCKED));
        taskQueue.clear();
        queue.clear();
        priorityQueue.clear();
    }

    static class Queue<E> {
        Node head;
        Node tail;

        void enque(Node oldest, Node youngest) {
            if (tail == null) {
                tail = youngest;
                head = oldest;
            } else {
                tail.node = oldest;
                tail = youngest;
            }
        }

        /**
         * Deques an item. If no item is found, false is returned.
         *
         * @return the dequed item.
         */
        E deque() {
            for (; ; ) {
                if (head == null) {
                    // queue is empty.
                    return null;
                }

                E item = (E) head.item;
                head.item = null;

                if (head.node == null) {
                    head = null;
                    tail = null;
                } else {
                    head = head.node;
                }

                if (item != null) {
                    return item;
                }

                // if there is no item on the node, we move on to the next node and try again.
            }
        }

        void clear() {
            head = null;
            tail = null;
        }

        boolean hasItems() {
            return head != null;
        }
    }

    static class Node {
        State state;
        Node node;
        Object item;
        long bytesOffered;
        int urgentFrameCount;

        Node(OutboundFrame frame) {
            this.item = frame;
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
                    ", item=" + item +
                    ", bytesOffered=" + bytesOffered +
                    ", prev=" + node +
                    '}';
        }
    }
}
