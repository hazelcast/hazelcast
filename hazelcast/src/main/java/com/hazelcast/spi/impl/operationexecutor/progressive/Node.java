package com.hazelcast.spi.impl.operationexecutor.progressive;

import static com.hazelcast.spi.impl.operationexecutor.progressive.PartitionQueueState.Executing;
import static com.hazelcast.spi.impl.operationexecutor.progressive.PartitionQueueState.ExecutingPriority;
import static com.hazelcast.spi.impl.operationexecutor.progressive.PartitionQueueState.Parked;
import static com.hazelcast.spi.impl.operationexecutor.progressive.PartitionQueueState.Stolen;
import static com.hazelcast.spi.impl.operationexecutor.progressive.PartitionQueueState.StolenUnparked;
import static com.hazelcast.spi.impl.operationexecutor.progressive.PartitionQueueState.Unparked;
import static com.hazelcast.spi.impl.operationexecutor.progressive.PartitionQueueState.UnparkedPriority;

/**
 * A node stores work. It is single-linked-list.  So work is first stored in a stack, and later
 * it is reversed so you get buffer behavior.
 * <p/>
 * State of this object is considered immutable after publication in the PartitionQueue.
 */
class Node {

    public final static int HISTORY = 10;

    static final Node PARKED = new Node(Parked);
    static final Node UNPARKED = new Node(Unparked);
    static final Node UNPARKED_PRIORITY = new Node(UnparkedPriority);
    static final Node STOLEN = new Node(Stolen);
    static final Node STOLEN_UNPARKED = new Node(StolenUnparked);
    static final Node EXECUTING = new Node(Executing);
    static final Node EXECUTING_PRIORITY = new Node(ExecutingPriority);

    PartitionQueueState state;
    int normalSize;
    int prioritySize;
    Object task;
    Node prev;
    boolean hasPriority;
    PartitionQueueState previousState;

    Node[] history;

    Node() {
    }

    Node(PartitionQueueState state) {
        this.state = state;
    }

    int size() {
        return normalSize + prioritySize;
    }

    void parked(Node prev){

    }

    //todo:
    // a method which makes a copy of an original node, only with a change new state.
    // currently we just add a node on top, but we don't need this dummy node. We can
    // just take the previous node

    void withNewState(Node node, PartitionQueueState state) {
        this.previousState = node.state;
        this.state = state;
        this.normalSize = node.normalSize;
        this.prioritySize = node.prioritySize;
        this.task = node.task;
        this.prev = node.prev;
        this.hasPriority = node.hasPriority;

        if (state == PartitionQueueState.Parked) {
            assert normalSize == 0;
            assert prioritySize == 0;
        }
    }

    void init(Object task, PartitionQueueState state, Node prev) {
        this.hasPriority = false;
        this.task = task;
        this.state = state;
        this.previousState = prev == null ? null : prev.state;

        int taskSize = task == null ? 0 : 1;

        if (prev == null || prev.size() == 0) {
            this.prev = null;
            this.normalSize = taskSize;
            this.prioritySize = 0;
        } else {
            this.prev = prev;
            this.normalSize = prev.normalSize + taskSize;
            this.prioritySize = prev.prioritySize;

        }

        if (state == PartitionQueueState.Parked) {
            assert normalSize == 0;
            assert prioritySize == 0;
        }
    }

    void priorityInit(Object task, PartitionQueueState state, Node prev) {
        this.hasPriority = true;
        this.task = task;
        this.state = state;
        this.previousState = prev == null ? null : prev.state;

        int taskSize = task == null ? 0 : 1;
        if (prev == null || prev.size() == 0) {
            this.prev = null;
            this.normalSize = 0;
            this.prioritySize = taskSize;
        } else {
            this.prev = prev;
            this.normalSize = prev.normalSize;
            this.prioritySize = prev.prioritySize + taskSize;
        }

        if (state == PartitionQueueState.Parked) {
            assert normalSize == 0;
            assert prioritySize == 0;
        }
    }

    @Override
    public String toString() {
        return "Node{"
                + "state=" + state
                + ", prevState=" + previousState
                + ", normalSize=" + normalSize
                + ", prioritySize=" + prioritySize
                + ", hasPriority=" + hasPriority
                + ", prev=" + prev
                + ", task=" + task
                + '}';
    }
}
