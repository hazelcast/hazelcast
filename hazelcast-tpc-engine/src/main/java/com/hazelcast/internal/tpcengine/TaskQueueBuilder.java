package com.hazelcast.internal.tpcengine;

import com.hazelcast.internal.TaskQueueHandle;

import java.util.Queue;

import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkPositive;

public class TaskQueueBuilder {

    private final Eventloop eventloop;
    private String name;
    private int shares;
    private Queue<Object> queue;
    private boolean concurrent;

    public TaskQueueBuilder(Eventloop eventloop) {
        this.eventloop = eventloop;
    }

    public TaskQueueBuilder setName(String name) {
        this.name = checkNotNull(name, name);
        return this;
    }

    public TaskQueueBuilder setShares(int shares) {
        this.shares = checkPositive(shares, "shares");
        return this;
    }

    public TaskQueueBuilder setConcurrent(boolean concurrent) {
        this.concurrent = concurrent;
        return this;
    }

    public TaskQueueBuilder setQueue(Queue<Object> queue) {
        this.queue = checkNotNull(queue, "queue");
        return this;
    }

    public TaskQueueHandle build() {
        // todo: name check
        // todo: already build check
        // todo: loop active check

        TaskQueue taskQueue = new TaskQueue(name, shares, queue);

        eventloop.taskQueues = add(taskQueue, eventloop.taskQueues);
        if (concurrent) {
            eventloop.concurrentTaskQueues = add(taskQueue, eventloop.concurrentTaskQueues);
        }

        return new TaskQueueHandle(eventloop.taskQueues.length - 1);
    }

    private TaskQueue[] add(TaskQueue taskQueue, TaskQueue[] oldArray) {
        TaskQueue[] newArray = new TaskQueue[oldArray.length + 1];
        System.arraycopy(oldArray, 0, newArray, 0, oldArray.length);
        newArray[oldArray.length] = taskQueue;
        return newArray;
    }
}
