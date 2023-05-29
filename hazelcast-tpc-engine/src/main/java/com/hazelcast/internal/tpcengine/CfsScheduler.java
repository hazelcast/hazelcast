package com.hazelcast.internal.tpcengine;

import java.util.PriorityQueue;

import static java.lang.Math.max;

/**
 * https://docs.kernel.org/scheduler/sched-design-CFS.html
 */
class CfsScheduler {

    private PriorityQueue<TaskGroup> runQueue = new PriorityQueue();

    private long min_vruntime;

    /**
     * Returns the number of items in the runQueue.
     *
     * @return the size of the runQueue.
     */
    public int size() {
        return runQueue.size();
    }

    public TaskGroup pickNext() {
        TaskGroup group = runQueue.poll();
        if (group == null) {
            return null;
        }

        TaskGroup peek = runQueue.peek();
        if (peek != null) {
            min_vruntime = peek.vruntimeNanos;
        }

        return group;
    }

    public void enqueue(TaskGroup taskGroup) {
        taskGroup.state = TaskGroup.STATE_RUNNING;
        taskGroup.vruntimeNanos = max(taskGroup.vruntimeNanos, min_vruntime);
        runQueue.add(taskGroup);
    }
}
