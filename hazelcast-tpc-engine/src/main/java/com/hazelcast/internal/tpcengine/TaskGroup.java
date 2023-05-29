package com.hazelcast.internal.tpcengine;

import com.hazelcast.internal.tpcengine.logging.TpcLogger;
import com.hazelcast.internal.tpcengine.logging.TpcLoggerLocator;

import java.util.Queue;

/**
 * vruntime/pruntime
 * This number could be distorted when there are other threads running on the same CPU because
 * If a different task would be executed while a task is running on the CPU, the measured time
 * will include the time of that task as well.
 */
public class TaskGroup implements Comparable<TaskGroup> {
    public static final int STATE_RUNNING = 0;
    public static final int STATE_BLOCKED = 1;

    // the total number of nanoseconds this task has spend on the CPU
    public long vruntimeNanos = 0;
    protected final TpcLogger logger = TpcLoggerLocator.getLogger(getClass());

    public int state;
    public String name;
    public int shares;
    public Queue<Object> queue;
    public Scheduler scheduler;
    public int size;
    public boolean concurrent;
    public Eventloop eventloop;
    // the actual amount of time this task has spend on the CPU
    public long pruntimeNanos;
    public long tasksProcessed;
    public long blockedCount;

    @Override
    public int compareTo(TaskGroup that) {
        if (that.vruntimeNanos == this.vruntimeNanos) {
            return 0;
        }

        return this.vruntimeNanos > that.vruntimeNanos ? 1 : -1;
    }

    public boolean offer(Object task) {
        if (!queue.offer(task)) {
            return false;
        }

        if (!concurrent && state == STATE_BLOCKED) {
            eventloop.scheduler.enqueue(this);
        }

        return true;
    }

    TaskGroup left, right, parent;
    int color;
}