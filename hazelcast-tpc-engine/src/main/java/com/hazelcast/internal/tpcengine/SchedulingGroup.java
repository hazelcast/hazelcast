package com.hazelcast.internal.tpcengine;

import com.hazelcast.internal.tpcengine.logging.TpcLogger;
import com.hazelcast.internal.tpcengine.logging.TpcLoggerLocator;

import java.util.Queue;

// comparable for the PriorityQueue
public class SchedulingGroup implements Comparable<SchedulingGroup> {
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

    @Override
    public int compareTo(SchedulingGroup that) {
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
            eventloop.runQueue.insert(this);
        }

        return true;
    }

//
//    public boolean offer(Object task) {
//        return queue.offer(task);
//    }
//
//    // todo: problem is that empty is used for concurrent not sleeping
//    // and now the scheduler queue will trigger the wakeup as well even though it
//    // isn't concurrent
//    public boolean isEmpty() {
//        return queue.isEmpty() && scheduler.queue().isEmpty();
//    }
//
//    public boolean process() {
//        // todo: we are ignoring the scheduler
//        // todo: we should prevent running the same task in the same cycle.
//        for (int l = 0; l < shares; l++) {
//            Object task = queue.poll();
//            if (task == null) {
//                // there are no more tasks
//                break;
//            } else if (task instanceof Runnable) {
//                try {
//                    ((Runnable) task).run();
//                } catch (Exception e) {
//                    logger.warning(e);
//                }
//            } else {
//                try {
//                    scheduler.schedule(task);
//                } catch (Exception e) {
//                    logger.warning(e);
//                }
//            }
//        }
//
//        return !queue.isEmpty();
//    }

    SchedulingGroup left, right, parent;
    int color;

//    @Override
//    public void run() {
//        if (task != null) {
//            task.run();
//        }
//
//        if (periodNanos != -1 || delayNanos != -1) {
//            if (periodNanos != -1) {
//                deadlineNanos += periodNanos;
//            } else {
//                deadlineNanos = eventloop.nanoClock.nanoTime() + delayNanos;
//            }
//
//            if (deadlineNanos < 0) {
//                deadlineNanos = Long.MAX_VALUE;
//            }
//
//            if (!eventloop.scheduledTaskQueue.offer(this)) {
//                eventloop.logger.warning("Failed schedule task: " + this + " because there is no space in scheduledTaskQueue");
//            }
//        } else {
//            if (promise != null) {
//                promise.complete(null);
//            }
//        }
//    }

}