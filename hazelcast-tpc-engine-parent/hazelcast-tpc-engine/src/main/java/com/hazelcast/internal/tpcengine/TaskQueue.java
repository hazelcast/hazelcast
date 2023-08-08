/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.tpcengine;

import com.hazelcast.internal.tpcengine.util.AbstractBuilder;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.internal.tpcengine.CfsTaskQueueScheduler.niceToWeight;
import static com.hazelcast.internal.tpcengine.util.EpochClock.epochNanos;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkOnEventloopThread;

/**
 * A TaskQueue is the unit of scheduling within the eventloop. Each eventloop
 * has a default TaskQueue. But it is also possible to create additional
 * TaskQueues. For example when you have tasks from clients, but also long
 * running tasks from e.g. some compaction process, you could give the clients
 * and the compaction process their own taskQueues. If no clients are busy,
 * the compaction process can get all resources. But when clients need to CPU,
 * they can get it.
 * <p>
 * The TaskQueue can be configured with either either (or both):
 * <ol>
 *     <li>inside queue: for tasks submitted within the eventloop. This queue
 *     doesn't need to be thread safe.</li>
 *     <li>outside queue: for tasks submitted outside of the eventloop. This
 *     queue needs to be thread safe.</li>
 * </ol>
 * <p/>
 * When there is only 1 queue, Tasks in the same TaskQueue will be processed in
 * FIFO order. When there are 2 queues, tasks will be picked in round robin
 * fashion and tasks in the same queue will be picked in FIFO order.
 * <p>
 * TaskGroups are relatively cheap. A task group that is blocked, will not be
 * on the run queue of the scheduler. But if the task group has a outside queue,
 * periodically a check will be done to see if there are tasks on the outside
 * queue.
 * <p>
 * Every TaskQueue has a vruntime which stands for virtual runtime. This is used
 * by the {@link CfsTaskQueueScheduler} to pick the TaskQueue with the lowest
 * vruntime.
 * <p>
 * vruntime/pruntime
 * This number could be distorted when there are other threads running on the
 * same CPU because If a different task would be executed while a task is running
 * on the CPU, the measured time will include the time of that task as well.
 * <p>
 * In Linux terms the TaskQueue would be the sched_entity.
 * <p>
 * The TaskQueue is inspired by the <a href="https://github.com/DataDog/glommio">Glommio</>
 * TaskQueue.
 * <p>
 * The TaskQueue isn't threadsafe and should only be used from the eventloop thread.
 * The only method which is threadsafe is the {@link #offerOutside(Object)} since
 * jobs can be offered outside of the eventloop.
 */
@SuppressWarnings({"checkstyle:VisibilityModifier"})
public final class TaskQueue implements Comparable<TaskQueue> {

    public static final int POLL_INSIDE_ONLY = 1;
    public static final int POLL_OUTSIDE_ONLY = 2;
    public static final int POLL_INSIDE_FIRST = 3;
    public static final int POLL_OUTSIDE_FIRST = 4;

    public static final int RUN_STATE_RUNNING = 1;
    public static final int RUN_STATE_BLOCKED = 2;

    int pollState;

    // the interval in which the time on the CPU is measured. 1 means every
    // interval.
    int clockSampleInterval;
    int runState = RUN_STATE_BLOCKED;
    String name;
    Queue<Object> inside;
    Queue<Object> outside;

    // any runnable on the queue will be processed as is.
    // any Task on the queue will also be processed according to the contract
    // of the task. anything else is offered to the taskFactory to be wrapped
    // inside a task.
    TaskProcessor processor;
    Eventloop eventloop;
    TaskQueueScheduler scheduler;
    // The accumulated amount of time this task has spend on the CPU. If there
    // are other threads running on the same processor, sumExecRuntimeNanos can
    // be distorted because these threads can contribute to the runtime of this
    // taskQueue if such a thread gets context switched while a task of the
    // TaskQueue is running.
    long actualRuntimeNanos;
    // Field is only used when the TaskQueue is scheduled by the CfsTaskQueueScheduler.
    // the virtual runtime. The vruntime is weighted + also when reinserted into
    // the tree, the vruntime is always updated to the min_vruntime. So the vruntime
    // isn't the actual amount of time spend on the CPU
    long virtualRuntimeNanos;
    long tasksProcessed;
    // the number of times this taskQueue has been blocked
    long blockedCount;
    // the number of times this taskQueue has been context switched.
    long contextSwitchCount;

    // the start time of this TaskQueue
    long startNanos;

    // The TakGroup is an intrusive double-linked-list-node. This is used to keep track
    // of blocked shared tasksGroups.
    TaskQueue prev;
    TaskQueue next;

    final Metrics metrics = new Metrics();
    //the weight is only used by the CfsTaskQueueScheduler.
    int weight = 1;
    Object task;

    boolean isEmpty() {
        return (inside != null && inside.isEmpty()) && (outside != null && outside.isEmpty());
    }

    int size() {
        return (inside == null ? 0 : inside.size()) + (outside == null ? 0 : outside.size());
    }

    /**
     * Selects the next task from the queues.
     *
     * @return true if there was a task, false otherwise.
     */
    boolean next() {
        switch (pollState) {
            case POLL_INSIDE_ONLY:
                task = inside.poll();
                break;
            case POLL_OUTSIDE_ONLY:
                task = outside.poll();
                break;
            case POLL_OUTSIDE_FIRST:
                task = outside.poll();
                if (task != null) {
                    pollState = POLL_INSIDE_FIRST;
                } else {
                    task = inside.poll();
                    if (task == null) {
                        pollState = POLL_INSIDE_FIRST;
                    }
                }
                break;
            case POLL_INSIDE_FIRST:
                task = inside.poll();
                if (task != null) {
                    pollState = POLL_OUTSIDE_FIRST;
                } else {
                    task = outside.poll();
                    if (task == null) {
                        pollState = POLL_OUTSIDE_FIRST;
                    }
                }
                break;
            default:
                throw new IllegalStateException("Unknown pollState:" + pollState);
        }

        return task != null;
    }

    /**
     * Polls for a single Runnable. If only the inside queue is set, a poll is
     * done from the inside queue. If only a outside queue is set, a poll is done
     * from the outside queue. If both inside and outside queue are set, then a round
     * robin poll is done over these 2 queues.
     *
     * @return the Runnable that is next or <code>null</code> if this TaskQueue
     * has no more tasks to execute.
     */
    void run() {
        assert task != null;

        try {
            if (processor != null) {
                processor.process(task);
            } else {
                ((Runnable) task).run();
            }
        } catch (Exception e) {
            // todo: exception handling needs to improve.
            e.printStackTrace();
        } finally {
            tasksProcessed++;
        }
    }

    /**
     * Offers a task to the inside queue
     * <p>
     * Should only be done from the eventloop thread.
     *
     * @param task the task to offer.
     * @return true if task was successfully offered, false if the task was
     * rejected.
     * @throws NullPointerException throws if task is null or when inside queue
     *                              is null.
     */
    public boolean offerInside(Object task) {
        if (!inside.offer(task)) {
            return false;
        }

        if (runState == RUN_STATE_RUNNING) {
            return true;
        }

        if (outside != null) {
            eventloop.removeBlockedOutside(this);
        }

        scheduler.enqueue(this);
        return true;
    }

    /**
     * Offers a task to the outside queue.
     *
     * This method is threadsafe since it can be called outside of the eventloop.
     *
     * @param task the task to offer.
     * @return true if task was successfully offered, false if the task was
     * rejected.
     * @throws NullPointerException if task or outside is null.
     */
    boolean offerOutside(Object task) {
        return outside.offer(task);
    }

    @Override
    public int compareTo(TaskQueue that) {
        return Long.compare(this.virtualRuntimeNanos, that.virtualRuntimeNanos);
    }

    @Override
    public String toString() {
        return "TaskQueue{"
                + "name='" + name + '\''
                + ", pollState=" + pollState
                + ", runState=" + runState
                + ", weight=" + weight
                + ", sumExecRuntimeNanos=" + actualRuntimeNanos
                + ", vruntimeNanos=" + virtualRuntimeNanos
                + ", tasksProcessed=" + tasksProcessed
                + ", blockedCount=" + blockedCount
                + ", contextSwitchCount=" + contextSwitchCount
                + ", startNanos=" + startNanos
                + ", prev=" + prev
                + ", next=" + next
                + '}';
    }

    /**
     * Contains the metrics for a {@link TaskQueue}.
     * <p/>
     * The metrics should only be updated by the event loop thread, but can be
     * read by any thread.
     */
    public static final class Metrics {
        private static final VarHandle TASKS_PROCESSED_COUNT;
        private static final VarHandle CPU_TIME_NANOS;

        private volatile long taskCompletedCount;
        private volatile long cpuTimeNanos;

        static {
            try {
                MethodHandles.Lookup l = MethodHandles.lookup();
                TASKS_PROCESSED_COUNT = l.findVarHandle(Metrics.class, "taskCompletedCount", long.class);
                CPU_TIME_NANOS = l.findVarHandle(Metrics.class, "cpuTimeNanos", long.class);
            } catch (ReflectiveOperationException e) {
                throw new ExceptionInInitializerError(e);
            }
        }

        public long taskProcessCount() {
            return (long) TASKS_PROCESSED_COUNT.getOpaque(this);
        }

        public void incTasksProcessedCount(int delta) {
            TASKS_PROCESSED_COUNT.setOpaque(this, (long) TASKS_PROCESSED_COUNT.getOpaque(this) + delta);
        }

        public long cpuTimeNanos() {
            return (long) CPU_TIME_NANOS.getOpaque(this);
        }

        public void incCpuTimeNanos(long delta) {
            CPU_TIME_NANOS.setOpaque(this, (long) CPU_TIME_NANOS.getOpaque(this) + delta);
        }
    }

    /**
     * A handle to a {@link TaskQueue}. A TaskQueue should not be directly
     * accessed and the interactions like destruction, changing priorities,
     * adding tasks etc, should be done through the TaskQueueHandle.
     */
    @SuppressWarnings({"checkstyle:VisibilityModifier"})
    public static class Handle {
        // todo: the visibility should be reduced.
        public final TaskQueue queue;
        private final Metrics metrics;

        public Handle(TaskQueue queue, Metrics metrics) {
            this.queue = checkNotNull(queue, "queue");
            this.metrics = checkNotNull(metrics, "metrics");
        }

        /**
         * Returns the TaskQueueMetrics associated with the TaskQueue this
         * handle is referring to.
         *
         * @return the metrics.
         */
        public Metrics metrics() {
            return metrics;
        }

        @Override
        public String toString() {
            return queue.name;
        }
    }

    /**
     * A {@link Builder} is used to configure and create a {@link TaskQueue}.
     */
    public static final class Builder extends AbstractBuilder<Handle> {

        public static final int MIN_NICE = -20;
        public static final int MAX_NICE = 20;

        private static final AtomicLong ID = new AtomicLong();

        /**
         * The Eventloop this Context belongs to.
         */
        public Eventloop eventloop;

        /**
         * Sets the name of the TaskQueue. The name is used for logging and
         * debugging purposes.
         */
        public String name;

        /**
         * Sets the nice value. When the CfsScheduler is used, the nice value
         * determines the size of the time slice and the priority of the task
         * queue. For the FcfsTaskQueueScheduler, the value is ignored.
         * <p>
         * -20 is the lowest nice, which means the task isn't nice at all and
         * wants to spend as much time on the CPU as possible. 20 is the highest
         * nice value, which means the task is fine giving up its time on the CPU
         * for any less nicer task queue.
         * <p>
         * A task that has a nice level of <code>n</code> will get 20 percent larger
         * time slice than a task with a priority of <code>n-1</code>.
         */
        public int nice;

        /**
         * Sets the inside queue of the TaskQueue. The inside queue is should be
         * used for tasks generated within the eventloop. The inside queue doesn't
         * need to be thread-safe.
         */
        public Queue<Object> inside;

        /**
         * Sets the outside queue of the TaskQueue. The outside queue is should be
         * used for tasks generated outside of the eventloop and therefor must
         * be thread-safe.
         */
        public Queue<Object> outside;

        /**
         * Measuring the execution time of every task in a TaskQueue can be
         * expensive. To reduce the overhead, the  clock sample interval option
         * can be used. This will only measure the execution time out of every
         * n tasks within the TaskQueue. There are a few drawback with setting
         * the interval to a value larger than 1:
         * <ol>
         *      <li>it can lead to skid where you wrongly identify a task as a
         *      stalling task. If interval is 10 and third task stalls, because
         *      time is measured at the the 10th task, task 10 will task will be
         *      seen as the stalling task even though the third task caused the
         *      problem.</li>
         *      <li>task group could run longer than desired.</li>
         *      <li>I/O scheduling could be delayed.</li>
         *      <li>Deadline scheduling could be delayed.</li>
         * </ol>
         * For the time being this option is mostly useful for benchmark and
         * performance tuning to reduce the overhead of calling System.nanotime.
         */
        public int clockSampleInterval = 1;

        /**
         * Sets the {@link TaskProcessor} that will be used to process tasks from
         * {@link TaskQueue}.
         */
        public TaskProcessor processor;

        @Override
        protected void conclude() {
            super.conclude();

            checkNotNull(eventloop, "eventloop");
            checkOnEventloopThread(eventloop);
            //checkNotNull(processor, "processor");

            if (nice < MIN_NICE) {
                throw new IllegalArgumentException("nice can't be smaller than " + MIN_NICE);
            } else if (nice > MAX_NICE) {
                throw new IllegalArgumentException("nice can't be larger than " + MAX_NICE);
            }

            if (Thread.currentThread() != eventloop.reactor.eventloopThread()) {
                throw new IllegalStateException("Can only call from eventloop thread");
            }

            if (inside == null && outside == null) {
                throw new IllegalStateException("The inside and outside queue can't both be null.");
            }

            if (eventloop.taskQueues.size() == eventloop.taskQueueScheduler.capacity()) {
                throw new IllegalStateException("Too many taskgroups.");
            }

            if (name == null) {
                name = "taskqueue-" + ID.incrementAndGet();
            }
        }

        @Override
        protected void prebuild() {
            super.prebuild();
            checkOnEventloopThread(eventloop);
        }

        @Override
        protected Handle doBuild() {
            TaskQueue taskQueue = new TaskQueue();
            taskQueue.startNanos = epochNanos();
            taskQueue.inside = inside;
            taskQueue.outside = outside;
            if (inside == null) {
                taskQueue.pollState = POLL_OUTSIDE_ONLY;
            } else if (outside == null) {
                taskQueue.pollState = POLL_INSIDE_ONLY;
            } else {
                taskQueue.pollState = POLL_OUTSIDE_FIRST;
            }
            taskQueue.clockSampleInterval = clockSampleInterval;
            taskQueue.processor = processor;
            taskQueue.name = name;
            taskQueue.eventloop = eventloop;
            taskQueue.scheduler = eventloop.taskQueueScheduler;
            taskQueue.runState = RUN_STATE_BLOCKED;
            taskQueue.weight = niceToWeight(nice);

            if (taskQueue.outside != null) {
                eventloop.addBlockedOutside(taskQueue);
            }

            eventloop.taskQueues.add(taskQueue);
            return new Handle(taskQueue, taskQueue.metrics);
        }
    }
}
