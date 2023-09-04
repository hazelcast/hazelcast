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
import com.hazelcast.internal.tpcengine.util.CircularQueue;
import com.hazelcast.internal.tpcengine.util.Reference;
import org.jctools.queues.MpscArrayQueue;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.internal.tpcengine.CompletelyFairScheduler.niceToWeight;
import static com.hazelcast.internal.tpcengine.Task.RUN_BLOCKED;
import static com.hazelcast.internal.tpcengine.Task.RUN_COMPLETED;
import static com.hazelcast.internal.tpcengine.Task.RUN_YIELD;
import static com.hazelcast.internal.tpcengine.util.EpochClock.epochNanos;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;
import static java.lang.Math.max;

/**
 * A TaskQueue is the unit of scheduling within the {@link Scheduler}. Each
 * eventloop has a default TaskQueue. But it is also possible to create additional
 * TaskQueues. For example when you have tasks for clients, but also long
 * running tasks from e.g. some compaction process, you could give the clients
 * and the compaction process their own taskQueues. If no clients are busy,
 * the compaction process can get all resources. But when clients need to CPU,
 * they can get it.
 * <p>
 * The TaskQueue can be configured as concurrent. When it is concurrent it
 * is threadsafe (the Queue needs to be threadsafe) and tasks can be offered
 * outside of the eventloop. Concurrent TaskQueues are more expensive than
 * non concurrent one because:
 * <ol>
 *     <li>the overhead of the thread-safe queue.</li>
 *     <li>the overhead of registering the queue for checking for outside
 *     events periodically.</li>
 * </ol>
 * <p/>
 * Tasks are processed in FIFO order.
 * <p>
 * TaskGroups are relatively cheap. A task group that is blocked, will not be
 * on the run queue of the scheduler. But if the taskqueue is concurrent,
 * periodically a check will be done to see if there are tasks on the queue
 * queue.
 * <p>
 * Every TaskQueue has a vruntime which stands for virtual runtime. This is used
 * by the {@link CompletelyFairScheduler} to pick the TaskQueue with the lowest
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
 */
@SuppressWarnings({"checkstyle:VisibilityModifier"})
public final class TaskQueue implements Comparable<TaskQueue> {

    static final int STATE_RUNNING = 1;
    static final int STATE_BLOCKED = 2;

    // the interval in which the time on the CPU is measured. 1 means every
    // interval.
    int clockSampleInterval;
    int runState = STATE_BLOCKED;
    String name;
    // the queue for tasks
    Queue<Object> queue;
    boolean concurrent;
    // any runnable on the queue will be processed as is.
    // any Task on the queue will also be processed according to the contract
    // of the task. anything else is offered to the taskFactory to be wrapped
    // inside a task.
    TaskRunner taskRunner;
    // The eventloop this TaskQueue belongs to.
    Eventloop eventloop;
    // The scheduler that processed the TaskQueue.
    Scheduler scheduler;
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

    // the start time of this TaskQueue
    long startNanos;

    // The TakGroup is an intrusive double-linked-list-node. This is used to
    // keep track of blocked outside tasksGroups in the TaskQueueScheduler
    TaskQueue prev;
    TaskQueue next;

    final Metrics metrics = new Metrics();
    //the weight is only used by the CfsTaskQueueScheduler.
    int weight = 1;
    final Reference taskRef = new Reference();

    /**
     * Returns the Metrics. This method is threadsafe.
     *
     * @return the Metrics.
     */
    public Metrics metrics() {
        return metrics;
    }

    boolean isEmpty() {
        return queue.isEmpty();
    }

    int size() {
        return queue.size();
    }

    /**
     * Runs as much work from the TaskQueue as allowed.
     * <p>
     * This loop is very sensitive to changes; especially when there are many
     * tasks that can be processed in batch. So don't try to pull out methods
     * or make modifications if you don't have an extensive set of benchmarks
     * to back it up.
     */
    @SuppressWarnings({"checkstyle:NPathComplexity",
            "checkstyle:MethodLength", "checkstyle:CyclomaticComplexity"})
    void run(final RunContext runCtx) throws Exception {
        final Reactor.Metrics reactorMetrics = runCtx.reactorMetrics;
        final TaskRunner taskRunner = this.taskRunner;
        final Reference taskRef = this.taskRef;
        final Metrics metrics = this.metrics;
        final Scheduler scheduler = this.scheduler;
        final Queue queue = this.queue;

        runCtx.taskDeadlineNanos = runCtx.nowNanos + scheduler.timeSliceNanosActive();

        // The time the taskGroup has spend on the CPU.
        long cpuTimeNanos = 0;
        //  int taskProcessedCount = 0;
        boolean taskQueueEmpty = false;
        // This forces immediate time measurement of the first task.
        int clockSampleRound = 1;
        // Process the tasks in a queue as long as the deadline is not exceeded.

        while (runCtx.nowNanos <= runCtx.taskDeadlineNanos) {
            // Find the next task to perform
            Object task = queue.poll();
            if (task == null) {
                // queue is empty, we are done.
                taskQueueEmpty = true;
                break;
            }

            runCtx.taskStartNanos = runCtx.nowNanos;

            // process the task and handle potential exceptions
            int runResult;
            taskRef.value = task;
            try {
                runResult = taskRunner.run(taskRef);
            } catch (Throwable e) {
                metrics.incTaskErrorCount();
                runResult = taskRunner.handleError(taskRef, e);
            }
            task = taskRef.value;
            metrics.incTaskCsCount();

            // deal with the runResult.
            switch (runResult) {
                case RUN_BLOCKED:
                    break;
                case RUN_YIELD:
                    // todo: return
                    queue.offer(task);
                    break;
                case RUN_COMPLETED:
                    break;
                default:
                    throw new IllegalStateException();
            }
            reactorMetrics.incTaskCsCount();

            // Updat the time if needed
            if (clockSampleRound == 1) {
                runCtx.nowNanos = epochNanos();
                clockSampleRound = clockSampleInterval;
            } else {
                clockSampleRound--;
            }

            long taskEndNanos = runCtx.nowNanos;
            // make sure that a task always progresses the time.
            long taskCpuTimeNanos = max(runCtx.taskStartNanos - taskEndNanos, 1);
            cpuTimeNanos += taskCpuTimeNanos;

            // handle stall violations.
            if (taskCpuTimeNanos > runCtx.stallThresholdNanos) {
                runCtx.stallHandler.onStall(
                        eventloop.reactor, this, task, runCtx.taskStartNanos, taskCpuTimeNanos);
            }

            // periodically we need to tick the io schedulers.
            if (runCtx.nowNanos >= runCtx.ioDeadlineNanos) {
                eventloop.ioSchedulerTick();
                runCtx.nowNanos = epochNanos();
                runCtx.ioDeadlineNanos = runCtx.nowNanos + runCtx.ioIntervalNanos;
            }
        }
        taskRef.value = null;
        scheduler.updateActive(cpuTimeNanos);
        metrics.incCpuTimeNanos(cpuTimeNanos);
        reactorMetrics.incTaskQueueCsCount();

        if (taskQueueEmpty || isEmpty()) {
            // the taskQueue has been fully drained.
            scheduler.dequeueActive();
            runState = STATE_BLOCKED;
            metrics.incBlockedCount();
            if (queue != null) {
                // add it to the shared taskQueues so the eventloop will see
                // any items that are written to outside queues
                scheduler.addConcurrentBlocked(this);
            }
        } else {
            // Task queue wasn't fully drained, so the taskQueue is going to yield.
            scheduler.yieldActive();
        }
    }

    @Override
    public int compareTo(TaskQueue that) {
        return Long.compare(this.virtualRuntimeNanos, that.virtualRuntimeNanos);
    }

    /**
     * Offers a task.
     * <p/>
     * This method is thread-safe.
     *
     * @param task the task to offer.
     * @return true if the task was successfully offered, false otherwise.
     * @throws IllegalStateException if the method was called outside of the
     *                               eventloop thread and the TaskQueue isn't
     *                               concurrent.
     */
    public boolean offer(Object task) {
        checkNotNull(task, "task");

        if (Thread.currentThread() == eventloop.eventloopThread) {
            if (!queue.offer(task)) {
                return false;
            }

            if (runState == STATE_RUNNING) {
                return true;
            }

            if (concurrent) {
                // If is concurrent, we don't need to notified of any events
                // because the queue will register itself if it blocks.
                scheduler.removeConcurrentBlocked(this);
            }

            scheduler.enqueue(this);
            return true;
        } else {
            if (!concurrent) {
                throw new IllegalStateException(
                        "Can't offer a task to non concurrent TaskGroup " + name
                                + " outside of the EventloopThread. "
                                + "Current thread is " + Thread.currentThread().getName());
            }

            if (!queue.offer(task)) {
                return false;
            }

            // by waking up the reactor, the queue of is guaranteed to be scanned.
            eventloop.reactor.wakeup();
            return true;
        }
    }

    @Override
    public String toString() {
        return "TaskQueue{"
                + "name='" + name + '\''
                + ", runState=" + runState
                + ", weight=" + weight
                + ", sumExecRuntimeNanos=" + actualRuntimeNanos
                + ", vruntimeNanos=" + virtualRuntimeNanos
                + ", startNanos=" + startNanos
                + '}';
    }

    static class RunContext {
        Eventloop eventloop;
        Reactor.Metrics reactorMetrics;
        Scheduler scheduler;
        StallHandler stallHandler;
        long minGranularityNanos;
        long stallThresholdNanos;

        // the last measured epoch time in nanos.
        // {@link EpochClock#epochNanos()} is pretty expensive (+/-25ns)
        // due to {@link System#nanoTime()}. For every task processed we do
        // not want to call the {@link EpochClock#epochNanos()} more than
        // once because the clock already dominates the context switch time.
        long nowNanos;
        // epoch time in nanos when the current task from the taskGroup started.
        long ioIntervalNanos;
        // the epoch time in nano seconds the next ioSchedulerTick needs to run
        long ioDeadlineNanos;
        // the epoch time in nanos the current task started. If no task is running,
        // this value is undefined.
        long taskStartNanos;
        // the deadline in time for tasks in the current taskGroup. So no further
        // task should be run and the taskGroup should yield (or complete).
        long taskDeadlineNanos;
    }

    /**
     * Contains the metrics for a {@link TaskQueue}.
     * <p/>
     * The metrics should only be updated by the event loop thread, but can be
     * read by any thread.
     */
    public static final class Metrics {
        private static final VarHandle TASK_CS_COUNT;
        private static final VarHandle CPU_TIME_NANOS;
        private static final VarHandle TASK_ERROR_COUNT;
        private static final VarHandle BLOCKED_COUNT;

        private volatile long taskCsCount;
        private volatile long taskErrorCount;
        private volatile long cpuTimeNanos;
        private volatile long blockedCount;

        static {
            try {
                MethodHandles.Lookup l = MethodHandles.lookup();
                TASK_CS_COUNT = l.findVarHandle(Metrics.class, "taskCsCount", long.class);
                TASK_ERROR_COUNT = l.findVarHandle(Metrics.class, "taskErrorCount", long.class);
                CPU_TIME_NANOS = l.findVarHandle(Metrics.class, "cpuTimeNanos", long.class);
                BLOCKED_COUNT = l.findVarHandle(Metrics.class, "blockedCount", long.class);
            } catch (ReflectiveOperationException e) {
                throw new ExceptionInInitializerError(e);
            }
        }

        /**
         * Returns the number of times this TaskQueue was blocked (so didn't
         * have any work to do).
         *
         * @return the number of times blocked.
         */
        public long blockedCount() {
            return (long) BLOCKED_COUNT.getOpaque(this);
        }

        /**
         * Increases the number of times this TaskQueue was blocked by 1.
         */
        public void incBlockedCount() {
            BLOCKED_COUNT.setOpaque(this, (long) BLOCKED_COUNT.getOpaque(this) + 1);
        }

        /**
         * Returns the number of errors the TaskQueue encountered while processing
         * tasks.
         *
         * @return the number of errors.
         */
        public long taskErrorCount() {
            return (long) TASK_ERROR_COUNT.getOpaque(this);
        }

        /**
         * Increases the number of errors the TaskQueue encountered by 1.
         */
        public void incTaskErrorCount() {
            TASK_ERROR_COUNT.setOpaque(this, (long) TASK_ERROR_COUNT.getOpaque(this) + 1);
        }

        /**
         * Returns the number of task context switches.
         *
         * @return the number of task context switches.
         */
        public long taskCsCount() {
            return (long) TASK_CS_COUNT.getOpaque(this);
        }

        /**
         * Increases the number of task context switches by 1.
         */
        public void incTaskCsCount() {
            TASK_CS_COUNT.setOpaque(this, (long) TASK_CS_COUNT.getOpaque(this) + 1);
        }

        /**
         * Returns the amount of time in nanoseconds this TaskQueue was on the
         * CPU. The actual time this task was on the CPU can't be measured
         * because it could be that the OS ran other processes while tasks from
         * this TaskQueue were being processed. So probably we need to come up
         * with a less missleading name for this metric.
         *
         * @return the amount of time this TaskQueue was on the CPU.
         */
        public long cpuTimeNanos() {
            return (long) CPU_TIME_NANOS.getOpaque(this);
        }

        /**
         * Increases the amount of time this TaskQueue was on the CPU with the
         * given delta.
         *
         * @param delta
         */
        public void incCpuTimeNanos(long delta) {
            CPU_TIME_NANOS.setOpaque(this, (long) CPU_TIME_NANOS.getOpaque(this) + delta);
        }
    }

    /**
     * A {@link Builder} is used to configure and create a {@link TaskQueue}.
     */
    public static final class Builder extends AbstractBuilder<TaskQueue> {

        public static final int MIN_NICE = -10;
        public static final int MAX_NICE = 10;
        public static final int DEFAULT_QUEUE_CAPACITY = 16384;

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
         * Sets queue for the task Queue.
         */
        public Queue<Object> queue;

        /**
         * If the Queue can be accessed outside of the eventloop. If concurrent
         * is set to true, then the queue needs to be a threadsafe queue (mpsc is
         * sufficient), and otherwise it doesn't and a {@link CircularQueue} is
         * a good option.
         */
        public boolean concurrent = true;

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
         * Sets the {@link TaskRunner} that will be used to run tasks from
         * {@link TaskQueue}. So here you can application specific logic how
         * you to run the tasks.
         */
        public TaskRunner taskRunner;

        /**
         * Builder needs to be created through {@link Eventloop#newTaskQueueBuilder()}.
         */
        public Builder() {
        }

        @Override
        protected void conclude() {
            super.conclude();

            checkNotNull(eventloop, "eventloop");

            eventloop.checkOnEventloopThread();

            if (nice < MIN_NICE) {
                throw new IllegalArgumentException("nice can't be smaller than " + MIN_NICE);
            } else if (nice > MAX_NICE) {
                throw new IllegalArgumentException("nice can't be larger than " + MAX_NICE);
            }

            if (queue == null) {
                if (concurrent) {
                    queue = new MpscArrayQueue<>(DEFAULT_QUEUE_CAPACITY);
                } else {
                    queue = new CircularQueue<>(DEFAULT_QUEUE_CAPACITY);
                }
            }

            if (eventloop.scheduler.taskQueues.size() == eventloop.scheduler.runQueueLimit()) {
                throw new IllegalArgumentException("Too many taskgroups.");
            }

            if (name == null) {
                name = "taskqueue-" + ID.incrementAndGet();
            }

            if (taskRunner == null) {
                taskRunner = DefaultTaskRunner.INSTANCE;
            }
        }

        @Override
        protected TaskQueue construct() {
            TaskQueue taskQueue = new TaskQueue();
            taskQueue.startNanos = epochNanos();
            taskQueue.queue = queue;
            taskQueue.concurrent = concurrent;
            taskQueue.clockSampleInterval = clockSampleInterval;
            taskQueue.taskRunner = taskRunner;
            taskRunner.init(eventloop);
            taskQueue.name = name;
            taskQueue.eventloop = eventloop;
            taskQueue.scheduler = eventloop.scheduler;
            taskQueue.runState = STATE_BLOCKED;
            taskQueue.weight = niceToWeight(nice);

            if (taskQueue.queue != null) {
                eventloop.scheduler.addConcurrentBlocked(taskQueue);
            }

            eventloop.scheduler.taskQueues.add(taskQueue);
            eventloop.reactor.taskQueues.add(taskQueue);
            return taskQueue;
        }
    }
}
