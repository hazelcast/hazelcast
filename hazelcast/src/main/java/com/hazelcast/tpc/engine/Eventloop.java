/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.tpc.engine;


import com.hazelcast.internal.util.ThreadAffinity;
import com.hazelcast.internal.util.executor.HazelcastManagedThread;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.tpc.engine.frame.Frame;
import com.hazelcast.tpc.util.CircularQueue;
import org.jctools.queues.MpmcArrayQueue;

import java.io.Closeable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;

import static com.hazelcast.internal.nio.IOUtil.closeResources;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static com.hazelcast.tpc.engine.EventloopState.*;
import static com.hazelcast.tpc.util.Util.epochNanos;
import static java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater;

/**
 * A EventLoop is a thread that is an event loop.
 *
 * The Eventloop infrastructure is unaware of what is being send. So it isn't aware of requests/responses.
 *
 * A single eventloop can deal with many server ports.
 */
public abstract class Eventloop {

    private static final EventloopTask SHUTDOWN_TASK = () -> {};

    protected final static AtomicReferenceFieldUpdater<Eventloop, EventloopState> STATE
            = newUpdater(Eventloop.class, EventloopState.class, "state");

    protected final ILogger logger = Logger.getLogger(getClass());
    protected final Set<Closeable> resources = new CopyOnWriteArraySet<>();

    public final AtomicBoolean wakeupNeeded = new AtomicBoolean(true);
    public final MpmcArrayQueue concurrentRunQueue;

    public final ConcurrentMap context = new ConcurrentHashMap();

    protected final Scheduler scheduler;
    public final CircularQueue<EventloopTask> localRunQueue;
    protected boolean spin;

    PriorityQueue<ScheduledTask> scheduledTaskQueue = new PriorityQueue();

    protected volatile EventloopState state = NEW;

    private final CountDownLatch terminationLatch = new CountDownLatch(1);

    public final Unsafe unsafe = new Unsafe();

    protected long earliestDeadlineEpochNanos = -1;

    protected final EventloopThread eventloopThread;

    public Eventloop(AbstractConfiguration config) {
        this.spin = config.spin;
        this.scheduler = config.scheduler;
        this.localRunQueue = new CircularQueue<>(config.localRunQueueCapacity);
        this.concurrentRunQueue = new MpmcArrayQueue(config.concurrentRunQueueCapacity);
        scheduler.eventloop(this);
        this.eventloopThread = new EventloopThread(config);
    }

    public Scheduler scheduler() {
        return scheduler;
    }

    public EventloopThread eventloopThread() {
        return eventloopThread;
    }

    /**
     * Returns the state of the Eventloop.
     *
     * This method is thread-safe.
     *
     * @return the state.
     */
    public final EventloopState state() {
        return state;
    }

    /**
     * Is called before the {@link #eventLoop()} is called.
     *
     * This method can be used to initialize resources.
     *
     * Is called from the {@link EventloopThread}.
     */
    protected void beforeEventloop() {
    }

    /**
     * Executes the actual eventloop.
     *
     * @throws Exception
     */
    protected abstract void eventLoop() throws Exception;

    /**
     * Starts the eventloop.
     */
    public void start() {
        if (!STATE.compareAndSet(Eventloop.this, NEW, RUNNING)) {
            throw new IllegalStateException("Can't start eventLoop, invalid state:" + state);
        }

        eventloopThread.start();
    }

    /**
     * Shuts down the Eventloop.
     *
     * This call can safely be made no matter the state of the Eventloop.
     *
     * This method is thread-safe.
     */
    public final void shutdown() {
        for (; ; ) {
            EventloopState oldState = state;
            switch (oldState) {
                case NEW:
                    if (STATE.compareAndSet(this, oldState, TERMINATED)) {
                        return;
                    }
                    break;
                case RUNNING:
                    if (STATE.compareAndSet(this, oldState, SHUTDOWN)) {
                        concurrentRunQueue.add(SHUTDOWN_TASK);
                        wakeup();
                        return;
                    }
                    break;
                default:
                    return;
            }
        }
    }

    /**
     * Awaits for the termination of the Eventloop.
     *
     * @param timeout the timeout
     * @param unit    the TimeUnit
     * @return true if the Eventloop is terminated.
     * @throws InterruptedException
     */
    public final boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        terminationLatch.await(timeout, unit);
        return state == TERMINATED;
    }

    protected abstract void wakeup();

    /**
     * Registers a resource on this Eventloop.
     *
     * Registered resources are automatically closed when the eventloop closes.
     * Some examples: AsyncSocket and AsyncServerSocket.
     *
     * If the Eventloop isn't in the running state, false is returned.
     *
     * This method is thread-safe.
     *
     * @param resource
     * @return true if the resource was successfully register, false otherwise.
     * @throws NullPointerException if resource is null.
     */
    public final boolean registerResource(Closeable resource) {
        checkNotNull(resource, "resource can't be null");

        if (state != RUNNING) {
            return false;
        }

        resources.add(resource);

        if (state != RUNNING) {
            resources.remove(resource);
            return false;
        }

        return true;
    }

    /**
     * Deregisters a resource.
     *
     * This method is thread-safe.
     *
     * This method can be called no matter the state of the Eventloop.
     *
     * @param resource the resource to deregister.
     */
    public final void deregisterResource(Closeable resource) {
        checkNotNull(resource, "resource can't be null");

        resources.remove(resource);
    }

    /**
     * Gets a collection of all registered resources.
     *
     * This method is thread-safe.
     *
     * @return the collection of all registered resources.
     */
    public final Collection<Closeable> resources() {
        return resources;
    }

    /**
     * Executes an EventloopTask on this Eventloop.
     *
     * @param task the task to execute.
     * @throws NullPointerException if task is null.
     */
    public final void execute(EventloopTask task) {
        if (Thread.currentThread() == eventloopThread) {
            localRunQueue.offer(task);
        } else {
            concurrentRunQueue.add(task);
            wakeup();
        }
    }

    /**
     * Executes a collection of frames on this Eventloop.
     *
     * @param requests the collection of requests.
     * @throws NullPointerException if requests is null or an request is null.
     */
    public final void execute(Collection<Frame> requests) {
        concurrentRunQueue.addAll(requests);
        wakeup();
    }

    /**
     * Executes a request on this eventloop.
     *
     * @param request the request to execute.
     * @throws NullPointerException if request is null.
     */
    public final void execute(Frame request) {
        concurrentRunQueue.add(request);
        wakeup();
    }

    protected final void runLocalTasks() {
        for (; ; ) {
            ScheduledTask scheduledTask = scheduledTaskQueue.peek();
            if (scheduledTask == null) {
                break;
            }

            if (scheduledTask.deadlineEpochNanos > epochNanos()) {
                // Task should not yet be executed.
                earliestDeadlineEpochNanos = scheduledTask.deadlineEpochNanos;
                break;
            }

            scheduledTaskQueue.poll();
            earliestDeadlineEpochNanos = -1;
            try {
                scheduledTask.run();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        for (; ; ) {
            EventloopTask task = localRunQueue.poll();
            if (task == null) {
                break;
            }

            try {
                task.run();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    protected final void runConcurrentTasks() {
        for (; ; ) {
            Object task = concurrentRunQueue.poll();
            if (task == null) {
                break;
            } else if (task instanceof EventloopTask) {
                try {
                    ((EventloopTask) task).run();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else if (task instanceof Frame) {
                scheduler.schedule((Frame) task);
            } else {
                throw new RuntimeException("Unrecognized type:" + task.getClass());
            }
        }
    }


    public static class AbstractConfiguration {
        private boolean spin;
        private Scheduler scheduler = new NopScheduler();
        private int localRunQueueCapacity = 1024;
        private int concurrentRunQueueCapacity = 4096;
        private String threadName;
        private ThreadAffinity threadAffinity;

        public void setThreadName(String threadName) {
            this.threadName = threadName;
        }

        public void setThreadAffinity(ThreadAffinity threadAffinity) {
            this.threadAffinity = threadAffinity;
        }

        public void setLocalRunQueueCapacity(int localRunQueueCapacity) {
            this.localRunQueueCapacity = checkPositive("localRunQueueCapacity", localRunQueueCapacity);
        }

        public void setConcurrentRunQueueCapacity(int concurrentRunQueueCapacity) {
            this.concurrentRunQueueCapacity = checkPositive("concurrentRunQueueCapacity", concurrentRunQueueCapacity);
        }

        public final void setSpin(boolean spin) {
            this.spin = spin;
        }

        public final void setScheduler(Scheduler scheduler) {
            this.scheduler = checkNotNull(scheduler);
        }
    }

    protected static final class ScheduledTask implements EventloopTask, Comparable<ScheduledTask> {

        private Future future;
        private long deadlineEpochNanos;

        @Override
        public void run() throws Exception {
            future.complete(null);
        }

        @Override
        public int compareTo(Eventloop.ScheduledTask that) {
            if (that.deadlineEpochNanos == this.deadlineEpochNanos) {
                return 0;
            }

            return this.deadlineEpochNanos > that.deadlineEpochNanos ? 1 : -1;
        }
    }

    /**
     * The {@link Thread} that executes the {@link Eventloop}.
     */
    public final class EventloopThread extends HazelcastManagedThread {

        private EventloopThread(AbstractConfiguration config) {
            if (config.threadName != null) {
                setName(config.threadName);
            }
            if (config.threadAffinity != null) {
                setThreadAffinity(config.threadAffinity);
            }
        }

        @Override
        public void executeRun() {
            try {
                beforeEventloop();
                eventLoop();
            } catch (Throwable e) {
                e.printStackTrace();
                logger.severe(e);
            } finally {
                state = TERMINATED;
                closeResources(resources);
                terminationLatch.countDown();
                System.out.println(getName() + " terminated");
            }
        }
    }

    /**
     * Exposes methods that should only be called from within the Eventloop.
     */
    public final class Unsafe {

        public <E> Future<E> newCompletedFuture(E value) {
            Future<E> future = Future.newReadyFuture(value);
            future.eventloop = Eventloop.this;
            return future;
        }

        public <E> Future<E> newFuture() {
            Future<E> future = Future.newFuture();
            future.eventloop = Eventloop.this;
            return future;
        }

        public void execute(EventloopTask task) {
            localRunQueue.offer(task);
        }

        public Future sleep(long delay, TimeUnit unit) {
            Future future = newFuture();
            ScheduledTask scheduledTask = new ScheduledTask();
            scheduledTask.future = future;
            scheduledTask.deadlineEpochNanos = epochNanos() + unit.toNanos(delay);
            scheduledTaskQueue.add(scheduledTask);
            return future;
        }

        public <I, O> Future<List<O>> map(List<I> input, List<O> output, Function<I, O> function) {
            Future future = newFuture();

            //todo: task can be pooled
            EventloopTask task = new EventloopTask() {
                Iterator<I> it = input.iterator();

                @Override
                public void run() {
                    if (it.hasNext()) {
                        I item = it.next();
                        O result = function.apply(item);
                        output.add(result);
                    }

                    if (it.hasNext()) {
                        unsafe.execute(this);
                    } else {
                        future.complete(output);
                    }
                }
            };

            execute(task);
            return future;
        }

        /**
         * Keeps calling the loop function until it returns false.
         *
         * @param loopFunction the function that is called in a loop.
         * @return the future that is completed as soon as the loop finishes.
         */
        public Future loop(Function<Eventloop, Boolean> loopFunction) {
            Future future = newFuture();

            //todo: task can be pooled
            EventloopTask task = new EventloopTask() {
                @Override
                public void run() {
                    if (loopFunction.apply(Eventloop.this)) {
                        unsafe.execute(this);
                    } else {
                        future.complete(null);
                    }
                }
            };
            execute(task);
            return future;
        }
    }
}
