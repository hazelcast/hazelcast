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
import com.hazelcast.internal.util.ThreadAffinityHelper;
import com.hazelcast.internal.util.executor.HazelcastManagedThread;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.tpc.engine.iobuffer.IOBuffer;
import com.hazelcast.tpc.util.CircularQueue;
import org.jctools.queues.MpmcArrayQueue;

import java.io.Closeable;
import java.util.BitSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;

import static com.hazelcast.internal.nio.IOUtil.closeResources;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static com.hazelcast.tpc.engine.Eventloop.State.*;
import static com.hazelcast.tpc.util.Util.epochNanos;
import static java.lang.System.getProperty;
import static java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater;

/**
 * A EventLoop is a thread that is an event loop.
 * <p>
 * The Eventloop infrastructure is unaware of what is being send. So it isn't aware of requests/responses.
 * <p>
 * A single eventloop can deal with many server ports.
 */
public abstract class Eventloop {

    private static final Runnable SHUTDOWN_TASK = () -> {
    };

    protected final static AtomicReferenceFieldUpdater<Eventloop, State> STATE
            = newUpdater(Eventloop.class, State.class, "state");

    protected final ILogger logger = Logger.getLogger(getClass());
    protected final Set<Closeable> resources = new CopyOnWriteArraySet<>();

    protected final AtomicBoolean wakeupNeeded = new AtomicBoolean(true);
    protected final MpmcArrayQueue concurrentRunQueue;

    public final ConcurrentMap context = new ConcurrentHashMap();

    protected final Scheduler scheduler;
    public final CircularQueue<Runnable> localRunQueue;
    protected final boolean spin;
    private final Type type;
    private final BitSet allowedCpus;

    PriorityQueue<ScheduledTask> scheduledTaskQueue = new PriorityQueue();

    protected volatile State state = NEW;

    private final CountDownLatch terminationLatch = new CountDownLatch(1);

    private final FutAllocator futAllocator;

    protected Unsafe unsafe;

    protected long earliestDeadlineEpochNanos = -1;

    protected final Thread eventloopThread;

    Engine engine;

    /**
     * Creates a new {@link Eventloop}.
     *
     * @param config the {@link Configuration} uses to create this {@link Eventloop}.
     * @throws NullPointerException if config is null.
     */
    public Eventloop(Configuration config, Type type) {
        this.type = checkNotNull(type);
        this.spin = config.spin;
        this.scheduler = config.scheduler;
        this.localRunQueue = new CircularQueue<>(config.localRunQueueCapacity);
        this.concurrentRunQueue = new MpmcArrayQueue(config.concurrentRunQueueCapacity);
        scheduler.eventloop(this);
        this.eventloopThread = config.threadFactory.newThread(new EventloopThreadTask());
        if (config.threadName != null) {
            eventloopThread.setName(config.threadName);
        }

        this.allowedCpus = config.threadAffinity == null ? null : config.threadAffinity.nextAllowedCpus();
        this.futAllocator = new FutAllocator(this, 1024);
    }

    /**
     * Returns the {@link Type} of this {@link Eventloop}.
     * <p>
     * This method is thread-safe.
     *
     * @return the {@link Type} of this {@link Eventloop}. Value will never be null.
     */
    public final Type type() {
        return type;
    }

    /**
     * Gets the Unsafe instance for this Eventloop.
     *
     * @return the Unsafe instance.
     */
    public final Unsafe unsafe() {
        return unsafe;
    }

    /**
     * Returns the {Scheduler} for this {@link Eventloop}.
     * <p>
     * This method is thread-safe.
     *
     * @return the {@link Scheduler}.
     */
    public Scheduler scheduler() {
        return scheduler;
    }

    /**
     * Returns the {@link Thread} that runs this {@link Eventloop}.
     * <p>
     * This method is thread-safe.
     *
     * @return the EventloopThread.
     */
    public Thread eventloopThread() {
        return eventloopThread;
    }

    /**
     * Returns the state of the Eventloop.
     * <p>
     * This method is thread-safe.
     *
     * @return the state.
     */
    public final State state() {
        return state;
    }

    protected abstract Unsafe createUnsafe();

    /**
     * Is called before the {@link #eventLoop()} is called.
     * <p>
     * This method can be used to initialize resources.
     * <p>
     * Is called from the eventloop thread.
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
     *
     * @throws IllegalStateException if the Eventloop isn't in NEW state.
     */
    public void start() {
        if (!STATE.compareAndSet(Eventloop.this, NEW, RUNNING)) {
            throw new IllegalStateException("Can't start eventLoop, invalid state:" + state);
        }

        eventloopThread.start();
    }

    /**
     * Shuts down the Eventloop.
     * <p>
     * This call can safely be made no matter the state of the Eventloop.
     * <p>
     * This method is thread-safe.
     */
    public final void shutdown() {
        for (; ; ) {
            State oldState = state;
            switch (oldState) {
                case NEW:
                    if (STATE.compareAndSet(this, oldState, TERMINATED)) {
                        terminationLatch.countDown();
                        if (engine != null) {
                            engine.notifyEventloopTerminated();
                        }
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

    /**
     * Wakes up the {@link Eventloop} when it is blocked an needs to be woken up.
     */
    protected abstract void wakeup();

    /**
     * Registers a resource on this Eventloop.
     * <p>
     * Registered resources are automatically closed when the eventloop closes.
     * Some examples: AsyncSocket and AsyncServerSocket.
     * <p>
     * If the Eventloop isn't in the running state, false is returned.
     * <p>
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
     * <p>
     * This method is thread-safe.
     * <p>
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
     * <p>
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
     * @return true if the task was accepted, false otherwise.
     * @throws NullPointerException if task is null.
     */
    public final boolean execute(Runnable task) {
        if (Thread.currentThread() == eventloopThread) {
            return localRunQueue.offer(task);
        } else if (concurrentRunQueue.offer(task)) {
            wakeup();
            return true;
        } else {
            return false;
        }
    }

    /**
     * Executes a request on this eventloop.
     *
     * @param request the request to execute.
     * @return true if the task was accepted, false otherwise.
     * @throws NullPointerException if request is null.
     */
    public final boolean execute(IOBuffer request) {
        if (concurrentRunQueue.offer(request)) {
            wakeup();
            return true;
        } else {
            return false;
        }
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
            Runnable task = localRunQueue.poll();
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
            } else if (task instanceof Runnable) {
                try {
                    ((Runnable) task).run();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else if (task instanceof IOBuffer) {
                scheduler.schedule((IOBuffer) task);
            } else {
                throw new RuntimeException("Unrecognized type:" + task.getClass());
            }
        }
    }

    /**
     * Contains the Configuration for the {@link Eventloop}.
     */
    public static class Configuration {
        private boolean spin = Boolean.parseBoolean(getProperty("reactor.spin", "false"));
        private Scheduler scheduler = new NopScheduler();
        private int localRunQueueCapacity = 1024;
        private int concurrentRunQueueCapacity = 4096;
        private String threadName;
        private ThreadAffinity threadAffinity;
        private ThreadFactory threadFactory = HazelcastManagedThread::new;

        public void setThreadFactory(ThreadFactory threadFactory) {
            this.threadFactory = checkNotNull(threadFactory, "threadFactory");
        }

        public void setThreadName(String threadName) {
            this.threadName = threadName;
        }

        /**
         * Sets the {@link ThreadAffinity}. If the threadAffinity is null, no thread affinity
         * is applied.
         *
         * @param threadAffinity the ThreadAffinity.
         */
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

    protected static final class ScheduledTask implements Runnable, Comparable<ScheduledTask> {

        private Fut fut;
        private long deadlineEpochNanos;

        @Override
        public void run() {
            fut.complete(null);
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
     * The {@link Runnable} executed by the eventloop {@link Thread}.
     */
    private final class EventloopThreadTask implements Runnable {

        @Override
        public void run() {
            if (allowedCpus != null) {
                ThreadAffinityHelper.setAffinity(allowedCpus);
                BitSet actualCpus = ThreadAffinityHelper.getAffinity();
                if (!actualCpus.equals(allowedCpus)) {
                    logger.warning(Thread.currentThread().getName() + " affinity was not applied successfully. "
                            + "Expected CPUs:" + allowedCpus + ". Actual CPUs:" + actualCpus);
                } else {
                    logger.info(Thread.currentThread().getName() + " has affinity for CPUs:" + allowedCpus);
                }
            }

            try {
                unsafe = createUnsafe();
                beforeEventloop();
                eventLoop();
            } catch (Throwable e) {
                e.printStackTrace();
                logger.severe(e);
            } finally {
                state = TERMINATED;
                closeResources(resources);
                terminationLatch.countDown();
                if (engine != null) {
                    engine.notifyEventloopTerminated();
                }
                if (logger.isInfoEnabled()) {
                    logger.info(Thread.currentThread().getName() + " terminated");
                }
            }
        }
    }

    /**
     * Exposes methods that should only be called from within the Eventloop.
     */
    public abstract class Unsafe {

        public Eventloop eventloop() {
            return Eventloop.this;
        }

        public abstract AsyncFile newAsyncFile(String path);

        public <E> Fut<E> newCompletedFuture(E value) {
            Fut<E> fut = futAllocator.allocate();
            fut.complete(value);
            return fut;
        }

        public <E> Fut<E> newFut() {
            return futAllocator.allocate();
        }

        public boolean execute(Runnable task) {
            return localRunQueue.offer(task);
        }

        public Fut sleep(long delay, TimeUnit unit) {
            Fut fut = newFut();
            ScheduledTask scheduledTask = new ScheduledTask();
            scheduledTask.fut = fut;
            scheduledTask.deadlineEpochNanos = epochNanos() + unit.toNanos(delay);
            scheduledTaskQueue.add(scheduledTask);
            return fut;
        }

        public <I, O> Fut<List<O>> map(List<I> input, List<O> output, Function<I, O> function) {
            Fut fut = newFut();

            //todo: task can be pooled
            Runnable task = new Runnable() {
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
                        fut.complete(output);
                    }
                }
            };

            execute(task);
            return fut;
        }

        /**
         * Keeps calling the loop function until it returns false.
         *
         * @param loopFunction the function that is called in a loop.
         * @return the future that is completed as soon as the loop finishes.
         */
        public Fut loop(Function<Eventloop, Boolean> loopFunction) {
            Fut fut = newFut();

            //todo: task can be pooled
            Runnable task = new Runnable() {
                @Override
                public void run() {
                    if (loopFunction.apply(Eventloop.this)) {
                        unsafe.execute(this);
                    } else {
                        fut.complete(null);
                    }
                }
            };
            execute(task);
            return fut;
        }
    }

    /**
     * The state of the {@link Eventloop}.
     */
    public enum State {
        NEW,
        RUNNING,
        SHUTDOWN,
        TERMINATED
    }


    public interface Scheduler {

        void eventloop(Eventloop eventloop);

        boolean tick();

        void schedule(IOBuffer task);
    }

    /**
     * The Type of {@link Eventloop}.
     */
    public enum Type {

        NIO, EPOLL, IOURING;

        public static Type fromString(String type) {
            String typeLowerCase = type.toLowerCase();
            if (typeLowerCase.equals("io_uring") || typeLowerCase.equals("iouring")) {
                return IOURING;
            } else if (typeLowerCase.equals("nio")) {
                return NIO;
            } else if (typeLowerCase.equals("epoll")) {
                return EPOLL;
            } else {
                throw new RuntimeException("Unrecognized eventloop type [" + type + ']');
            }
        }
    }
}
