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

package com.hazelcast.internal.tpc;


import com.hazelcast.internal.tpc.iobuffer.IOBuffer;
import com.hazelcast.internal.tpc.logging.TpcLogger;
import com.hazelcast.internal.tpc.logging.TpcLoggerLocator;
import com.hazelcast.internal.tpc.util.CircularQueue;
import com.hazelcast.internal.util.ThreadAffinity;
import com.hazelcast.internal.util.ThreadAffinityHelper;
import org.jctools.queues.MpmcArrayQueue;

import java.io.Closeable;
import java.util.BitSet;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Supplier;

import static com.hazelcast.internal.tpc.Eventloop.State.NEW;
import static com.hazelcast.internal.tpc.Eventloop.State.RUNNING;
import static com.hazelcast.internal.tpc.Eventloop.State.SHUTDOWN;
import static com.hazelcast.internal.tpc.Eventloop.State.TERMINATED;
import static com.hazelcast.internal.tpc.util.IOUtil.closeResources;
import static com.hazelcast.internal.tpc.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.tpc.util.Preconditions.checkPositive;
import static com.hazelcast.internal.tpc.util.Util.epochNanos;
import static java.lang.System.getProperty;
import static java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater;

/**
 * A EventLoop is a thread that is an event loop.
 * <p>
 * The Eventloop infrastructure is unaware of what is being send. So it isn't aware of requests/responses.
 * <p>
 * A single eventloop can deal with many server ports.
 */
@SuppressWarnings({"checkstyle:VisibilityModifier", "rawtypes"})
public abstract class Eventloop implements Executor {

    protected static final AtomicReferenceFieldUpdater<Eventloop, State> STATE
            = newUpdater(Eventloop.class, State.class, "state");

    private static final int INITIAL_ALLOCATOR_CAPACITY = 1 << 10;
    private static final Runnable SHUTDOWN_TASK = () -> {
    };

    /**
     * Allows for objects to be bound to this Eventloop. Useful for the lookup of services and other dependencies.
     */
    public final ConcurrentMap<?, ?> context = new ConcurrentHashMap<>();

    public final CircularQueue<Runnable> localRunQueue;

    protected final PriorityQueue<ScheduledTask> scheduledTaskQueue = new PriorityQueue<>();

    protected final TpcLogger logger = TpcLoggerLocator.getLogger(getClass());
    protected final Set<Closeable> closables = new CopyOnWriteArraySet<>();

    protected final AtomicBoolean wakeupNeeded = new AtomicBoolean(true);
    protected final MpmcArrayQueue concurrentRunQueue;

    protected final Scheduler scheduler;
    protected final boolean spin;

    protected Unsafe unsafe;
    protected final Thread eventloopThread;
    protected volatile State state = NEW;
    protected long earliestDeadlineEpochNanos = -1;

    TpcEngine engine;

    private final FutAllocator futAllocator;
    private final Type type;
    private final BitSet allowedCpus;
    private final CountDownLatch terminationLatch = new CountDownLatch(1);

    /**
     * Creates a new {@link Eventloop}.
     *
     * @param config the {@link Configuration} uses to create this {@link Eventloop}.
     * @throws NullPointerException if config is null.
     */
    public Eventloop(Configuration config, Type type) {
        this.type = checkNotNull(type);
        this.spin = config.spin;
        this.scheduler = config.schedulerSupplier.get();
        this.localRunQueue = new CircularQueue<>(config.localRunQueueCapacity);
        this.concurrentRunQueue = new MpmcArrayQueue(config.concurrentRunQueueCapacity);
        scheduler.init(this);
        this.eventloopThread = config.threadFactory.newThread(new EventloopTask());
        if (config.threadNameSupplier != null) {
            eventloopThread.setName(config.threadNameSupplier.get());
        }

        this.allowedCpus = config.threadAffinity == null ? null : config.threadAffinity.nextAllowedCpus();
        this.futAllocator = new FutAllocator(this, INITIAL_ALLOCATOR_CAPACITY);
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

    /**
     * Opens an AsyncServerSocket and ties that socket to the this Eventloop instance.
     * <p>
     * This method is thread-safe.
     *
     * @return the opened AsyncServerSocket.
     */
    public abstract AsyncServerSocket openAsyncServerSocket();

    /**
     * Opens an AsyncSocket.
     * <p/>
     * This AsyncSocket isn't tied to this Eventloop. After it is opened, it needs to be assigned to a particular
     * eventloop by calling {@link AsyncSocket#activate(Eventloop)}. The reason why this isn't done in 1 go, is
     * that it could be that the when the AsyncServerSocket accepts an AsyncSocket, it could be that it want to
     * assign that AsyncSocket to a different eventloop.
     * <p>
     * This method is thread-safe.
     *
     * @return the opened AsyncSocket.
     */
    public abstract AsyncSocket openAsyncSocket();

    /**
     * Creates the Eventloop specific Unsafe instance.
     *
     * @return the create Unsafe instance.
     */
    protected abstract Unsafe createUnsafe();

    /**
     * Is called before the {@link #eventLoop()} is called.
     * <p>
     * This method can be used to initialize resources.
     * <p>
     * Is called from the eventloop thread.
     */
    protected void beforeEventloop() throws Exception {
    }

    /**
     * Executes the actual eventloop.
     *
     * @throws Exception
     */
    protected abstract void eventLoop() throws Exception;

    /**
     * Is called after the {@link #eventLoop()} is called.
     * <p>
     * This method can be used to cleanup resources.
     * <p>
     * Is called from the eventloop thread.
     */
    protected void afterEventloop() throws Exception {
    }

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
     * Awaits for the termination of the Eventloop with the given timeout.
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
     * Wakes up the {@link Eventloop} when it is blocked and needs to be woken up.
     */
    protected abstract void wakeup();

    /**
     * Registers a clo on this Eventloop.
     * <p>
     * Registered resources are automatically closed when the eventloop closes.
     * Some examples: AsyncSocket and AsyncServerSocket.
     * <p>
     * If the Eventloop isn't in the running state, false is returned.
     * <p>
     * This method is thread-safe.
     *
     * @param closable
     * @return true if the clo was successfully register, false otherwise.
     * @throws NullPointerException if clo is null.
     */
    public final boolean registerClosable(Closeable closable) {
        checkNotNull(closable, "closable");

        if (state != RUNNING) {
            return false;
        }

        closables.add(closable);

        if (state != RUNNING) {
            closables.remove(closable);
            return false;
        }

        return true;
    }

    /**
     * Deregisters a closable from this Eventloop.
     * <p>
     * This method is thread-safe.
     * <p>
     * This method can be called no matter the state of the Eventloop.
     *
     * @param closable the closable to deregister.
     */
    public final void deregisterClosable(Closeable closable) {
        checkNotNull(closable, "resource");
        closables.remove(closable);
    }

    @Override
    public void execute(Runnable command) {
        if (!offer(command)) {
            throw new RejectedExecutionException("Task " + command.toString()
                    + " rejected from " + this);
        }
    }

    /**
     * Offers a task to be executed on this {@link Eventloop}.
     *
     * @param task the task to execute.
     * @return true if the task was accepted, false otherwise.
     * @throws NullPointerException if task is null.
     */
    public final boolean offer(Runnable task) {
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
     * Offers an {@link IOBuffer} to be processed by this {@link Eventloop}.
     *
     * @param buff the {@link IOBuffer} to process.
     * @return true if the buffer was accepted, false otherwise.
     * @throws NullPointerException if buff is null.
     */
    public final boolean offer(IOBuffer buff) {
        //todo: Don't want to add localRunQueue optimization like the offer(Runnable)?

        if (concurrentRunQueue.offer(buff)) {
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
                logger.warning(e);
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
                logger.warning(e);
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
                    logger.warning(e);
                }
            } else if (task instanceof IOBuffer) {
                scheduler.schedule((IOBuffer) task);
            } else {
                throw new RuntimeException("Unrecognized type:" + task.getClass());
            }
        }
    }

    /**
     * Contains the Configuration for {@link Eventloop} instances.
     * <p/>
     * The configuration should not be modified after it is used to create Eventloops.
     */
    public abstract static class Configuration {
        private static final int INITIAL_LOCAL_QUEUE_CAPACITY = 1 << 10;
        private static final int INITIAL_CONCURRENT_QUEUE_CAPACITY = 1 << 12;
        protected final Type type;
        private Supplier<Scheduler> schedulerSupplier = NopScheduler::new;
        private Supplier<String> threadNameSupplier;
        private ThreadAffinity threadAffinity;
        private ThreadFactory threadFactory = Thread::new;
        private boolean spin = Boolean.parseBoolean(getProperty("hazelcast.tpc.eventloop.spin", "false"));
        private int localRunQueueCapacity = INITIAL_LOCAL_QUEUE_CAPACITY;
        private int concurrentRunQueueCapacity = INITIAL_CONCURRENT_QUEUE_CAPACITY;

        protected Configuration(Type type) {
            this.type = type;
        }

        /**
         * Creates the Eventloop based on this Configuration.
         *
         * @return the created Eventloop.
         */
        public abstract Eventloop create();

        /**
         * Sets the ThreadFactory used to create the Thread that runs the {@link Eventloop}.
         *
         * @param threadFactory the ThreadFactory
         * @throws NullPointerException if threadFactory is null.
         */
        public void setThreadFactory(ThreadFactory threadFactory) {
            this.threadFactory = checkNotNull(threadFactory, "threadFactory");
        }

        /**
         * Sets the supplier for the thread name. If configured, the thread name is set
         * after the thread is created.
         * <p/>
         * If null, there is no thread name supplier and the default is used.
         *
         * @param threadNameSupplier the supplier for the thread name.
         */
        public void setThreadNameSupplier(Supplier<String> threadNameSupplier) {
            this.threadNameSupplier = threadNameSupplier;
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
            this.localRunQueueCapacity = checkPositive(localRunQueueCapacity, "localRunQueueCapacity");
        }

        public void setConcurrentRunQueueCapacity(int concurrentRunQueueCapacity) {
            this.concurrentRunQueueCapacity = checkPositive(concurrentRunQueueCapacity, "concurrentRunQueueCapacity");
        }

        public final void setSpin(boolean spin) {
            this.spin = spin;
        }

        public final void setSchedulerSupplier(Supplier<Scheduler> schedulerSupplier) {
            this.schedulerSupplier = checkNotNull(schedulerSupplier);
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
     * The {@link Runnable} containing the actual eventloop logic and is executed by by the eventloop {@link Thread}.
     */
    private final class EventloopTask implements Runnable {

        @Override
        public void run() {
            setAffinity();

            try {
                unsafe = createUnsafe();
                beforeEventloop();
                eventLoop();
            } catch (Throwable e) {
                logger.severe(e);
            } finally {
                state = TERMINATED;
                closeResources(closables);
                terminationLatch.countDown();
                if (engine != null) {
                    engine.notifyEventloopTerminated();
                }
                if (logger.isInfoEnabled()) {
                    logger.info(Thread.currentThread().getName() + " terminated");
                }
            }
        }

        private void setAffinity() {
            if (allowedCpus != null) {
                ThreadAffinityHelper.setAffinity(allowedCpus);
                BitSet actualCpus = ThreadAffinityHelper.getAffinity();
                if (!actualCpus.equals(allowedCpus)) {
                    logger.warning(Thread.currentThread().getName() + " affinity was not applied successfully. "
                            + "Expected CPUs:" + allowedCpus + ". Actual CPUs:" + actualCpus);
                } else {
                    if (logger.isFineEnabled()) {
                        logger.fine(Thread.currentThread().getName() + " has affinity for CPUs:" + allowedCpus);
                    }
                }
            }
        }
    }

    /**
     * Exposes methods that should only be called from within the {@link Eventloop}.
     */
    public abstract class Unsafe {

        /**
         * Returns the {@link Eventloop} that belongs to this {@link Unsafe} instance.
         *
         * @return the Eventloop.
         */
        public Eventloop eventloop() {
            return Eventloop.this;
        }

        public <E> Fut<E> newFut() {
            return futAllocator.allocate();
        }

        /**
         * Offers a task to be scheduled on the eventloop.
         *
         * @param task the task to schedule.
         * @return true if the task was successfully offered, false otherwise.
         */
        public boolean offer(Runnable task) {
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

        void init(Eventloop eventloop);

        boolean tick();

        void schedule(IOBuffer task);
    }

    /**
     * The Type of {@link Eventloop}.
     */
    public enum Type {

        NIO, IOURING;

        public static Type fromString(String type) {
            String typeLowerCase = type.toLowerCase();
            if (typeLowerCase.equals("io_uring") || typeLowerCase.equals("iouring")) {
                return IOURING;
            } else if (typeLowerCase.equals("nio")) {
                return NIO;
            } else {
                throw new IllegalArgumentException("Unrecognized eventloop type [" + type + ']');
            }
        }
    }
}
