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

package com.hazelcast.internal.tpc;


import com.hazelcast.internal.tpc.logging.TpcLogger;
import com.hazelcast.internal.tpc.logging.TpcLoggerLocator;
import com.hazelcast.internal.tpc.util.CircularQueue;
import com.hazelcast.internal.util.ThreadAffinity;
import com.hazelcast.internal.util.ThreadAffinityHelper;
import org.jctools.queues.MpmcArrayQueue;

import java.util.BitSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static com.hazelcast.internal.tpc.Reactor.State.NEW;
import static com.hazelcast.internal.tpc.Reactor.State.RUNNING;
import static com.hazelcast.internal.tpc.Reactor.State.SHUTDOWN;
import static com.hazelcast.internal.tpc.Reactor.State.TERMINATED;
import static java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater;

/**
 * A Reactor is an implementation of the reactor design pattern. So it listen to some
 * event sources and then dispatches the events to the appropriate handler. This is coordinated
 * from the eventloop that is inside each reactor.
 * <p/>
 * There are various forms of events:
 * <ol>
 *     <li>Concurrency tasks: tasks that are offered by other threads</li>
 *     <li>Local tasks: tasks that have been generated by the Reactor itself</li>
 *     <li>Scheduled tasks: tasks that have been scheduled by the Reactor</li>
 *     <li>Tasks from some asynchronous eventing system that interacts with I/O. </li>>
 * </ol>
 */
@SuppressWarnings({"checkstyle:DeclarationOrder", "checkstyle:VisibilityModifier", "rawtypes"})
public abstract class Reactor implements Executor {

    protected static final AtomicReferenceFieldUpdater<Reactor, State> STATE
            = newUpdater(Reactor.class, State.class, "state");

    protected final ConcurrentMap<?, ?> context = new ConcurrentHashMap<>();
    protected final TpcLogger logger = TpcLoggerLocator.getLogger(getClass());
    protected final MpmcArrayQueue externalTaskQueue;
    protected final Eventloop eventloop;
    protected final CircularQueue localTaskQueue;
    protected final boolean spin;
    protected final Thread eventloopThread;
    protected final String name;
    protected final AtomicBoolean wakeupNeeded;
    private final TpcEngine engine;
    private final ReactorType type;
    private final CountDownLatch terminationLatch = new CountDownLatch(1);
    private final CountDownLatch startLatch = new CountDownLatch(1);
    private final Scheduler scheduler;
    protected volatile State state = NEW;

    /**
     * Creates a new {@link Reactor}.
     *
     * @param builder the {@link ReactorBuilder} uses to create this {@link Reactor}.
     * @throws NullPointerException if builder is null.
     */
    protected Reactor(ReactorBuilder builder) {
        this.type = builder.type;
        this.spin = builder.spin;
        this.engine = builder.engine;
        CompletableFuture<Eventloop> eventloopFuture = new CompletableFuture<>();
        this.eventloopThread = builder.threadFactory.newThread(new StartEventloopTask(eventloopFuture, builder));

        if (builder.threadNameSupplier != null) {
            eventloopThread.setName(builder.threadNameSupplier.get());
        }
        this.name = builder.reactorNameSupplier.get();

        // The eventloopThread is started so eventloop gets created on the eventloop thread.
        // but the actual processing of the eventloop is only done after start() is called.
        eventloopThread.start();

        // wait for the eventloop to be created.
        eventloop = eventloopFuture.join();
        // There is a happens-before edge between writing to the eventloopFuture and
        // the join. So at this point we can safely read the fields that have been
        // set in the constructor of the eventloop.
        this.externalTaskQueue = eventloop.externalTaskQueue;
        this.localTaskQueue = eventloop.localTaskQueue;
        this.wakeupNeeded = eventloop.wakeupNeeded;
        this.scheduler = eventloop.scheduler;
    }

    /**
     * Allows for objects to be bound to this Reactor. Useful for the lookup
     * of services and other dependencies.
     */
    public final ConcurrentMap<?, ?> context() {
        return context;
    }

    /**
     * Gets the name of this reactor. Useful for debugging purposes.
     *
     * @return the name.
     */
    public final String name() {
        return name;
    }

    /**
     * Returns the {@link ReactorType} of this {@link Reactor}.
     * <p>
     * This method is thread-safe.
     *
     * @return the {@link ReactorType} of this {@link Reactor}. Value will never be null.
     */
    public final ReactorType type() {
        return type;
    }

    /**
     * Returns the scheduler for this Reactor.
     *
     * @return the scheduler for this reactor.
     */
    public final Scheduler scheduler() {
        return scheduler;
    }

    /**
     * Returns the Eventloop for this {@link Reactor}.
     * <p>
     * This method is thread-safe. But Eventloop should only be touched
     * by the Eventloop thread because the Eventloop is not thread-safe.
     *
     * @return the {@link Eventloop}.
     */
    public final Eventloop eventloop() {
        return eventloop;
    }

    /**
     * Returns the {@link Thread} that runs the eventloop. The eventloop thread is created
     * when the Reactor is created.
     * <p>
     * This method is thread-safe.
     *
     * @return the thread running the eventloop.
     */
    public final Thread eventloopThread() {
        return eventloopThread;
    }

    /**
     * Returns the state of the Reactor.
     * <p>
     * This method is thread-safe.
     *
     * @return the state.
     */
    public final State state() {
        return state;
    }

    /**
     * Creates the Eventloop run by this Reactor. Will be called from the eventloop-thread.
     *
     * @return the created Eventloop instance.
     */
    protected abstract Eventloop newEventloop(ReactorBuilder builder);

    /**
     * Creates a new {@link AsyncServerSocketBuilder}.
     *
     * @return the created AsyncSocketBuilder.
     * @throws IllegalStateException if the reactor isn't running.
     */
    public abstract AsyncSocketBuilder newAsyncSocketBuilder();

    /**
     * Creates a new {@link AsyncServerSocketBuilder} for the given acceptRequest.
     *
     * @param acceptRequest a wrapper around a lower level socket implemented that needs
     *                      to be accepted.
     * @return the created AsyncSocketBuilder.
     * @throws NullPointerException if acceptRequest is null.
     * @throws IllegalStateException if the reactor isn't running.
     */
    public abstract AsyncSocketBuilder newAsyncSocketBuilder(AcceptRequest acceptRequest);

    /**
     * Creates a new AsyncServerSocketBuilder.
     *
     * @return the created AsyncServerSocketBuilder.
     * @throws IllegalStateException if the reactor isn't running.
     */
    public abstract AsyncServerSocketBuilder newAsyncServerSocketBuilder();

    protected void verifyRunning() {
        State state = this.state;
        if (RUNNING != state) {
            throw new IllegalStateException("Reactor not in RUNNING state, but " + state);
        }
    }

    /**
     * Starts the reactor.
     *
     * @throws IllegalStateException if the reactor isn't in NEW state.
     */
    public Reactor start() {
        if (!STATE.compareAndSet(Reactor.this, NEW, RUNNING)) {
            throw new IllegalStateException("Can't start reactor, invalid state:" + state);
        }
        startLatch.countDown();
        return this;
    }

    /**
     * Shuts down the Reactor.
     * <p/>
     * This call doesn't wait for the Reactor to shut down. The
     * {@link #awaitTermination(long, TimeUnit)} should be used for that.
     * <p>
     * This call can safely be made no matter the state of the Reactor.
     * <p>
     * This method is thread-safe.
     */
    public final void shutdown() {
        for (; ; ) {
            State oldState = state;
            switch (oldState) {
                case NEW:
                    if (STATE.compareAndSet(this, oldState, TERMINATED)) {
                        // the eventloop thread is waiting on the startLatch, so we need to
                        // wake it up. It will then check the status and terminate if needed.
                        startLatch.countDown();
                        return;
                    }

                    break;
                case RUNNING:
                    if (STATE.compareAndSet(this, oldState, SHUTDOWN)) {
                        execute(() -> eventloop.stop = true);
                        return;
                    }
                    break;
                default:
                    return;
            }
        }
    }

    /**
     * Awaits for the termination of the Reactor with the given timeout.
     *
     * @param timeout the timeout
     * @param unit    the TimeUnit
     * @return true if the Reactor is terminated.
     * @throws InterruptedException if the thread was interrupted while waiting.
     */
    public final boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        terminationLatch.await(timeout, unit);
        return state == TERMINATED;
    }

    /**
     * Wakes up the {@link Reactor} when it is blocked and needs to be woken up.
     */
    public abstract void wakeup();

    @Override
    public void execute(Runnable command) {
        if (!offer(command)) {
            throw new RejectedExecutionException("Task " + command.toString()
                    + " rejected from " + this);
        }
    }

    /**
     * Offers a task to be executed on this {@link Reactor}.
     *
     * @param task the task to execute.
     * @return true if the task was accepted, false otherwise.
     * @throws NullPointerException if task is null.
     */
    public final boolean offer(Runnable task) {
        return offer((Object) task);
    }

    /**
     * Offers a task to be executed on this {@link Reactor}.
     *
     * @param task the task to execute.
     * @return true if the task was accepted, false otherwise.
     * @throws NullPointerException if task is null.
     */
    public final boolean offer(Object task) {
        if (Thread.currentThread() == eventloopThread) {
            return localTaskQueue.offer(task);
        } else if (externalTaskQueue.offer(task)) {
            wakeup();
            return true;
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return name;
    }

    /**
     * The state of the {@link Reactor}.
     */
    public enum State {
        NEW,
        RUNNING,
        SHUTDOWN,
        TERMINATED
    }

    /**
     * The EventloopTask does a few important things:
     * <ol>
     *     <li>Configure the thread affinity</li>
     *     <li>Create the eventloop</li>
     *     <li>Run the eventloop</li>
     *     <li>Manage the lifecycle of the reactor when it terminates.</li>
     * </ol>
     */
    private final class StartEventloopTask implements Runnable {
        private final CompletableFuture<Eventloop> future;
        private final ReactorBuilder builder;

        private StartEventloopTask(CompletableFuture<Eventloop> future, ReactorBuilder builder) {
            this.future = future;
            this.builder = builder;
        }

        @Override
        public void run() {
            try {
                try {
                    configureThreadAffinity();
                    Eventloop eventloop = newEventloop(builder);
                    future.complete(eventloop);

                    startLatch.await();
                    try {
                        // it could be that the thread wakes up due to termination. So we need
                        // to check the state first before running.
                        if (state == RUNNING) {
                            eventloop.run();
                        }
                    } finally {
                        eventloop.destroy();
                    }
                } catch (Throwable e) {
                    future.completeExceptionally(e);
                    logger.severe(e);
                } finally {
                    state = TERMINATED;

                    terminationLatch.countDown();

                    if (engine != null) {
                        engine.notifyReactorTerminated();
                    }

                    if (logger.isInfoEnabled()) {
                        logger.info(Thread.currentThread().getName() + " terminated");
                    }
                }
            } catch (Throwable e) {
                // log whatever wasn't caught so that we don't swallow throwables.
                logger.severe(e);
            }
        }

        private void configureThreadAffinity() {
            ThreadAffinity threadAffinity = builder.threadAffinity;
            BitSet allowedCpus = threadAffinity == null ? null : threadAffinity.nextAllowedCpus();
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
}
