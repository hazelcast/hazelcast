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


import com.hazelcast.internal.tpc.iobuffer.IOBuffer;
import com.hazelcast.internal.tpc.logging.TpcLogger;
import com.hazelcast.internal.tpc.logging.TpcLoggerLocator;
import com.hazelcast.internal.tpc.util.CircularQueue;
import org.jctools.queues.MpmcArrayQueue;

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
 * A Reactor is a loop that processes events.
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

    /**
     * Allows for objects to be bound to this Reactor. Useful for the lookup of services
     * and other dependencies.
     */
    public final ConcurrentMap<?, ?> context = new ConcurrentHashMap<>();

    protected final TpcLogger logger = TpcLoggerLocator.getLogger(getClass());
    protected final AtomicBoolean wakeupNeeded;
    protected final MpmcArrayQueue concurrentTaskQueue;
    protected final Eventloop eventloop;
    public final CircularQueue<Runnable> localTaskQueue;
    protected final boolean spin;
    protected final Thread eventloopThread;
    protected volatile State state = NEW;

    TpcEngine engine;

    private final ReactorType type;
    final CountDownLatch terminationLatch = new CountDownLatch(1);


    /**
     * Creates a new {@link Reactor}.
     *
     * @param builder the {@link ReactorBuilder} uses to create this {@link Reactor}.
     * @throws NullPointerException if builder is null.
     */
    protected Reactor(ReactorBuilder builder) {
        this.type = builder.type;
        this.eventloop = createEventloop(builder);
        this.wakeupNeeded = eventloop.wakeupNeeded;
        this.spin = eventloop.spin;
        this.concurrentTaskQueue = eventloop.concurrentTaskQueue;
        this.localTaskQueue = eventloop.localTaskQueue;
        this.eventloopThread = builder.threadFactory.newThread(eventloop);
        if (builder.threadNameSupplier != null) {
            eventloopThread.setName(builder.threadNameSupplier.get());
        }
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
     * Returns the {@link Thread} that runs the eventloop.
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
     * Opens an TCP/IP (stream) async server socket and ties that socket to the this Reactor
     * instance. The returned socket assumes IPv4. When support for IPv6 is added, a boolean
     * 'ipv4' flag needs to be added.
     * <p>
     * This method is thread-safe.
     *
     * @return the opened AsyncServerSocket.
     */
    public abstract AsyncServerSocket openTcpAsyncServerSocket();

    /**
     * Opens TCP/IP (stream) based async socket. The returned socket assumes IPv4. When support for
     * IPv6 is added, a boolean 'ipv4' flag needs to be added.
     * <p/>
     * The opened AsyncSocket isn't tied to this Reactor. After it is opened, it needs to be assigned
     * to a particular reactor by calling {@link AsyncSocket#activate(Reactor)}. The reason why
     * this isn't done in 1 go, is that it could be that when the AsyncServerSocket accepts an
     * AsyncSocket, we want to assign that AsyncSocket to a different Reactor. Otherwise if there
     * would be 1 AsyncServerSocket, connected AsyncSockets can only run on top of the reactor of
     * the AsyncServerSocket instead of being distributed over multiple reactors.
     * <p>
     * This method is thread-safe.
     *
     * @return the opened AsyncSocket.
     */
    public abstract AsyncSocket openTcpAsyncSocket();

    /**
     * Creates the Eventloop run by this Reactor.
     *
     * @return the created Eventloop instance.
     */
    protected abstract Eventloop createEventloop(ReactorBuilder builder);

    /**
     * Starts the reactor.
     *
     * @throws IllegalStateException if the reactor isn't in NEW state.
     */
    public void start() {
        if (!STATE.compareAndSet(Reactor.this, NEW, RUNNING)) {
            throw new IllegalStateException("Can't start reactor, invalid state:" + state);
        }

        eventloopThread.start();
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
                        terminationLatch.countDown();
                        if (engine != null) {
                            engine.notifyReactorTerminated();
                        }
                        return;
                    }

                    break;
                case RUNNING:
                    if (STATE.compareAndSet(this, oldState, SHUTDOWN)) {
                        concurrentTaskQueue.add((Runnable) () -> eventloop.stop = true);
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
    protected abstract void wakeup();

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
        if (Thread.currentThread() == eventloopThread) {
            return localTaskQueue.offer(task);
        } else if (concurrentTaskQueue.offer(task)) {
            wakeup();
            return true;
        } else {
            return false;
        }
    }

    /**
     * Offers an {@link IOBuffer} to be processed by this {@link Reactor}.
     *
     * @param buff the {@link IOBuffer} to process.
     * @return true if the buffer was accepted, false otherwise.
     * @throws NullPointerException if buff is null.
     */
    public final boolean offer(IOBuffer buff) {
        //todo: Don't want to add localRunQueue optimization like the offer(Runnable)?

        if (concurrentTaskQueue.offer(buff)) {
            wakeup();
            return true;
        } else {
            return false;
        }
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
}
