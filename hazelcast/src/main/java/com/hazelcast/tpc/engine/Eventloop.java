package com.hazelcast.tpc.engine;


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
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;

import static com.hazelcast.internal.nio.IOUtil.closeResources;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.tpc.engine.EventloopState.*;
import static com.hazelcast.tpc.engine.Util.epochNanos;
import static java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater;

/**
 * A EventLoop is a thread that is an event loop.
 *
 * The Eventloop infrastructure is unaware of what is being send. So it isn't aware of requests/responses.
 *
 * A single eventloop can deal with many server ports.
 */
public abstract class Eventloop extends HazelcastManagedThread {

    protected final static AtomicReferenceFieldUpdater<Eventloop, EventloopState> STATE
            = newUpdater(Eventloop.class, EventloopState.class, "state");

    protected final ILogger logger = Logger.getLogger(getClass());
    protected final Set<Closeable> resources = new CopyOnWriteArraySet<>();

    public final AtomicBoolean wakeupNeeded = new AtomicBoolean(true);
    public final MpmcArrayQueue concurrentRunQueue = new MpmcArrayQueue(4096);

    protected Scheduler scheduler = new NopScheduler();
    public final CircularQueue<EventloopTask> localRunQueue = new CircularQueue<>(1024);
    protected boolean spin;

    PriorityQueue<ScheduledTask> scheduledTaskQueue = new PriorityQueue();

    protected volatile EventloopState state = CREATED;

    private final CountDownLatch terminationLatch = new CountDownLatch(1);

    public final Unsafe unsafe = new Unsafe();

    /**
     * Returns the state of the Eventloop.
     *
     * This method is thread-safe.
     *
     * @return the state.
     */
    public EventloopState state() {
        return state;
    }

    public boolean isSpin() {
        return spin;
    }

    public void setSpin(boolean spin) {
        this.spin = spin;
    }

    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
        scheduler.setEventloop(this);
    }

    public Scheduler getScheduler() {
        return scheduler;
    }

    /**
     * Shuts down the Eventloop.
     *
     * This call can safely be made no matter the state of the Eventloop.
     *
     * This method is thread-safe.
     */
    public void shutdown() {
        for (; ; ) {
            EventloopState oldState = state;
            switch (oldState) {
                case CREATED:
                    if (STATE.compareAndSet(this, oldState, TERMINATED)) {
                        return;
                    }
                    break;
                case RUNNING:
                    if (STATE.compareAndSet(this, oldState, SHUTDOWN)) {
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
     * @param unit the TimeUnit
     * @return true if the Eventloop is terminated.
     * @throws InterruptedException
     */
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException{
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
    public Collection<Closeable> resources() {
        return resources;
    }

    protected abstract void eventLoop() throws Exception;

    public void execute(EventloopTask task) {
        if (Thread.currentThread() == this) {
            localRunQueue.offer(task);
        } else {
            concurrentRunQueue.add(task);
            wakeup();
        }
    }

    public void execute(Collection<Frame> requests) {
        concurrentRunQueue.addAll(requests);
        wakeup();
    }

    public void execute(Frame request) {
        concurrentRunQueue.add(request);
        wakeup();
    }

    @Override
    public final void executeRun() {
        try {
            if (!STATE.compareAndSet(this, CREATED, RUNNING)) {
                throw new IllegalStateException("Can't start eventLoop, invalid state:" + state);
            }

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

    protected long earliestDeadlineEpochNanos = -1;

    protected void runLocalTasks() {
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

    protected void runConcurrentTasks() {
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

    protected static class ScheduledTask implements EventloopTask, Comparable<ScheduledTask> {

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

    public class Unsafe {

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
