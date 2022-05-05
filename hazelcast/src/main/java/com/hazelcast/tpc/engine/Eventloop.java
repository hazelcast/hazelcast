package com.hazelcast.tpc.engine;


import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.internal.util.executor.HazelcastManagedThread;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.tpc.engine.frame.Frame;
import org.jctools.queues.MpmcArrayQueue;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.tpc.engine.EventloopState.*;

/**
 * A EventLoop is a thread that is an event loop.
 *
 * The Eventloop infrastructure is unaware of what is being send. So it isn't aware of requests/responses.
 *
 * A single eventloop can deal with many server ports.
 */
public abstract class Eventloop extends HazelcastManagedThread {
    protected final ILogger logger = Logger.getLogger(getClass());
    protected final Set<AsyncSocket> registeredSockets = new CopyOnWriteArraySet<>();
    protected final Set<AsyncServerSocket> registeredServerSockets = new CopyOnWriteArraySet<>();

    public final AtomicBoolean wakeupNeeded = new AtomicBoolean(true);
    public final MpmcArrayQueue concurrentRunQueue = new MpmcArrayQueue(4096);

    protected Scheduler scheduler;
    public final CircularQueue<EventloopTask> localRunQueue = new CircularQueue<>(1024);
    protected boolean spin;

    protected final AtomicReference<EventloopState> state = new AtomicReference<>(CREATED);

    public EventloopState state() {
        return state.get();
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

    public void shutdown() {
        for (; ; ) {
            EventloopState oldState = state.get();
            switch (oldState) {
                case CREATED:
                    if (state.compareAndSet(oldState, SHUTDOWN)) {
                        return;
                    }
                    break;
                case RUNNING:
                    if (state.compareAndSet(oldState, SHUTTING_DOWN)) {
                        wakeup();
                        return;
                    }
                    break;
                default:
                    return;
            }
        }
    }

    protected abstract void wakeup();

    public boolean registerSocket(AsyncSocket socket) {
        if (state.get() != RUNNING) {
            return false;
        }

        registeredSockets.add(socket);

        if (state.get() != RUNNING) {
            registeredSockets.remove(socket);
            return false;
        }

        return true;
    }

    public void deregisterSocket(AsyncSocket socket) {
        registeredSockets.remove(socket);
    }

    public boolean registerServerSocket(AsyncServerSocket serverSocket) {
        if (state.get() != RUNNING) {
            return false;
        }

        registeredServerSockets.add(serverSocket);

        if (state.get() != RUNNING) {
            registeredServerSockets.remove(serverSocket);
            return false;
        }

        return true;
    }

    public void deregisterSocket(AsyncServerSocket socket) {
        registeredServerSockets.remove(socket);
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

    public Collection<AsyncSocket> asyncSockets() {
        return registeredSockets;
    }

    @Override
    public final void executeRun() {
        try {
            if (!state.compareAndSet(CREATED, RUNNING)) {
                throw new IllegalStateException("Can't start eventLoop, invalid state:" + state);
            }

            eventLoop();
        } catch (Throwable e) {
            e.printStackTrace();
            logger.severe(e);
        } finally {
            state.set(SHUTDOWN);
            closeSockets();
            System.out.println(getName() + " terminated");
        }
    }

    private void closeSockets() {
        for (AsyncSocket socket : registeredSockets) {
            closeResource(socket);
        }

        for (AsyncServerSocket serverSocket : registeredServerSockets) {
            closeResource(serverSocket);
        }
    }

    protected void runLocalTasks() {
        for (; ; ) {
            EventloopTask task = localRunQueue.poll();
            if (task == null) {
                break;
            }

            try {
                task.run();
            } catch (Exception e) {
                //todo
                e.printStackTrace();
            }
        }
    }

    protected void runConcurrentTasks() {
        for (; ; ) {
            Object task = concurrentRunQueue.poll();
            if (task == null) {
                break;
            }

            if (task instanceof EventloopTask) {
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
}
