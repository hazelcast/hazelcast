package com.hazelcast.spi.impl.engine;


import com.hazelcast.internal.util.executor.HazelcastManagedThread;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.impl.engine.frame.Frame;
import org.jctools.queues.MpmcArrayQueue;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A EventLoop is a thread that is an event loop.
 *
 * The Eventloop infrastructure is unaware of what is being send. So it isn't aware of requests/responses.
 *
 * A single eventloop can deal with many server ports.
 */
public abstract class Eventloop extends HazelcastManagedThread {
    public final ILogger logger = Logger.getLogger(getClass());
    public final Set<AsyncSocket> registeredAsyncSockets = new CopyOnWriteArraySet<>();

    public final AtomicBoolean wakeupNeeded = new AtomicBoolean(true);
    public final MpmcArrayQueue concurrentRunQueue = new MpmcArrayQueue(4096);

    protected Scheduler scheduler;
    public final CircularQueue<AsyncSocket> dirtySockets = new CircularQueue<>(1024);
    protected boolean spin;
    protected volatile boolean running = true;

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
        running = false;
    }

    protected abstract void wakeup();

    public void removeSocket(AsyncSocket socket) {
        registeredAsyncSockets.remove(socket);
    }

    protected abstract void eventLoop() throws Exception;

    public void execute(EventloopTask task) {
        concurrentRunQueue.add(task);
        wakeup();
    }

    public void execute(Collection<Frame> requests) {
        concurrentRunQueue.addAll(requests);
        wakeup();
    }

    public void execute(Frame request) {
        concurrentRunQueue.add(request);
        wakeup();
    }

    public void execute(AsyncSocket socket) {
        if (Thread.currentThread() == this) {
            dirtySockets.offer(socket);
        } else {
            concurrentRunQueue.add(socket);
            wakeup();
        }
    }

    public Collection<AsyncSocket> asyncSockets() {
        return registeredAsyncSockets;
    }

    @Override
    public final void executeRun() {
        try {
            eventLoop();
        } catch (Throwable e) {
            e.printStackTrace();
            logger.severe(e);
        }
    }

    protected void flushDirtySockets() {
        for (; ; ) {
            AsyncSocket channel = dirtySockets.poll();
            if (channel == null) {
                break;
            }

            try {
                channel.handleWriteReady();
            } catch (IOException e) {
                channel.handleException(e);
            }
        }
    }

    protected void runTasks() {
        for (; ; ) {
            Object task = concurrentRunQueue.poll();
            if (task == null) {
                break;
            }

            if (task instanceof AsyncSocket) {
                AsyncSocket channel = (AsyncSocket) task;
                try {
                    channel.handleWriteReady();
                } catch (Exception e) {
                    channel.handleException(e);
                }
            } else if (task instanceof Frame) {
                scheduler.schedule((Frame) task);
            } else if (task instanceof EventloopTask) {
                try {
                    ((EventloopTask) task).run();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else {
                throw new RuntimeException("Unrecognized type:" + task.getClass());
            }
        }
    }
}
