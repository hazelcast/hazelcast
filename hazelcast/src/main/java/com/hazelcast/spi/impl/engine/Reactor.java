package com.hazelcast.spi.impl.engine;


import com.hazelcast.internal.util.executor.HazelcastManagedThread;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.engine.frame.Frame;
import org.jctools.queues.MpmcArrayQueue;

import java.net.SocketAddress;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Future;

/**
 * A Reactor is a thread that is an event loop.
 *
 * The Reactor infrastructure is unaware of what is being send. So it isn't aware of requests/responses.
 *
 * A single reactor can deal with many server ports.
 */
public abstract class Reactor extends HazelcastManagedThread {
    public final ConcurrentMap context = new ConcurrentHashMap();
    protected final ILogger logger;
    public final Set<Channel> registeredChannels = new CopyOnWriteArraySet<>();
    public final MpmcArrayQueue publicRunQueue = new MpmcArrayQueue(4096);
    public final Scheduler scheduler;
    public final CircularQueue<Channel> dirtyChannels = new CircularQueue<>(1024);
    protected final boolean spin;
    protected final int idx;
    protected volatile boolean running = true;

    public Reactor(int idx, String name, ILogger logger, Scheduler scheduler, boolean spin) {
        super(name);
        this.idx = idx;
        this.logger = logger;
        this.scheduler = scheduler;
        this.spin = spin;
    }

    public Scheduler getScheduler() {
        return scheduler;
    }

    public int getIdx() {
        return idx;
    }

    public void shutdown() {
        running = false;
    }

    public abstract Future<Channel> connect(Channel channel, SocketAddress address);

    protected abstract void wakeup();

    public void removeChannel(Channel nioChannel) {
        registeredChannels.remove(nioChannel);
    }

    protected abstract void eventLoop() throws Exception;

    public void schedule(ReactorTask task) {
        publicRunQueue.add(task);
        wakeup();
    }

    public void schedule(Collection<Frame> requests) {
        publicRunQueue.addAll(requests);
        wakeup();
    }

    public void schedule(Frame request) {
        publicRunQueue.add(request);
        wakeup();
    }

    public void schedule(Channel channel) {
        if (Thread.currentThread() == this) {
            dirtyChannels.offer(channel);
        } else {
            publicRunQueue.add(channel);
            wakeup();
        }
    }

    public Collection<Channel> channels() {
        return registeredChannels;
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

    protected void flushDirtyChannels() {
        for (; ; ) {
            Channel channel = dirtyChannels.poll();
            if (channel == null) {
                break;
            }

            channel.handleWrite();
        }
    }

    protected void runTasks() {
        for (; ; ) {
            Object task = publicRunQueue.poll();
            if (task == null) {
                break;
            }

            if (task instanceof Channel) {
                ((Channel) task).handleWrite();
            } else if (task instanceof Frame) {
                scheduler.schedule((Frame) task);
            } else if (task instanceof ReactorTask) {
                try {
                    ((ReactorTask) task).run();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else {
                throw new RuntimeException("Unrecognized type:" + task.getClass());
            }
        }
    }
}
