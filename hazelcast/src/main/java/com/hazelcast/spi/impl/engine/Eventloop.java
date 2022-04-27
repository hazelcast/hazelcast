package com.hazelcast.spi.impl.engine;


import com.hazelcast.internal.util.executor.HazelcastManagedThread;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.engine.frame.Frame;
import org.jctools.queues.MpmcArrayQueue;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
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
    public final ConcurrentMap context = new ConcurrentHashMap();
    public final ILogger logger;
    public final Set<Channel> registeredChannels = new CopyOnWriteArraySet<>();

    public final AtomicBoolean wakeupNeeded = new AtomicBoolean(true);
    public final MpmcArrayQueue publicRunQueue = new MpmcArrayQueue(4096);

    public final Scheduler scheduler;
    public final CircularQueue<Channel> dirtyChannels = new CircularQueue<>(1024);

    protected final boolean spin;
    protected final int idx;
    protected volatile boolean running = true;

    public Eventloop(int idx, String name, ILogger logger, Scheduler scheduler, boolean spin) {
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

    public abstract CompletableFuture<Channel> connect(Channel channel, SocketAddress address);

    protected abstract void wakeup();

    public void removeChannel(Channel nioChannel) {
        registeredChannels.remove(nioChannel);
    }

    protected abstract void eventLoop() throws Exception;

    public void execute(EventloopTask task) {
        publicRunQueue.add(task);
        wakeup();
    }

    public void execute(Collection<Frame> requests) {
        publicRunQueue.addAll(requests);
        wakeup();
    }

    public void execute(Frame request) {
        publicRunQueue.add(request);
        wakeup();
    }

    public void execute(Channel channel) {
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

            try {
                channel.handleWrite();
            }catch (IOException e){
                channel.handleException(e);
            }
        }
    }

    protected void runTasks() {
        for (; ; ) {
            Object task = publicRunQueue.poll();
            if (task == null) {
                break;
            }

            if (task instanceof Channel) {
                Channel channel = (Channel)task;
                try {
                    channel.handleWrite();
                }catch (Exception e){
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
