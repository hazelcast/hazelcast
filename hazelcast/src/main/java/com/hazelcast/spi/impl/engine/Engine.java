package com.hazelcast.spi.impl.engine;

import com.hazelcast.internal.util.ThreadAffinity;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.impl.engine.frame.Frame;
import com.hazelcast.spi.impl.engine.nio.NioEventloop;
import io.netty.channel.epoll.EpollEventloop;
import io.netty.incubator.channel.uring.IOUringEventloop;

import java.util.Collection;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.hazelcast.internal.util.HashUtil.hashToIndex;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static java.lang.System.getProperty;
import static java.lang.System.out;

/**
 * The Engine is effectively an array of eventloops
 *
 * The Engine is not aware of any specific applications. E.g. it could execute operations, but it
 * can equally well run client requests or completely different applications.
 */
public final class Engine {

    private final boolean monitorSilent;
    private final Supplier<Scheduler> schedulerSupplier;
    private boolean eventloopSpin;
    private EventloopType eventloopType;
    private final ThreadAffinity threadAffinity;
    private Eventloop[] eventloops;
    private int eventloopCount;
    private String eventloopBasename = "eventloop-";
    private MonitorThread monitorThread;

    public Engine(Supplier<Scheduler> schedulerSupplier) {
        this.schedulerSupplier = checkNotNull(schedulerSupplier);
        this.eventloopCount = Integer.parseInt(getProperty("reactor.count", "" + Runtime.getRuntime().availableProcessors()));
        this.eventloopSpin = Boolean.parseBoolean(getProperty("reactor.spin", "false"));
        this.eventloopType = EventloopType.fromString(getProperty("reactor.type", "nio"));
        this.threadAffinity = ThreadAffinity.newSystemThreadAffinity("reactor.cpu-affinity");
        this.monitorSilent = Boolean.parseBoolean(getProperty("reactor.monitor.silent", "false"));
    }

    public EventloopType getEventloopType() {
        return eventloopType;
    }

    public Engine setEventLoopType(EventloopType eventloopType) {
        this.eventloopType = checkNotNull(eventloopType);
        return this;
    }

    /**
     * Sets the ThreadAffinity for the reactor threads.
     *
     * Can only be called before the {@link #start()}.
     *
     * @param threadAffinity the ThreadAffinity
     * @throws NullPointerException if threadAffinity is null.
     */
    public Engine setThreadAffinity(ThreadAffinity threadAffinity) {
        checkNotNull(threadAffinity);

        for (Eventloop eventloop : eventloops) {
            eventloop.setThreadAffinity(threadAffinity);
        }

        return this;
    }

    public Engine setEventloopBasename(String baseName) {
        this.eventloopBasename = checkNotNull(baseName, "baseName");
        return this;
    }

    public Engine setEventloopCount(int eventloopCount) {
        this.eventloopCount = checkPositive("reactorCount", eventloopCount);
        return this;
    }

    public int eventloopCount() {
        return eventloopCount;
    }

    public void forEach(Consumer<Eventloop> function) {
        for (Eventloop eventloop : eventloops) {
            function.accept(eventloop);
        }
    }

    public Engine setSpin(boolean evenloopSpin) {
        this.eventloopSpin = evenloopSpin;
        return this;
    }

    public Eventloop eventloopForHash(int hash) {
        return eventloops[hashToIndex(hash, eventloops.length)];
    }

    public Eventloop eventloop(int eventloopIdx) {
        return eventloops[eventloopIdx];
    }

    public void run(int eventloopIdx, Collection<Frame> frames) {
        eventloops[eventloopIdx].schedule(frames);
    }

    public void run(int eventloopIdx, Frame frame) {
        eventloops[eventloopIdx].schedule(frame);
    }

    public void run(int eventloopIdx, EventloopTask task) {
        eventloops[eventloopIdx].schedule(task);
    }

    public void start() {
        this.eventloops = new Eventloop[eventloopCount];
        ILogger logger = Logger.getLogger(Eventloop.class);
        for (int idx = 0; idx < eventloops.length; idx++) {
            String name = eventloopBasename + idx;
            Scheduler scheduler = schedulerSupplier.get();
            switch (eventloopType) {
                case NIO:
                    eventloops[idx] = new NioEventloop(idx, name, logger, scheduler, eventloopSpin);
                    break;
                case EPOLL:
                    eventloops[idx] = new EpollEventloop(idx, name, logger, scheduler, eventloopSpin);
                    break;
                case IOURING:
                    eventloops[idx] = new IOUringEventloop(idx, name, logger, scheduler, eventloopSpin);
                    break;
                default:
                    throw new IllegalStateException("Unknown reactorType:" + eventloopType);
            }
        }

        for (Eventloop eventloop : eventloops) {
            if (threadAffinity != null) {
                eventloop.setThreadAffinity(threadAffinity);
            }
            eventloop.start();
        }

        this.monitorThread = new MonitorThread(eventloops, monitorSilent);
        this.monitorThread.start();
    }

    public void shutdown() {
        for (Eventloop eventloop : eventloops) {
            eventloop.shutdown();
        }

        monitorThread.shutdown();
    }

    public void printConfig() {
        out.println("reactor.count:" + eventloopCount);
        out.println("reactor.spin:" + eventloopSpin);
        out.println("reactor.type:" + eventloopType);
        out.println("reactor.cpu-affinity:" + threadAffinity);
    }
}
