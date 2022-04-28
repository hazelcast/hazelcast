package com.hazelcast.spi.impl.engine;

import com.hazelcast.internal.util.ThreadAffinity;
import com.hazelcast.spi.impl.engine.frame.Frame;
import com.hazelcast.spi.impl.engine.nio.NioEventloop;
import com.hazelcast.spi.impl.engine.epoll.EpollEventloop;
import com.hazelcast.spi.impl.engine.iouring.IOUringEventloop;

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
    private boolean spin;
    private EventloopType eventloopType;
    private final ThreadAffinity threadAffinity;
    private Eventloop[] eventloops;
    private int eventloopCount;
    private String eventloopBasename = "eventloop-";
    private MonitorThread monitorThread;

    public Engine(Supplier<Scheduler> schedulerSupplier) {
        this.schedulerSupplier = checkNotNull(schedulerSupplier);
        this.eventloopCount = Integer.parseInt(getProperty("reactor.count", "" + Runtime.getRuntime().availableProcessors()));
        this.spin = Boolean.parseBoolean(getProperty("reactor.spin", "false"));
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

    public Eventloop[] eventloops(){
        return eventloops;
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
        this.spin = evenloopSpin;
        return this;
    }

    public Eventloop eventloopForHash(int hash) {
        return eventloops[hashToIndex(hash, eventloops.length)];
    }

    public Eventloop eventloop(int eventloopIdx) {
        return eventloops[eventloopIdx];
    }

    public void run(int eventloopIdx, Collection<Frame> frames) {
        eventloops[eventloopIdx].execute(frames);
    }

    public void run(int eventloopIdx, Frame frame) {
        eventloops[eventloopIdx].execute(frame);
    }

    public void run(int eventloopIdx, EventloopTask task) {
        eventloops[eventloopIdx].execute(task);
    }

    public void createEventLoops(){
        this.eventloops = new Eventloop[eventloopCount];
        for (int idx = 0; idx < eventloops.length; idx++) {
            Scheduler scheduler = schedulerSupplier.get();
            Eventloop eventloop;
            switch (eventloopType) {
                case NIO:
                    eventloop = new NioEventloop();
                    break;
                case EPOLL:
                    eventloop = new EpollEventloop();
                    break;
                case IOURING:
                    eventloop = new IOUringEventloop();
                    break;
                default:
                    throw new IllegalStateException("Unknown reactorType:" + eventloopType);
            }

            eventloop.setName(eventloopBasename + idx);
            eventloop.setScheduler(scheduler);
            eventloop.setSpin(spin);
            eventloops[idx] = eventloop;
        }
    }

    public void start() {
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
        out.println("reactor.spin:" + spin);
        out.println("reactor.type:" + eventloopType);
        out.println("reactor.cpu-affinity:" + threadAffinity);
    }
}
