package com.hazelcast.spi.impl.engine;

import com.hazelcast.internal.util.ThreadAffinity;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.impl.engine.frame.Frame;
import com.hazelcast.spi.impl.engine.nio.NioReactor;
import io.netty.channel.epoll.EpollReactor;
import io.netty.incubator.channel.uring.IOUringReactor;

import java.util.Collection;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.hazelcast.internal.util.HashUtil.hashToIndex;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static java.lang.System.getProperty;
import static java.lang.System.out;

/**
 * The Engine is effectively an array of reactors.
 *
 * The Engine is not aware of any specific applications. E.g. it could execute operations, but it
 * can equally well run client requests or completely different applications.
 */
public final class Engine {

    private final boolean monitorSilent;
    private final Supplier<Scheduler> schedulerSupplier;
    private boolean reactorSpin;
    private ReactorType reactorType;
    private final ThreadAffinity threadAffinity;
    private Reactor[] reactors;
    private int reactorCount;
    private String reactorBaseName = "reactor-";
    private MonitorThread reactorMonitorThread;

    public Engine(Supplier<Scheduler> schedulerSupplier) {
        this.schedulerSupplier = checkNotNull(schedulerSupplier);
        this.reactorCount = Integer.parseInt(getProperty("reactor.count", "" + Runtime.getRuntime().availableProcessors()));
        this.reactorSpin = Boolean.parseBoolean(getProperty("reactor.spin", "false"));
        this.reactorType = ReactorType.fromString(getProperty("reactor.type", "nio"));
        this.threadAffinity = ThreadAffinity.newSystemThreadAffinity("reactor.cpu-affinity");
        this.monitorSilent = Boolean.parseBoolean(getProperty("reactor.monitor.silent", "false"));
    }

    public ReactorType getReactorType() {
        return reactorType;
    }

    public Engine setReactorType(ReactorType reactorType) {
        this.reactorType = checkNotNull(reactorType);
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

        for (Reactor reactor : reactors) {
            reactor.setThreadAffinity(threadAffinity);
        }

        return this;
    }

    public Engine setReactorBaseName(String baseName) {
        this.reactorBaseName = checkNotNull(baseName, "baseName");
        return this;
    }

    public Engine setReactorCount(int reactorCount) {
        this.reactorCount = checkPositive("reactorCount", reactorCount);
        return this;
    }

    public int reactorCount() {
        return reactorCount;
    }

    public void forEach(Consumer<Reactor> function) {
        for (Reactor reactor : reactors) {
            function.accept(reactor);
        }
    }

    public Engine setSpin(boolean reactorSpin) {
        this.reactorSpin = reactorSpin;
        return this;
    }

    public Reactor reactorForHash(int hash) {
        return reactors[hashToIndex(hash, reactors.length)];
    }

    public Reactor reactor(int reactorIdx) {
        return reactors[reactorIdx];
    }

    public void run(int reactorIdx, Collection<Frame> frames) {
        reactors[reactorIdx].schedule(frames);
    }

    public void run(int reactorIdx, Frame frame) {
        reactors[reactorIdx].schedule(frame);
    }

    public void run(int reactorIdx, ReactorTask task) {
        reactors[reactorIdx].schedule(task);
    }

    public void start() {
        this.reactors = new Reactor[reactorCount];
        ILogger logger = Logger.getLogger(Reactor.class);
        for (int idx = 0; idx < reactors.length; idx++) {
            String name = reactorBaseName + idx;
            Scheduler scheduler = schedulerSupplier.get();
            switch (reactorType) {
                case NIO:
                    reactors[idx] = new NioReactor(idx, name, logger, scheduler, reactorSpin);
                    break;
                case EPOLL:
                    reactors[idx] = new EpollReactor(idx, name, logger, scheduler, reactorSpin);
                    break;
                case IOURING:
                    reactors[idx] = new IOUringReactor(idx, name, logger, scheduler, reactorSpin);
                    break;
                default:
                    throw new IllegalStateException("Unknown reactorType:" + reactorType);
            }
        }

        for (Reactor reactor : reactors) {
            if (threadAffinity != null) {
                reactor.setThreadAffinity(threadAffinity);
            }
            reactor.start();
        }

        this.reactorMonitorThread = new MonitorThread(reactors, monitorSilent);
        this.reactorMonitorThread.start();
    }

    public void shutdown() {
        for (Reactor reactor : reactors) {
            reactor.shutdown();
        }

        reactorMonitorThread.shutdown();
    }

    public void printConfig() {
        out.println("reactor.count:" + reactorCount);
        out.println("reactor.spin:" + reactorSpin);
        out.println("reactor.type:" + reactorType);
        out.println("reactor.cpu-affinity:" + threadAffinity);
    }
}
