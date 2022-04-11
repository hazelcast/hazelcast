package com.hazelcast.spi.impl.reactor;


import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.internal.util.executor.HazelcastManagedThread;
import com.hazelcast.logging.ILogger;
import org.jctools.queues.MpmcArrayQueue;
import org.jctools.queues.MpscArrayQueue;

import java.net.SocketAddress;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.spi.impl.reactor.Frame.OFFSET_REQUEST_PAYLOAD;

/**
 * A Reactor is a thread that is an event loop.
 */
public abstract class Reactor extends HazelcastManagedThread {
    protected final ReactorFrontEnd frontend;
    protected final ILogger logger;
    protected final Address thisAddress;
    protected final int port;
    protected final ChannelConfig channelConfig;
    protected final Set<Channel> channels = new CopyOnWriteArraySet<>();
    protected final FrameAllocator requestFrameAllocator;
    protected final FrameAllocator remoteResponseFrameAllocator;
    protected final FrameAllocator localResponseFrameAllocator;
    public final MpmcArrayQueue publicRunQueue = new MpmcArrayQueue(4096);
    protected final SwCounter requests = SwCounter.newSwCounter();
    protected final Scheduler scheduler;
    private final OpAllocator opAllocator = new OpAllocator();
    public final CircularQueue<Channel> dirtyChannels = new CircularQueue<>(1024);
    private final Managers managers;
    protected volatile boolean running = true;

    public Reactor(ReactorConfig config) {
        super(config.name);
        this.frontend = config.frontend;
        this.channelConfig = config.channelConfig;
        this.logger = config.logger;
        this.thisAddress = config.thisAddress;
        this.port = config.port;
        this.managers = config.managers;
        this.scheduler = new Scheduler(32768, Integer.MAX_VALUE);
        this.requestFrameAllocator = config.poolRequests
                ? new NonConcurrentPooledFrameAllocator(128, true)
                : new UnpooledFrameAllocator();
        this.remoteResponseFrameAllocator = config.poolResponses
                ? new ConcurrentPooledFrameAllocator(128, true)
                : new UnpooledFrameAllocator();
        this.localResponseFrameAllocator = config.poolResponses
                ? new NonConcurrentPooledFrameAllocator(128, true)
                : new UnpooledFrameAllocator();
        setThreadAffinity(config.threadAffinity);
    }

    public void shutdown(){
        running = false;
    }

    public Future<Channel> schedule(SocketAddress address, Connection connection) {
        System.out.println("asyncConnect connect to " + address);

        ConnectRequest request = new ConnectRequest();
        request.address = address;
        request.connection = connection;
        request.future = new CompletableFuture<>();

        schedule(request);

        return request.future;
    }

    protected abstract void wakeup();

    public void removeChannel(Channel nioChannel) {
        channels.remove(nioChannel);
    }

    protected abstract void setupServerSocket() throws Exception;

    protected abstract void eventLoop() throws Exception;

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

    public void schedule(ConnectRequest request) {
        publicRunQueue.add(request);
        wakeup();
    }

    public Collection<Channel> channels() {
        return channels;
    }

    protected abstract void handleConnect(ConnectRequest request);

    protected abstract void handleWrite(Channel task);

    @Override
    public final void executeRun() {
        try {
            setupServerSocket();
        } catch (Throwable e) {
            logger.severe(e);
            return;
        }

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

            handleWrite(channel);
        }
    }

    protected void runTasks() {
        for (; ; ) {
            Object task = publicRunQueue.poll();
            if (task == null) {
                break;
            }

            if (task instanceof Channel) {
                handleWrite((Channel) task);
            } else if (task instanceof Frame) {
                handleRequest((Frame) task);
            } else if (task instanceof ConnectRequest) {
                handleConnect((ConnectRequest) task);
            } else {
                throw new RuntimeException("Unrecognized type:" + task.getClass());
            }
        }
    }

    protected void handleRequest(Frame request) {
        requests.inc();
        Op op = opAllocator.allocate(request);
        op.managers = managers;
        if (request.future == null) {
            op.response = localResponseFrameAllocator.allocate(21);
        } else {
            op.response = remoteResponseFrameAllocator.allocate(21);
        }
        op.request = request.position(OFFSET_REQUEST_PAYLOAD);
        scheduler.schedule(op);
    }
}
