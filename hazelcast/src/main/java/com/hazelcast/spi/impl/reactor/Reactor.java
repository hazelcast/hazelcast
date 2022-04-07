package com.hazelcast.spi.impl.reactor;


import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.internal.util.executor.HazelcastManagedThread;
import com.hazelcast.logging.ILogger;

import java.net.SocketAddress;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Future;

import static com.hazelcast.spi.impl.reactor.Frame.OFFSET_REQUEST_PAYLOAD;

public abstract class Reactor extends HazelcastManagedThread {
    protected final ReactorFrontEnd frontend;
    protected final ILogger logger;
    protected final Address thisAddress;
    protected final int port;
    protected final ChannelConfig channelConfig;
    protected final Set<Channel> channels = new CopyOnWriteArraySet<>();
    protected final FrameAllocator requestFrameAllocator = new UnpooledFrameAllocator(128);
    protected final FrameAllocator responseFrameAllocator = new UnpooledFrameAllocator(128);
    protected final ConcurrentLinkedQueue runQueue = new ConcurrentLinkedQueue();
    protected final SwCounter requests = SwCounter.newSwCounter();
    protected final Scheduler scheduler;
    private final OpAllocator opAllocator = new OpAllocator();

    protected final CircularQueue<Channel> dirtyChannels = new CircularQueue<>(512);

    public Reactor(ReactorFrontEnd frontend, ChannelConfig channelConfig, Address thisAddress, int port, String name) {
        super(name);
        this.frontend = frontend;
        this.channelConfig = channelConfig;
        this.logger = frontend.nodeEngine.getLogger(getClass());
        this.thisAddress = thisAddress;
        this.port = port;
        this.scheduler = new Scheduler(4096, Integer.MAX_VALUE);
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

    public static class ConnectRequest {
        public Connection connection;
        public SocketAddress address;
        public CompletableFuture<Channel> future;
    }

    protected abstract void setupServerSocket() throws Exception;

    protected abstract void eventLoop() throws Exception;

    public void schedule(Frame request) {
        runQueue.add(request);
        wakeup();
    }

    public void schedule(Channel channel) {
        if (Thread.currentThread() == this) {
            dirtyChannels.enqueue(channel);
        } else {
            runQueue.add(channel);
            wakeup();
        }
    }

    public void schedule(ConnectRequest request) {
        runQueue.add(request);
        wakeup();
    }

    public Collection<Channel> channels() {
        return channels;
    }

    protected abstract void handleConnectRequest(ConnectRequest task);

    protected abstract void handleOutbound(Channel task);

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
            Channel channel = dirtyChannels.dequeue();
            if (channel == null) {
                break;
            }

            handleOutbound(channel);
        }
    }

    protected void runTasks() {
        for (; ; ) {
            Object task = runQueue.poll();
            if (task == null) {
                break;
            }

            if (task instanceof Channel) {
                handleOutbound((Channel) task);
            } else if (task instanceof Frame) {
                handleRequest((Frame) task);
            } else if (task instanceof ConnectRequest) {
                handleConnectRequest((ConnectRequest) task);
            } else {
                throw new RuntimeException("Unrecognized type:" + task.getClass());
            }
        }
    }

    protected void handleRequest(Frame request) {
        requests.inc();
        Op op = opAllocator.allocate(request);
        op.managers = frontend.managers;
        op.response = responseFrameAllocator.allocate(21);
        op.request = request.position(OFFSET_REQUEST_PAYLOAD);
        scheduler.schedule(op);
    }
}
