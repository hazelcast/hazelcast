package com.hazelcast.spi.impl.reactor;


import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.internal.util.executor.HazelcastManagedThread;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.reactor.frame.ConcurrentPooledFrameAllocator;
import com.hazelcast.spi.impl.reactor.frame.Frame;
import com.hazelcast.spi.impl.reactor.frame.FrameAllocator;
import com.hazelcast.spi.impl.reactor.frame.NonConcurrentPooledFrameAllocator;
import com.hazelcast.spi.impl.reactor.frame.UnpooledFrameAllocator;
import org.jctools.queues.MpmcArrayQueue;

import java.net.SocketAddress;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Future;

import static com.hazelcast.spi.impl.reactor.frame.Frame.OFFSET_REQUEST_PAYLOAD;
import static com.hazelcast.spi.impl.reactor.frame.Frame.OFFSET_RESPONSE_PAYLOAD;

/**
 * A Reactor is a thread that is an event loop.
 */
public abstract class Reactor extends HazelcastManagedThread {
    protected final ReactorFrontEnd frontend;
    protected final ILogger logger;
    protected final Set<Channel> registeredChannels = new CopyOnWriteArraySet<>();
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
        this.logger = config.logger;

        this.managers = config.managers;
        this.scheduler = new Scheduler(32768, Integer.MAX_VALUE);
        this.requestFrameAllocator = config.poolRequests
                ? new NonConcurrentPooledFrameAllocator(128, true)
                : new UnpooledFrameAllocator();
        this.remoteResponseFrameAllocator = config.poolRemoteResponses
                ? new ConcurrentPooledFrameAllocator(128, true)
                : new UnpooledFrameAllocator();
        this.localResponseFrameAllocator = config.poolLocalResponses
                ? new NonConcurrentPooledFrameAllocator(128, true)
                : new UnpooledFrameAllocator();
        setThreadAffinity(config.threadAffinity);
    }

    public void shutdown(){
        running = false;
    }

    public Future<Channel> schedule(SocketAddress address, Connection connection, SocketConfig socketConfig) {
        System.out.println("asyncConnect connect to " + address);

        ConnectRequest request = new ConnectRequest();
        request.address = address;
        request.connection = connection;
        request.future = new CompletableFuture<>();
        request.socketConfig = socketConfig;
        schedule(request);
        return request.future;
    }

    protected abstract void wakeup();

    public void removeChannel(Channel nioChannel) {
        registeredChannels.remove(nioChannel);
    }

    protected abstract void eventLoop() throws Exception;

    public void schedule(ReactorTask task){
        publicRunQueue.add(task);
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

    public void schedule(ConnectRequest request) {
        publicRunQueue.add(request);
        wakeup();
    }

    public Collection<Channel> channels() {
        return registeredChannels;
    }

    protected abstract void handleConnect(ConnectRequest request);

    protected abstract void handleWrite(Channel task);

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
            } else if(task instanceof ReactorTask){
                try {
                    ((ReactorTask)task).run();
                } catch (Exception e) {
                    e.printStackTrace();
                }
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
            op.response = localResponseFrameAllocator.allocate(OFFSET_RESPONSE_PAYLOAD);
        } else {
            op.response = remoteResponseFrameAllocator.allocate(OFFSET_RESPONSE_PAYLOAD);
        }
        op.request = request.position(OFFSET_REQUEST_PAYLOAD);
        scheduler.schedule(op);
    }
}
