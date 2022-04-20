package com.hazelcast.spi.impl.requestservice;

import com.hazelcast.cluster.Address;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.server.ServerConnectionManager;
import com.hazelcast.internal.server.tcp.TcpServerConnection;
import com.hazelcast.internal.util.concurrent.MPSCQueue;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.engine.Channel;
import com.hazelcast.spi.impl.engine.Engine;
import com.hazelcast.spi.impl.engine.Reactor;
import com.hazelcast.spi.impl.engine.ReactorType;
import com.hazelcast.spi.impl.engine.SocketConfig;
import com.hazelcast.spi.impl.engine.frame.ConcurrentPooledFrameAllocator;
import com.hazelcast.spi.impl.engine.frame.Frame;
import com.hazelcast.spi.impl.engine.frame.FrameAllocator;
import com.hazelcast.spi.impl.engine.frame.NonConcurrentPooledFrameAllocator;
import com.hazelcast.spi.impl.engine.frame.UnpooledFrameAllocator;
import com.hazelcast.spi.impl.engine.nio.NioChannel;
import com.hazelcast.spi.impl.engine.nio.NioReactor;
import com.hazelcast.spi.impl.engine.nio.NioServerChannel;
import com.hazelcast.table.impl.PipelineImpl;
import com.hazelcast.table.impl.TableManager;
import io.netty.channel.epoll.EpollChannel;
import io.netty.channel.epoll.EpollReactor;
import io.netty.channel.epoll.EpollServerChannel;
import io.netty.incubator.channel.uring.IOUringChannel;
import io.netty.incubator.channel.uring.IOUringReactor;
import io.netty.incubator.channel.uring.IOUringServerChannel;
import org.jctools.util.PaddedAtomicLong;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static com.hazelcast.internal.util.HashUtil.hashToIndex;
import static com.hazelcast.spi.impl.engine.frame.Frame.OFFSET_RESPONSE_CALL_ID;


/**
 * The RequestService is an application of the Engine.
 *
 * The Reactor is very specific to requests/responses. It isn't a flexible framework unlike Seastar.
 *
 * Mapping from partition to CPU is easy; just a simple mod.
 *
 * RSS: How can we align:
 * - the CPU receiving data from some TCP/IP-connection.
 * - and pinning the same CPU to the RX-queue that processes that TCP/IP-connection
 * So how can we make sure that all TCP/IP-connections for that CPU are processed by the same CPU processing the IRQ.
 *
 * And how can we make sure that for example we want to isolate a few CPUs for the RSS part, but then
 * forward to the CPU that owns the TCP/IP-connection
 *
 * So it appears that Seastar is using the toeplitz hash
 * https://github.com/scylladb/seastar/issues/654
 *
 * So we have a list of channels to some machine.
 *
 * And we determine for each of the channel the toeplitz hash based on src/dst port/ip;
 *
 * So this would determine which channels are mapped to some CPU.
 *
 * So how do we go from partition to a channel?
 */
public class RequestService {

    public final NodeEngineImpl nodeEngine;
    public final InternalSerializationService ss;
    public final ILogger logger;
    private final Address thisAddress;
    private final int channelCount;
    private final SocketConfig socketConfig;
    private final boolean poolRequests;
    private final boolean poolLocalResponses;
    private final boolean poolRemoteResponses;
    private final boolean writeThrough;
    private final int responseThreadCount;
    private final boolean responseThreadSpin;
    private final int requestTimeoutMs;
    private final boolean regularSchedule;
    private volatile ServerConnectionManager connectionManager;
    public volatile boolean shuttingdown = false;
    public final Managers managers;
    private final ConcurrentMap<SocketAddress, Requests> requestsPerChannel = new ConcurrentHashMap<>();
    private final ResponseThread[] responseThreads;
    private int[] partitionIdToChannel;
    private Engine engine;
    private int concurrentRequestLimit;

    public RequestService(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(RequestService.class);
        this.ss = (InternalSerializationService) nodeEngine.getSerializationService();
        this.responseThreadCount = Integer.parseInt(java.lang.System.getProperty("reactor.responsethread.count", "1"));
        this.responseThreadSpin = Boolean.parseBoolean(java.lang.System.getProperty("reactor.responsethread.spin", "false"));
        this.writeThrough = Boolean.parseBoolean(java.lang.System.getProperty("reactor.write-through", "false"));
        this.regularSchedule = Boolean.parseBoolean(java.lang.System.getProperty("reactor.regular-schedule", "true"));
        this.poolRequests = Boolean.parseBoolean(java.lang.System.getProperty("reactor.pool-requests", "true"));
        this.poolLocalResponses = Boolean.parseBoolean(java.lang.System.getProperty("reactor.pool-local-responses", "true"));
        this.poolRemoteResponses = Boolean.parseBoolean(java.lang.System.getProperty("reactor.pool-remote-responses", "false"));
        this.concurrentRequestLimit = Integer.parseInt(java.lang.System.getProperty("reactor.concurrent-request-limit", "-1"));
        this.requestTimeoutMs = Integer.parseInt(java.lang.System.getProperty("reactor.request.timeoutMs", "23000"));

        this.channelCount = Integer.parseInt(java.lang.System.getProperty("reactor.channels", "" + Runtime.getRuntime().availableProcessors()));
        printReactorInfo();
        this.thisAddress = nodeEngine.getThisAddress();
        this.engine = newApplication();
        this.socketConfig = new SocketConfig();
        this.managers = new Managers();
        //hack
        managers.tableManager = new TableManager(271);

        this.partitionIdToChannel = new int[271];
        for (int k = 0; k < 271; k++) {
            partitionIdToChannel[k] = hashToIndex(k, channelCount);
        }

        this.responseThreads = new ResponseThread[responseThreadCount];
        for (int k = 0; k < responseThreadCount; k++) {
            this.responseThreads[k] = new ResponseThread();
        }
    }

    public int getRequestTimeoutMs() {
        return requestTimeoutMs;
    }

    @NotNull
    private Engine newApplication() {
        Engine engine = new Engine(() -> {
            FrameAllocator remoteResponseFrameAllocator = poolRemoteResponses
                    ? new ConcurrentPooledFrameAllocator(128, true)
                    : new UnpooledFrameAllocator();
            FrameAllocator localResponseFrameAllocator = poolLocalResponses
                    ? new NonConcurrentPooledFrameAllocator(128, true)
                    : new UnpooledFrameAllocator();

            return new OpScheduler(32768,
                    Integer.MAX_VALUE,
                    managers,
                    localResponseFrameAllocator,
                    remoteResponseFrameAllocator);
        });
        engine.setReactorBaseName("Reactor:[" + thisAddress.getHost() + ":" + thisAddress.getPort() + "]:");
        engine.printConfig();
        return engine;
    }

    private void configureNio() {
        engine.forEach(r -> {
            try {
                NioReactor reactor = (NioReactor) r;
                int port = toPort(thisAddress, reactor.getIdx());

                NioServerChannel serverChannel = new NioServerChannel();
                serverChannel.socketConfig = socketConfig;
                serverChannel.address = new InetSocketAddress(thisAddress.getInetAddress(), port);

                Supplier<NioChannel> channelSupplier = () -> {
                    RequestNioChannel channel = new RequestNioChannel();
                    channel.writeThrough = writeThrough;
                    channel.regularSchedule = regularSchedule;
                    channel.opScheduler = (OpScheduler) reactor.scheduler;
                    channel.requestService = RequestService.this;
                    channel.socketConfig = socketConfig;
                    channel.requestFrameAllocator = poolRequests
                            ? new NonConcurrentPooledFrameAllocator(128, true)
                            : new UnpooledFrameAllocator();
                    channel.remoteResponseFrameAllocator = poolRemoteResponses
                            ? new ConcurrentPooledFrameAllocator(128, true)
                            : new UnpooledFrameAllocator();
                    return channel;
                };
                reactor.context.put("requestChannelSupplier", channelSupplier);
                serverChannel.channelSupplier = channelSupplier;

                reactor.accept(serverChannel);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    private void configureIOUring() {
        engine.forEach(r -> {
            try {
                IOUringReactor reactor = (IOUringReactor) r;
                int port = toPort(thisAddress, reactor.getIdx());

                IOUringServerChannel serverChannel = new IOUringServerChannel();
                serverChannel.socketConfig = socketConfig;
                serverChannel.address = new InetSocketAddress(thisAddress.getInetAddress(), port);

                Supplier<IOUringChannel> channelSupplier = () -> {
                    RequestIOUringChannel channel = new RequestIOUringChannel();
                    channel.opScheduler = (OpScheduler) reactor.scheduler;
                    channel.requestService = this;
                    channel.socketConfig = socketConfig;
                    channel.requestFrameAllocator = poolRequests
                            ? new NonConcurrentPooledFrameAllocator(128, true)
                            : new UnpooledFrameAllocator();
                    channel.remoteResponseFrameAllocator = poolRemoteResponses
                            ? new ConcurrentPooledFrameAllocator(128, true)
                            : new UnpooledFrameAllocator();
                    return channel;
                };
                reactor.context.put("requestChannelSupplier", channelSupplier);
                serverChannel.channelSupplier = channelSupplier;
                reactor.accept(serverChannel);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    private void configureEpoll() {
        engine.forEach(r -> {
            try {
                EpollReactor reactor = (EpollReactor) r;

                EpollServerChannel serverChannel = new EpollServerChannel();
                serverChannel.socketConfig = socketConfig;
                int port = toPort(thisAddress, reactor.getIdx());
                serverChannel.address = new InetSocketAddress(thisAddress.getInetAddress(), port);

                Supplier<EpollChannel> channelSupplier = () -> {
                    RequestEpollChannel channel = new RequestEpollChannel();
                    channel.opScheduler = (OpScheduler) reactor.scheduler;
                    channel.requestService = this;
                    channel.socketConfig = socketConfig;
                    channel.requestFrameAllocator = poolRequests
                            ? new NonConcurrentPooledFrameAllocator(128, true)
                            : new UnpooledFrameAllocator();
                    channel.remoteResponseFrameAllocator = poolRemoteResponses
                            ? new ConcurrentPooledFrameAllocator(128, true)
                            : new UnpooledFrameAllocator();
                    return channel;
                };
                reactor.context.put("requestChannelSupplier", channelSupplier);
                serverChannel.channelSupplier = channelSupplier;
                reactor.accept(serverChannel);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    private void printReactorInfo() {
        java.lang.System.out.println("reactor.responsethread.count:" + responseThreadCount);
        java.lang.System.out.println("reactor.write-through:" + writeThrough);
        java.lang.System.out.println("reactor.channels:" + channelCount);
        java.lang.System.out.println("reactor.pool-requests:" + poolRequests);
        java.lang.System.out.println("reactor.pool-local-responses:" + poolLocalResponses);
        java.lang.System.out.println("reactor.pool-remote-responses:" + poolRemoteResponses);
        java.lang.System.out.println("reactor.cpu-affinity:" + java.lang.System.getProperty("reactor.cpu-affinity"));
    }

    public int toPort(Address address, int cpu) {
        return (address.getPort() - 5701) * 100 + 11000 + cpu;
    }

    public int partitionIdToChannel(int partitionId) {
        return hashToIndex(partitionId, channelCount);
    }

    public void start() {
        logger.info("Starting ReactorFrontend");
        engine.start();

        ReactorType reactorType = engine.getReactorType();
        switch (reactorType) {
            case NIO:
                configureNio();
                break;
            case EPOLL:
                configureEpoll();
                break;
            case IOURING:
                configureIOUring();
                break;
            default:
                throw new RuntimeException();
        }

        for (ResponseThread responseThread : responseThreads) {
            responseThread.start();
        }
    }

    public void shutdown() {
        logger.info("Shutting down ReactorFrontend");

        shuttingdown = true;

        engine.shutdown();

        for (Requests requests : requestsPerChannel.values()) {
            for (Frame request : requests.map.values()) {
                request.future.completeExceptionally(new RuntimeException("Shutting down"));
            }
        }

        for (ResponseThread responseThread : responseThreads) {
            responseThread.shutdown();
        }
    }


    // TODO: We can simplify this by attaching the requests for a member, directly to that
    // channel so we don't need to do a requests lookup.
    public void handleResponse(Frame response) {
        if (response.next != null) {
            // probably better to use call-id.
            int index = responseThreadCount == 0
                    ? 0
                    : hashToIndex(response.getInt(Frame.OFFSET_PARTITION_ID), responseThreadCount);
            responseThreads[index].queue.add(response);
            return;
        }

        try {
            Requests requests = requestsPerChannel.get(response.channel.remoteAddress);
            if (requests == null) {
                System.out.println("Dropping response " + response + ", requests not found");
                return;
            }

            requests.complete();

            long callId = response.getLong(OFFSET_RESPONSE_CALL_ID);
            //System.out.println("response with callId:"+callId +" frame: "+response);

            Frame request = requests.map.remove(callId);
            if (request == null) {
                System.out.println("Dropping response " + response + ", invocation with id " + callId + " not found");
            } else {
                request.future.complete(response);
                request.release();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            response.release();
        }
    }

    public CompletableFuture invoke(Frame request, int partitionId) {
        if (shuttingdown) {
            throw new RuntimeException("Can't make invocation, frontend shutting down");
        }

        if (partitionId < 0) {
            throw new RuntimeException("Negative partition id not supported:" + partitionId);
        }

        Address address = nodeEngine.getPartitionService().getPartitionOwner(partitionId);
        CompletableFuture future = request.future;
        if (address.equals(thisAddress)) {
            // todo: hack with the assignment of a partition to a local cpu.
            engine.reactorForHash(partitionIdToChannel(partitionId)).schedule(request);
        } else {
            Channel channel = getConnection(address).channels[partitionIdToChannel[partitionId]];

            // we need to acquire the frame because storage will release it once written
            // and we need to keep the frame around for the response.
            request.acquire();
            Requests requests = getRequests(channel.remoteAddress);
            long callId = requests.nextCallId();
            request.putLong(Frame.OFFSET_REQUEST_CALL_ID, callId);
            //System.out.println("request.refCount:"+request.refCount());
            requests.map.put(callId, request);
            channel.writeAndFlush(request);
        }

        return future;
    }

    public void invoke(PipelineImpl pipeline) {
        if (shuttingdown) {
            throw new RuntimeException("Can't make invocation, frontend shutting down");
        }

        List<Frame> requestList = pipeline.getRequests();
        if (requestList.isEmpty()) {
            return;
        }

        int partitionId = pipeline.getPartitionId();
        Address address = nodeEngine.getPartitionService().getPartitionOwner(partitionId);
        if (address.equals(thisAddress)) {
            engine.reactorForHash(partitionId).schedule(requestList);
        } else {
            Channel channel = getConnection(address).channels[partitionIdToChannel[partitionId]];
            Requests requests = getRequests(channel.remoteAddress);

            long c = requests.nextCallId(requestList.size());

            int k = 0;
            for (Frame request : requestList) {
                request.acquire();
                long callId = c - k;
                requests.map.put(callId, request);
                request.putLong(Frame.OFFSET_REQUEST_CALL_ID, callId);
                k--;
            }

            channel.writeAll(requestList);
            channel.flush();
        }
    }

    private TcpServerConnection getConnection(Address address) {
        if (connectionManager == null) {
            connectionManager = nodeEngine.getNode().getServer().getConnectionManager(EndpointQualifier.MEMBER);
        }

        TcpServerConnection connection = (TcpServerConnection) connectionManager.get(address);
        if (connection == null) {
            connectionManager.getOrConnect(address);
            for (int k = 0; k < 60; k++) {
                try {
                    java.lang.System.out.println("Waiting for connection: " + address);
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                }
                connection = (TcpServerConnection) connectionManager.get(address);
                if (connection != null) {
                    break;
                }
            }

            if (connection == null) {
                throw new RuntimeException("Could not connect to : " + address);
            }
        }

        if (connection.channels == null) {
            synchronized (connection) {
                if (connection.channels == null) {
                    Channel[] channels = new Channel[channelCount];

                    List<SocketAddress> reactorAddresses = new ArrayList<>(channelCount);
                    List<Future<Channel>> futures = new ArrayList<>(channelCount);
                    for (int channelIndex = 0; channelIndex < channels.length; channelIndex++) {
                        SocketAddress reactorAddress = new InetSocketAddress(address.getHost(), toPort(address, channelIndex));
                        reactorAddresses.add(reactorAddress);
                        Reactor reactor = engine.reactorForHash(channelIndex);

                        Supplier<Channel> channelSupplier = (Supplier<Channel>) reactor.context.get("requestChannelSupplier");
                        Channel channel = channelSupplier.get();
                        Future<Channel> schedule = reactor.connect(channel, reactorAddress);
                        futures.add(schedule);
                    }

                    for (int channelIndex = 0; channelIndex < channels.length; channelIndex++) {
                        try {
                            channels[channelIndex] = futures.get(channelIndex).get();
                        } catch (Exception e) {
                            throw new RuntimeException("Failed to connect to :" + reactorAddresses.get(channelIndex), e);
                        }
                        //todo: assignment of the socket to the channels.
                    }

                    connection.channels = channels;
                }
            }
        }

        return connection;
    }

    public Requests getRequests(SocketAddress address) {
        // remove
        if (address == null) {
            throw new RuntimeException("Address can't be null");
        }

        Requests requests = requestsPerChannel.get(address);
        if (requests != null) {
            return requests;
        }

        Requests newRequests = new Requests();
        Requests foundRequests = requestsPerChannel.putIfAbsent(address, newRequests);
        return foundRequests == null ? newRequests : foundRequests;
    }

    public class ResponseThread extends Thread {
        public final MPSCQueue<Frame> queue;

        public ResponseThread() {
            super("ResponseThread");
            this.queue = new MPSCQueue<Frame>(this, null);
        }

        @Override
        public void run() {
            try {
                while (!shuttingdown) {
                    Frame frame;
                    if (responseThreadSpin) {
                        do {
                            frame = queue.poll();
                        } while (frame == null);
                    } else {
                        frame = queue.take();
                    }

                    do {
                        Frame next = frame.next;
                        frame.next = null;
                        handleResponse(frame);
                        frame = next;
                    } while (frame != null);
                }
            } catch (InterruptedException e) {
                System.out.println("ResponseThread stopping due to interrupt");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void shutdown() {
            interrupt();
        }
    }

    /**
     * Requests for a given member.
     */
    public class Requests {
        final ConcurrentMap<Long, Frame> map = new ConcurrentHashMap<>();
        final PaddedAtomicLong started = new PaddedAtomicLong();
        final PaddedAtomicLong completed = new PaddedAtomicLong();

        public void complete() {
            if (concurrentRequestLimit > -1) {
                completed.incrementAndGet();
            }
        }

        public long nextCallId() {
            if (concurrentRequestLimit == -1) {
                return started.incrementAndGet();
            } else {
                long endTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10);
                do {
                    if (completed.get() + concurrentRequestLimit > started.get()) {
                        return started.incrementAndGet();
                    } else {
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            throw new RuntimeException();
                        }
                    }
                } while (System.currentTimeMillis() < endTime);

                throw new RuntimeException("Member is overloaded with requests");
            }
        }

        public long nextCallId(int count) {
            if (concurrentRequestLimit == -1) {
                return started.addAndGet(count);
            } else {
                long endTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10);
                do {
                    if (completed.get() + concurrentRequestLimit > started.get() + count) {
                        return started.addAndGet(count);
                    } else {
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            throw new RuntimeException();
                        }
                    }
                } while (System.currentTimeMillis() < endTime);

                throw new RuntimeException("Member is overloaded with requests");
            }
        }
    }
}
