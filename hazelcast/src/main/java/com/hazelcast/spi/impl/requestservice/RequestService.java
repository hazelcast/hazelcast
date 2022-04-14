package com.hazelcast.spi.impl.requestservice;

import com.hazelcast.cluster.Address;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.server.ServerConnectionManager;
import com.hazelcast.internal.server.tcp.TcpServerConnection;
import com.hazelcast.internal.util.ThreadAffinity;
import com.hazelcast.internal.util.concurrent.MPSCQueue;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.reactor.Channel;
import com.hazelcast.spi.impl.reactor.Reactor;
import com.hazelcast.spi.impl.reactor.ReactorMonitorThread;
import com.hazelcast.spi.impl.reactor.SocketConfig;
import com.hazelcast.spi.impl.reactor.frame.ConcurrentPooledFrameAllocator;
import com.hazelcast.spi.impl.reactor.frame.Frame;
import com.hazelcast.spi.impl.reactor.frame.FrameAllocator;
import com.hazelcast.spi.impl.reactor.frame.NonConcurrentPooledFrameAllocator;
import com.hazelcast.spi.impl.reactor.frame.UnpooledFrameAllocator;
import com.hazelcast.spi.impl.reactor.nio.NioReactor;
import com.hazelcast.spi.impl.reactor.nio.NioReactorConfig;
import com.hazelcast.spi.impl.reactor.nio.NioServerChannel;
import com.hazelcast.table.impl.PipelineImpl;
import com.hazelcast.table.impl.TableManager;
import io.netty.channel.epoll.EpollReactor;
import io.netty.channel.epoll.EpollReactorConfig;
import io.netty.channel.epoll.EpollServerChannel;
import io.netty.incubator.channel.uring.IOUringReactor;
import io.netty.incubator.channel.uring.IOUringReactorConfig;
import io.netty.incubator.channel.uring.IOUringServerChannel;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;

import static com.hazelcast.internal.util.HashUtil.hashToIndex;
import static com.hazelcast.spi.impl.reactor.frame.Frame.OFFSET_RESPONSE_CALL_ID;


/**
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
    private final ThreadAffinity threadAffinity;
    private final int reactorCount;
    private final int channelCount;
    private final boolean reactorSpin;
    private final SocketConfig socketConfig;
    private final ReactorMonitorThread monitorThread;
    private final boolean poolRequests;
    private final boolean poolLocalResponses;
    private final boolean poolRemoteResponses;
    private final boolean writeThrough;
    private final int responseThreadCount;
    private final boolean monitorSilent;
    private final String reactorType;
    private volatile ServerConnectionManager connectionManager;
    public volatile boolean shuttingdown = false;
    private final Reactor[] reactors;
    public final Managers managers;
    private final ConcurrentMap<Address, Requests> requestsPerMember = new ConcurrentHashMap<>();
    private final ResponseThread[] responseThreads;
    private int[] partitionIdToChannel;

    public RequestService(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(RequestService.class);
        this.ss = (InternalSerializationService) nodeEngine.getSerializationService();
        this.reactorCount = Integer.parseInt(System.getProperty("reactor.count", "" + Runtime.getRuntime().availableProcessors()));
        this.responseThreadCount = Integer.parseInt(System.getProperty("reactor.responsethread.count", "1"));
        this.reactorSpin = Boolean.parseBoolean(System.getProperty("reactor.spin", "false"));
        this.writeThrough = Boolean.parseBoolean(System.getProperty("reactor.write-through", "false"));
        this.poolRequests = Boolean.parseBoolean(System.getProperty("reactor.pool-requests", "true"));
        this.poolLocalResponses = Boolean.parseBoolean(System.getProperty("reactor.pool-local-responses", "true"));
        this.poolRemoteResponses = Boolean.parseBoolean(System.getProperty("reactor.pool-remote-responses", "false"));
        this.reactorType = System.getProperty("reactor.type", "nio");
        this.monitorSilent = Boolean.parseBoolean(System.getProperty("reactor.monitor.silent", "false"));

        this.channelCount = Integer.parseInt(System.getProperty("reactor.channels", "" + Runtime.getRuntime().availableProcessors()));
        this.threadAffinity = ThreadAffinity.newSystemThreadAffinity("reactor.cpu-affinity");
        printReactorInfo();
        this.reactors = new Reactor[reactorCount];
        this.thisAddress = nodeEngine.getThisAddress();
        this.socketConfig = new SocketConfig();
        this.managers = new Managers();
        //hack
        managers.tableManager = new TableManager(271);

        this.partitionIdToChannel = new int[271];
        for (int k = 0; k < 271; k++) {
            partitionIdToChannel[k] = hashToIndex(k, channelCount);
        }

        for (int k = 0; k < reactors.length; k++) {
            int port = toPort(thisAddress, k);
            if (reactorType.equals("io_uring") || reactorType.equals("iouring")) {
                reactors[k] = newIO_UringReactor(port);
            } else if (reactorType.equals("nio")) {
                reactors[k] = newNioReactor(port);
            } else if (reactorType.equals("epoll")) {
                reactors[k] = newEpollReactor(port);
            } else {
                throw new RuntimeException("Unrecognized 'reactor.type' " + reactorType);
            }
        }

        this.monitorThread = new ReactorMonitorThread(reactors, monitorSilent);
        this.responseThreads = new ResponseThread[responseThreadCount];
        for (int k = 0; k < responseThreadCount; k++) {
            this.responseThreads[k] = new ResponseThread();
        }
    }

    @NotNull
    private NioReactor newNioReactor(int port) {
        try {
            NioReactorConfig config = new NioReactorConfig();
            OpScheduler scheduler = newOpScheduler();
            config.scheduler = scheduler;
            config.spin = reactorSpin;
            config.writeThrough = writeThrough;
            config.name = "NioReactor:[" + thisAddress.getHost() + ":" + thisAddress.getPort() + "]:" + port;
            config.logger = nodeEngine.getLogger(NioReactor.class);
            config.threadAffinity = threadAffinity;
            NioReactor reactor = new NioReactor(config);

            NioServerChannel serverChannel = new NioServerChannel();
            serverChannel.socketConfig = socketConfig;
            serverChannel.address = new InetSocketAddress(thisAddress.getInetAddress(), port);

            serverChannel.channelSupplier = () -> {
                RequestNioChannel channel = new RequestNioChannel();
                channel.opScheduler = scheduler;
                channel.requestService = this;
                channel.requestFrameAllocator = poolRequests
                        ? new NonConcurrentPooledFrameAllocator(128, true)
                        : new UnpooledFrameAllocator();
                channel.remoteResponseFrameAllocator = poolRemoteResponses
                        ? new ConcurrentPooledFrameAllocator(128, true)
                        : new UnpooledFrameAllocator();
                return channel;
            };

            reactor.accept(serverChannel);
            return reactor;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @NotNull
    private IOUringReactor newIO_UringReactor(int port) {
        try {
            IOUringReactorConfig config = new IOUringReactorConfig();
            OpScheduler scheduler = newOpScheduler();
            config.scheduler = scheduler;
            config.spin = reactorSpin;
            config.name = "IO_UringReactor:[" + thisAddress.getHost() + ":" + thisAddress.getPort() + "]:" + port;
            config.logger = nodeEngine.getLogger(IOUringReactor.class);
            config.threadAffinity = threadAffinity;
            IOUringReactor reactor = new IOUringReactor(config);

            IOUringServerChannel serverChannel = new IOUringServerChannel();
            serverChannel.socketConfig = socketConfig;
            serverChannel.address = new InetSocketAddress(thisAddress.getInetAddress(), port);

            serverChannel.channelSupplier = () -> {
                RequestIOUringChannel channel = new RequestIOUringChannel();
                channel.opScheduler = scheduler;
                channel.requestService = this;
                channel.requestFrameAllocator = poolRequests
                        ? new NonConcurrentPooledFrameAllocator(128, true)
                        : new UnpooledFrameAllocator();
                channel.remoteResponseFrameAllocator = poolRemoteResponses
                        ? new ConcurrentPooledFrameAllocator(128, true)
                        : new UnpooledFrameAllocator();
                return channel;
            };
            reactor.register(serverChannel);
            return reactor;
        } catch (IOException e) {
            throw new RuntimeException();
        }
    }

    @NotNull
    private EpollReactor newEpollReactor(int port) {
        try {
            EpollReactorConfig config = new EpollReactorConfig();
            OpScheduler scheduler = newOpScheduler();
            config.scheduler = scheduler;
            config.spin = reactorSpin;
            config.name = "EpollReactor:[" + thisAddress.getHost() + ":" + thisAddress.getPort() + "]:" + port;
            config.logger = nodeEngine.getLogger(IOUringReactor.class);
            config.threadAffinity = threadAffinity;
            EpollReactor reactor = new EpollReactor(config);

            EpollServerChannel serverChannel = new EpollServerChannel();
            serverChannel.socketConfig = socketConfig;
            serverChannel.address = new InetSocketAddress(thisAddress.getInetAddress(), port);
            serverChannel.channelSupplier = () -> {
                RequestEpollChannel channel = new RequestEpollChannel();
                channel.opScheduler = scheduler;
                channel.requestService = this;
                channel.requestFrameAllocator = poolRequests
                        ? new NonConcurrentPooledFrameAllocator(128, true)
                        : new UnpooledFrameAllocator();
                channel.remoteResponseFrameAllocator = poolRemoteResponses
                        ? new ConcurrentPooledFrameAllocator(128, true)
                        : new UnpooledFrameAllocator();
                return channel;
            };

            reactor.register(serverChannel);
            return reactor;
        } catch (IOException e) {
            throw new RuntimeException();
        }
    }

    private OpScheduler newOpScheduler() {
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
    }

    private void printReactorInfo() {
        System.out.println("reactor.count:" + reactorCount);
        System.out.println("reactor.spin:" + reactorSpin);
        System.out.println("reactor.responsethread.count:" + responseThreadCount);
        System.out.println("reactor.write-through:" + writeThrough);
        System.out.println("reactor.channels:" + channelCount);
        System.out.println("reactor.type:" + reactorType);
        System.out.println("reactor.pool-requests:" + poolRequests);
        System.out.println("reactor.pool-local-responses:" + poolLocalResponses);
        System.out.println("reactor.pool-remote-responses:" + poolRemoteResponses);
        System.out.println("reactor.cpu-affinity:" + System.getProperty("reactor.cpu-affinity"));
    }

    public int toPort(Address address, int cpu) {
        return (address.getPort() - 5701) * 100 + 11000 + cpu;
    }

    public int partitionIdToChannel(int partitionId) {
        return hashToIndex(partitionId, channelCount);
    }

    public void start() {
        logger.info("Starting ReactorFrontend");

        for (Reactor reactor : reactors) {
            reactor.start();
        }

        monitorThread.start();

        for (ResponseThread responseThread : responseThreads) {
            responseThread.start();
        }
    }

    public void shutdown() {
        logger.info("Shutting down ReactorFrontend");

        shuttingdown = true;

        for (Reactor r : reactors) {
            r.shutdown();
        }

        for (Requests requests : requestsPerMember.values()) {
            for (Frame request : requests.map.values()) {
                request.future.completeExceptionally(new RuntimeException("Shutting down"));
            }
        }

        for (ResponseThread responseThread : responseThreads) {
            responseThread.shutdown();
        }
        monitorThread.shutdown();
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
            Address remoteAddress = null;//response.connection.getRemoteAddress();
            Requests requests = requestsPerMember.get(remoteAddress);
            if (requests == null) {
                System.out.println("Dropping response " + response + ", requests not found");
                return;
            }

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
            //System.out.println("invoke:local");
            //System.out.println("local invoke");
            // todo: hack with the assignment of a partition to a local cpu.
            reactors[partitionIdToChannel(partitionId) % reactorCount].schedule(request);
        } else {
            // we need to acquire the frame because storage will release it once written
            // and we need to keep the frame around for the response.
            request.acquire();
            Requests requests = getRequests(address);
            long callId = requests.callId.incrementAndGet();
            request.putLong(Frame.OFFSET_REQUEST_CALL_ID, callId);
            //System.out.println("request.refCount:"+request.refCount());
            requests.map.put(callId, request);
            Channel channel = getConnection(address).channels[partitionIdToChannel[partitionId]];
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
            for (Frame request : requestList) {
                reactors[partitionIdToChannel(partitionId) % reactorCount].schedule(request);
            }
        } else {
            Requests requests = getRequests(address);
            TcpServerConnection connection = getConnection(address);
            Channel channel = connection.channels[partitionIdToChannel[partitionId]];

            long c = requests.callId.addAndGet(requestList.size());

            int k = 0;
            for (Frame request : requestList) {
                long callId = c - k;
                requests.map.put(callId, request);
                request.putLong(Frame.OFFSET_REQUEST_CALL_ID, callId);
                channel.write(request);
                k--;
            }

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
                    System.out.println("Waiting for connection: " + address);
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
                        Reactor reactor = reactors[hashToIndex(channelIndex, reactors.length)];

                        Channel channel;
                        if (reactorType.equals("io_uring") || reactorType.equals("iouring")) {
                            RequestIOUringChannel c = new RequestIOUringChannel();
                            c.requestFrameAllocator = null;
                            c.remoteResponseFrameAllocator = null;
                            c.opScheduler = null;
                            c.requestService = this;
                            channel = c;
                        } else if (reactorType.equals("nio")) {
                            RequestNioChannel c = new RequestNioChannel();
                            c.requestFrameAllocator = null;
                            c.remoteResponseFrameAllocator = null;
                            c.opScheduler = null;
                            c.requestService = this;
                            channel = c;
                        } else if (reactorType.equals("epoll")) {
                            RequestEpollChannel c = new RequestEpollChannel();
                            c.requestFrameAllocator = null;
                            c.remoteResponseFrameAllocator = null;
                            c.opScheduler = null;
                            c.requestService = this;
                            channel = c;
                        } else {
                            throw new RuntimeException("Unrecognized 'reactor.type' " + reactorType);
                        }
                        channel.config = socketConfig;

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
                    System.out.println("channels to " + address + " established");
                }
            }
        }

        return connection;
    }

    public Requests getRequests(Address address) {
        Requests requests = requestsPerMember.get(address);
        if (requests != null) {
            return requests;
        }

        Requests newRequests = new Requests();
        Requests foundRequests = requestsPerMember.putIfAbsent(address, newRequests);
        return foundRequests == null ? newRequests : foundRequests;
    }

    public class ResponseThread extends Thread {
        public final MPSCQueue<Frame> queue;

        public ResponseThread() {
            super("ResponseThread");
            this.queue = new MPSCQueue<>(this, null);
        }

        @Override
        public void run() {
            try {
                while (!shuttingdown) {
                    Frame frame = queue.take();
                    do {
                        Frame next = frame.next;
                        frame.next = null;
                        handleResponse(frame);
                        frame = next;
                    } while (frame != null);
                }
            } catch (InterruptedException e) {
                // ignore
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void shutdown() {
            interrupt();
        }
    }
}
