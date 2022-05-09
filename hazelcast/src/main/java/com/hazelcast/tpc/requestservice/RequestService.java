package com.hazelcast.tpc.requestservice;

import com.hazelcast.cluster.Address;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.server.ServerConnectionManager;
import com.hazelcast.internal.server.tcp.TcpServerConnection;
import com.hazelcast.internal.util.concurrent.MPSCQueue;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.tpc.engine.AsyncSocket;
import com.hazelcast.tpc.engine.Engine;
import com.hazelcast.tpc.engine.Eventloop;
import com.hazelcast.tpc.engine.EventloopType;
import com.hazelcast.tpc.engine.AsyncSocketReadHandler;
import com.hazelcast.tpc.engine.epoll.EpollAsyncServerSocket;
import com.hazelcast.tpc.engine.epoll.EpollEventloop;
import com.hazelcast.tpc.engine.epoll.EpollReadHandler;
import com.hazelcast.tpc.engine.frame.ParallelFrameAllocator;
import com.hazelcast.tpc.engine.frame.Frame;
import com.hazelcast.tpc.engine.frame.FrameAllocator;
import com.hazelcast.tpc.engine.frame.SerialFrameAllocator;
import com.hazelcast.tpc.engine.frame.UnpooledFrameAllocator;
import com.hazelcast.tpc.engine.nio.NioAsyncServerSocket;
import com.hazelcast.tpc.engine.nio.NioAsyncSocket;
import com.hazelcast.tpc.engine.nio.NioEventloop;
import com.hazelcast.tpc.engine.nio.NioReadHandler;
import com.hazelcast.table.impl.PipelineImpl;
import com.hazelcast.table.impl.TableManager;
import com.hazelcast.tpc.engine.epoll.EpollAsyncSocket;
import com.hazelcast.tpc.engine.iouring.IOUringAsyncServerSocket;
import com.hazelcast.tpc.engine.iouring.IOUringAsyncSocket;
import com.hazelcast.tpc.engine.iouring.IOUringEventloop;
import com.hazelcast.tpc.engine.iouring.IOUringReadHandler;
import org.jctools.util.PaddedAtomicLong;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static com.hazelcast.internal.util.HashUtil.hashToIndex;
import static com.hazelcast.tpc.engine.frame.Frame.OFFSET_REQ_CALL_ID;
import static com.hazelcast.tpc.engine.frame.Frame.OFFSET_RES_CALL_ID;


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
    private final int socketCount;
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
    private final Requests localRequest = new Requests();
    private final ResponseThread[] responseThreads;
    private int[] partitionIdToChannel;
    private Engine engine;
    private int concurrentRequestLimit;
    private final Map<Eventloop, Supplier<? extends AsyncSocketReadHandler>> readHandlerSuppliers = new HashMap<>();

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

        this.socketCount = Integer.parseInt(java.lang.System.getProperty("reactor.channels", "" + Runtime.getRuntime().availableProcessors()));
        printEventloopInfo();
        this.thisAddress = nodeEngine.getThisAddress();
        this.engine = newEngine();

        this.socketConfig = new SocketConfig();
        this.managers = new Managers();
        //hack
        managers.tableManager = new TableManager(271);

        this.partitionIdToChannel = new int[271];
        for (int k = 0; k < 271; k++) {
            partitionIdToChannel[k] = hashToIndex(k, socketCount);
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
    private Engine newEngine() {
        Engine engine = new Engine(() -> {
            FrameAllocator remoteResponseFrameAllocator = new ParallelFrameAllocator(128, true);
            FrameAllocator localResponseFrameAllocator = new SerialFrameAllocator(128, true);

            return new OpScheduler(32768,
                    Integer.MAX_VALUE,
                    managers,
                    localResponseFrameAllocator,
                    remoteResponseFrameAllocator);
        });
        engine.setEventloopBasename("Eventloop:[" + thisAddress.getHost() + ":" + thisAddress.getPort() + "]:");
        engine.createEventLoops();
        engine.printConfig();
        return engine;
    }

    private void configureNio() {
        for (int k = 0; k < engine.eventloopCount(); k++) {
            NioEventloop eventloop = (NioEventloop) engine.eventloop(k);

            Supplier<NioReadHandler> readHandlerSupplier = () -> {
                RequestNioReadHandler readHandler = new RequestNioReadHandler();
                readHandler.opScheduler = (OpScheduler) eventloop.getScheduler();
                readHandler.requestService = RequestService.this;
                readHandler.requestFrameAllocator = poolRequests
                        ? new SerialFrameAllocator(128, true)
                        : new UnpooledFrameAllocator();
                readHandler.remoteResponseFrameAllocator = poolRemoteResponses
                        ? new ParallelFrameAllocator(128, true)
                        : new UnpooledFrameAllocator();
                return readHandler;
            };
            readHandlerSuppliers.put(eventloop, readHandlerSupplier);

            try {
                int port = toPort(thisAddress, k);
                NioAsyncServerSocket serverSocket = NioAsyncServerSocket.open(eventloop);
                serverSocket.setReceiveBufferSize(socketConfig.receiveBufferSize);
                serverSocket.setReuseAddress(true);
                serverSocket.bind(new InetSocketAddress(thisAddress.getInetAddress(), port));
                serverSocket.accept(socket -> {
                    socket.setReadHandler(readHandlerSuppliers.get(eventloop).get());
                    socket.setWriteThrough(writeThrough);
                    socket.setRegularSchedule(regularSchedule);
                    socket.setSendBufferSize(socketConfig.sendBufferSize);
                    socket.setReceiveBufferSize(socketConfig.receiveBufferSize);
                    socket.setTcpNoDelay(socketConfig.tcpNoDelay);
                    socket.activate(eventloop);
                });
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }


    private void configureIOUring() {
        for (int k = 0; k < engine.eventloopCount(); k++) {
            IOUringEventloop eventloop = (IOUringEventloop) engine.eventloop(k);
            try {
                Supplier<IOUringReadHandler> readHandlerSupplier = () -> {
                    RequestIOUringReadHandler handler = new RequestIOUringReadHandler();
                    handler.opScheduler = (OpScheduler) eventloop.getScheduler();
                    handler.requestService = this;
                    handler.requestFrameAllocator = poolRequests
                            ? new SerialFrameAllocator(128, true)
                            : new UnpooledFrameAllocator();
                    handler.remoteResponseFrameAllocator = poolRemoteResponses
                            ? new ParallelFrameAllocator(128, true)
                            : new UnpooledFrameAllocator();
                    return handler;
                };
                readHandlerSuppliers.put(eventloop, readHandlerSupplier);

                int port = toPort(thisAddress, k);

                IOUringAsyncServerSocket serverSocket = IOUringAsyncServerSocket.open(eventloop);
                serverSocket.setReceiveBufferSize(socketConfig.receiveBufferSize);
                serverSocket.setReuseAddress(true);
                serverSocket.bind(new InetSocketAddress(thisAddress.getInetAddress(), port));
                serverSocket.listen(10);
                serverSocket.accept(socket -> {
                    socket.setReadHandler(readHandlerSuppliers.get(eventloop).get());
                    socket.setSendBufferSize(socketConfig.sendBufferSize);
                    socket.setReceiveBufferSize(socketConfig.receiveBufferSize);
                    socket.setTcpNoDelay(socketConfig.tcpNoDelay);
                    socket.activate(eventloop);
                });
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private void configureEpoll() {
        for (int k = 0; k < engine.eventloopCount(); k++) {
            EpollEventloop eventloop = (EpollEventloop) engine.eventloop(k);
            try {
                Supplier<EpollReadHandler> readHandlerSupplier = () -> {
                    RequestEpollReadHandler handler = new RequestEpollReadHandler();
                    handler.opScheduler = (OpScheduler) eventloop.getScheduler();
                    handler.requestService = this;
                    handler.requestFrameAllocator = poolRequests
                            ? new SerialFrameAllocator(128, true)
                            : new UnpooledFrameAllocator();
                    handler.remoteResponseFrameAllocator = poolRemoteResponses
                            ? new ParallelFrameAllocator(128, true)
                            : new UnpooledFrameAllocator();
                    return handler;
                };
                readHandlerSuppliers.put(eventloop, readHandlerSupplier);

                int port = toPort(thisAddress, k);

                EpollAsyncServerSocket serverSocket = EpollAsyncServerSocket.open(eventloop);
                serverSocket.setReceiveBufferSize(socketConfig.receiveBufferSize);
                serverSocket.setReuseAddress(true);
                serverSocket.bind(new InetSocketAddress(thisAddress.getInetAddress(), port));
                serverSocket.listen(10);
                serverSocket.accept(socket -> {
                    socket.setReadHandler(readHandlerSuppliers.get(eventloop).get());
                    socket.setSendBufferSize(socketConfig.sendBufferSize);
                    socket.setReceiveBufferSize(socketConfig.receiveBufferSize);
                    socket.setTcpNoDelay(socketConfig.tcpNoDelay);
                    socket.activate(eventloop);
                });
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private void printEventloopInfo() {
        java.lang.System.out.println("reactor.responsethread.count:" + responseThreadCount);
        java.lang.System.out.println("reactor.write-through:" + writeThrough);
        java.lang.System.out.println("reactor.channels:" + socketCount);
        java.lang.System.out.println("reactor.pool-requests:" + poolRequests);
        java.lang.System.out.println("reactor.pool-local-responses:" + poolLocalResponses);
        java.lang.System.out.println("reactor.pool-remote-responses:" + poolRemoteResponses);
        java.lang.System.out.println("reactor.cpu-affinity:" + java.lang.System.getProperty("reactor.cpu-affinity"));
    }

    public int toPort(Address address, int eventloopIdx) {
        return (address.getPort() - 5701) * 100 + 11000 + eventloopIdx;
    }

    public int partitionIdToChannel(int partitionId) {
        return hashToIndex(partitionId, socketCount);
    }

    public void start() {
        logger.info("Starting RequestService");
        engine.start();

        EventloopType eventloopType = engine.getEventloopType();
        switch (eventloopType) {
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

    private void ensureActive() {
        if (shuttingdown) {
            throw new RuntimeException("Can't make invocation, frontend shutting down");
        }
    }

    public void shutdown() {
        logger.info("Shutting down RequestService");

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
            int index = responseThreadCount == 0
                    ? 0
                    : hashToIndex(response.getLong(OFFSET_RES_CALL_ID), responseThreadCount);
            responseThreads[index].queue.add(response);
            return;
        }

        try {
            Requests requests = requestsPerChannel.get(response.socket.getRemoteAddress());
            if (requests == null) {
                System.out.println("Dropping response " + response + ", requests not found");
                response.release();
            } else {
                requests.complete();

                long callId = response.getLong(OFFSET_RES_CALL_ID);
                //System.out.println("response with callId:"+callId +" frame: "+response);

                Frame request = requests.map.remove(callId);
                if (request == null) {
                    System.out.println("Dropping response " + response + ", invocation with id " + callId + " not found");
                } else {
                    CompletableFuture future = request.future;
                    future.complete(response);
                    request.release();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public CompletableFuture invokeOnEventloop(Frame request, Address address, int eventloop) {
        ensureActive();

        CompletableFuture future = request.future;
        if (address.equals(thisAddress)) {
            engine.eventloop(eventloop).execute(request);
        } else {
            AsyncSocket socket = getConnection(address).sockets[eventloop];

            // we need to acquire the frame because storage will release it once written
            // and we need to keep the frame around for the response.
            request.acquire();
            Requests requests = getRequests(socket.getRemoteAddress());
            long callId = requests.nextCallId();
            request.putLong(OFFSET_REQ_CALL_ID, callId);
            //System.out.println("request.refCount:"+request.refCount());
            requests.map.put(callId, request);
            socket.writeAndFlush(request);
        }

        return future;
    }

    public CompletableFuture invokeOnPartition(Frame request, int partitionId) {
        ensureActive();

        if (partitionId < 0) {
            throw new RuntimeException("Negative partition id not supported:" + partitionId);
        }

        Address address = nodeEngine.getPartitionService().getPartitionOwner(partitionId);
        CompletableFuture future = request.future;
        if (address.equals(thisAddress)) {
            // todo: hack with the assignment of a partition to a local cpu.
            engine.eventloopForHash(partitionIdToChannel(partitionId)).execute(request);
        } else {
            AsyncSocket socket = getConnection(address).sockets[partitionIdToChannel[partitionId]];

            // we need to acquire the frame because storage will release it once written
            // and we need to keep the frame around for the response.
            request.acquire();
            Requests requests = getRequests(socket.getRemoteAddress());
            long callId = requests.nextCallId();
            request.putLong(OFFSET_REQ_CALL_ID, callId);
            //System.out.println("request.refCount:"+request.refCount());
            requests.map.put(callId, request);
            socket.writeAndFlush(request);
        }

        return future;
    }

    public CompletableFuture invoke(Frame request, AsyncSocket socket) {
        ensureActive();

        CompletableFuture future = request.future;
        // we need to acquire the frame because storage will release it once written
        // and we need to keep the frame around for the response.
        request.acquire();
        Requests requests = getRequests(socket.getRemoteAddress());
        long callId = requests.nextCallId();
        request.putLong(OFFSET_REQ_CALL_ID, callId);
        //System.out.println("request.refCount:"+request.refCount());
        requests.map.put(callId, request);
        socket.writeAndFlush(request);
        return future;
    }

    public void invokeOnPartition(PipelineImpl pipeline) {
        ensureActive();

        List<Frame> requestList = pipeline.getRequests();
        if (requestList.isEmpty()) {
            return;
        }

        int partitionId = pipeline.getPartitionId();
        Address address = nodeEngine.getPartitionService().getPartitionOwner(partitionId);
        if (address.equals(thisAddress)) {
            engine.eventloopForHash(partitionId).execute(requestList);
        } else {
            AsyncSocket socket = getConnection(address).sockets[partitionIdToChannel[partitionId]];
            Requests requests = getRequests(socket.getRemoteAddress());

            long c = requests.nextCallId(requestList.size());

            int k = 0;
            for (Frame request : requestList) {
                request.acquire();
                long callId = c - k;
                requests.map.put(callId, request);
                request.putLong(OFFSET_REQ_CALL_ID, callId);
                k--;
            }

            socket.writeAll(requestList);
            socket.flush();
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

        if (connection.sockets == null) {
            synchronized (connection) {
                if (connection.sockets == null) {
                    AsyncSocket[] sockets = new AsyncSocket[socketCount];

                    for (int socketIndex = 0; socketIndex < sockets.length; socketIndex++) {
                        SocketAddress eventloopAddress = new InetSocketAddress(address.getHost(), toPort(address, socketIndex));
                        sockets[socketIndex] = connect(eventloopAddress, socketIndex);
                    }

                    connection.sockets = sockets;
                }
            }
        }

        return connection;
    }

    public AsyncSocket connect(SocketAddress address, int channelIndex) {
        Eventloop eventloop = engine.eventloopForHash(channelIndex);

        AsyncSocket socket;
        switch (engine.getEventloopType()) {
            case NIO:
                NioAsyncSocket nioSocket = NioAsyncSocket.open();
                nioSocket.setWriteThrough(writeThrough);
                nioSocket.setRegularSchedule(regularSchedule);
                socket = nioSocket;
                break;
            case IOURING:
                socket = IOUringAsyncSocket.open();
                break;
            case EPOLL:
                socket = EpollAsyncSocket.open();
                break;
            default:
                throw new RuntimeException();
        }

        socket.setReadHandler(readHandlerSuppliers.get(eventloop).get());
        socket.setSendBufferSize(socketConfig.sendBufferSize);
        socket.setReceiveBufferSize(socketConfig.receiveBufferSize);
        socket.setTcpNoDelay(socketConfig.tcpNoDelay);
        socket.activate(eventloop);

        CompletableFuture future = socket.connect(address);
        future.join();
        System.out.println("AsyncSocket " + address + " connected");
        return socket;
    }

    public Requests getRequests(SocketAddress address) {
        Requests requests = requestsPerChannel.get(address);
        if (requests == null) {
            Requests newRequests = new Requests();
            Requests foundRequests = requestsPerChannel.putIfAbsent(address, newRequests);
            return foundRequests == null ? newRequests : foundRequests;
        } else {
            return requests;
        }
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
