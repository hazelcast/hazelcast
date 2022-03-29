package com.hazelcast.spi.impl.reactor;

import com.hazelcast.cluster.Address;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.nio.PacketIOHelper;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.ByteArrayObjectDataInput;
import com.hazelcast.internal.server.ServerConnectionManager;
import com.hazelcast.internal.server.tcp.TcpServerConnection;
import com.hazelcast.internal.util.ThreadAffinity;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.reactor.nio.NioReactor;
import io.netty.incubator.channel.uring.IO_UringReactor;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.internal.util.HashUtil.hashToIndex;
import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ReactorFrontEnd {

    private final NodeEngineImpl nodeEngine;
    public final InternalSerializationService ss;
    public final ILogger logger;
    private final Address thisAddress;
    private final ThreadAffinity threadAffinity;
    private final int reactorCount;
    private final int channelCount;
    private final boolean reactorSpin;
    private volatile ServerConnectionManager connectionManager;
    public volatile boolean shuttingdown = false;
    private final Reactor[] reactors;
    public final Managers managers = new Managers();
    private final ConcurrentMap<Address, Invocations> invocationsPerMember = new ConcurrentHashMap<>();

    public ReactorFrontEnd(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(ReactorFrontEnd.class);
        this.ss = (InternalSerializationService) nodeEngine.getSerializationService();
        this.reactorCount = 1;//Integer.parseInt(System.getProperty("reactor.count", "" + Runtime.getRuntime().availableProcessors()));
        this.reactorSpin = Boolean.parseBoolean(System.getProperty("reactor.spin", "false"));
        String reactorType = System.getProperty("reactor.type", "iouring");
        this.channelCount = 1;//Integer.parseInt(System.getProperty("reactor.channels", "" + Runtime.getRuntime().availableProcessors()));
        this.threadAffinity = ThreadAffinity.newSystemThreadAffinity("reactor.cpu-affinity");
        printReactorInfo(reactorType);
        this.reactors = new Reactor[reactorCount];
        this.thisAddress = nodeEngine.getThisAddress();

        for (int reactor = 0; reactor < reactors.length; reactor++) {
            int port = toPort(thisAddress, reactor);
            if (reactorType.equals("io_uring") || reactorType.equals("iouring")) {
                reactors[reactor] = new IO_UringReactor(this, thisAddress, port, reactorSpin);
            } else if (reactorType.equals("nio")) {
                reactors[reactor] = new NioReactor(this, thisAddress, port, reactorSpin);
            } else {
                throw new RuntimeException("Unrecognized 'reactor.type' " + reactorType);
            }
            reactors[reactor].setThreadAffinity(threadAffinity);
        }
    }

    private void printReactorInfo(String reactorType) {
        logger.info("reactor.count:" + reactorCount);
        logger.info("reactor.spin:" + reactorSpin);
        logger.info("reactor.channels:" + channelCount);
        logger.info("reactor.type:" + reactorType);
        logger.info("reactor.cpu-affinity:"+System.getProperty("reactor.cpu-affinity"));
    }

    public int toPort(Address address, int cpu) {
        return (address.getPort() - 5701) * 100 + 11000 + cpu;
    }

    public int partitionIdToCpu(int partitionId) {
        return hashToIndex(partitionId, reactors.length);
    }

    public void start() {
        logger.info("Starting ReactorFrontend");

        for (Reactor r : reactors) {
            r.start();
        }
    }

    public void shutdown() {
        logger.info("Shutting down ReactorFrontend");

        shuttingdown = true;

        for (Invocations invocations : invocationsPerMember.values()) {
            for (Invocation i : invocations.map.values()) {
                i.completableFuture.completeExceptionally(new RuntimeException("Shutting down"));
            }
        }
    }

    public CompletableFuture invoke(Request request) {
        if (shuttingdown) {
            throw new RuntimeException("Can't make invocation, frontend shutting down");
        }

        try {
            int partitionId = request.partitionId;

            if (partitionId >= 0) {
                Address address = nodeEngine.getPartitionService().getPartitionOwner(partitionId);
                Invocations invocations = getInvocations(address);
                Invocation invocation = new Invocation();
                invocation.callId = invocations.counter.incrementAndGet();
                request.out.writeLong(Request.OFFSET_CALL_ID, invocation.callId);
                invocation.request = request;
                request.invocation = invocation;
                invocations.map.put(invocation.callId, invocation);

                if (address.equals(thisAddress)) {
                    //System.out.println("local invoke");
                    reactors[partitionIdToCpu(partitionId)].enqueue(request);
                } else {
                    //System.out.println("remove invoke");
                    TcpServerConnection connection = getConnection(address);
                    Channel channel = connection.channels[partitionIdToCpu(partitionId)];

                    Packet packet = request.toPacket();
                    ByteBuffer buffer = ByteBuffer.allocate(packet.totalSize() + 30);
                    new PacketIOHelper().writeTo(packet, buffer);
                    buffer.flip();
                    channel.writeAndFlush(buffer);
                }

                return invocation.completableFuture;
            } else {
                throw new RuntimeException("Negative partition id not supported:" + partitionId);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (shuttingdown) {
                throw new RuntimeException("Can't make invocation, frontend shutting down");
            }
        }
    }

    private TcpServerConnection getConnection(Address address) {
        if (connectionManager == null) {
            connectionManager = nodeEngine.getNode().getServer().getConnectionManager(EndpointQualifier.MEMBER);
        }

        TcpServerConnection connection = (TcpServerConnection) connectionManager.get(address);
        if (connection == null) {
            connectionManager.getOrConnect(address);
            try {
                if (!connectionManager.blockOnConnect(thisAddress, SECONDS.toMillis(10), 0)) {
                    throw new RuntimeException("Failed to connect to:" + address);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
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
                        futures.add(reactors[hashToIndex(channelIndex, reactors.length)].enqueue(reactorAddress, connection));
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

    public Invocations getInvocations(Address address) {
        Invocations invocations = invocationsPerMember.get(address);
        if (invocations != null) {
            return invocations;
        }

        Invocations newInvocations = new Invocations();
        Invocations foundInvocations = invocationsPerMember.putIfAbsent(address, newInvocations);
        return foundInvocations == null ? newInvocations : foundInvocations;
    }

    private class Invocations {
        private final ConcurrentMap<Long, Invocation> map = new ConcurrentHashMap<>();
        private final AtomicLong counter = new AtomicLong(500);
    }

    public void handleResponse(Packet packet) {
        try {
            Address remoteAddress = packet.getConn().getRemoteAddress();
            Invocations targetInvocations = invocationsPerMember.get(remoteAddress);
            if (targetInvocations == null) {
                System.out.println("Dropping response " + packet + ", targetInvocations not found");
                return;
            }

            ByteArrayObjectDataInput in = new ByteArrayObjectDataInput(packet.toByteArray(), ss, BIG_ENDIAN);

            long callId = in.readLong();
            Invocation invocation = targetInvocations.map.remove(callId);
            if (invocation == null) {
                System.out.println("Dropping response " + packet + ", invocation not found");
            } else {
                invocation.completableFuture.complete(null);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
