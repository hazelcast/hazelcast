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
import com.hazelcast.table.impl.TableManager;
import io.netty.incubator.channel.uring.IO_UringReactor;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.internal.util.HashUtil.hashToIndex;
import static java.nio.ByteOrder.BIG_ENDIAN;

public class ReactorFrontEnd {

    public final NodeEngineImpl nodeEngine;
    public final InternalSerializationService ss;
    public final ILogger logger;
    private final Address thisAddress;
    private final ThreadAffinity threadAffinity;
    private final int reactorCount;
    private final int channelCount;
    private final boolean reactorSpin;
    private final ChannelConfig channelConfig;
    private final MonitorThread monitorThread;
    private volatile ServerConnectionManager connectionManager;
    public volatile boolean shuttingdown = false;
    private final Reactor[] reactors;
    public final Managers managers;
    private final ConcurrentMap<Address, Invocations> invocationsPerMember = new ConcurrentHashMap<>();
    public ResponseThread responseThread;

    public ReactorFrontEnd(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(ReactorFrontEnd.class);
        this.ss = (InternalSerializationService) nodeEngine.getSerializationService();
        this.reactorCount = Integer.parseInt(System.getProperty("reactor.count", "" + Runtime.getRuntime().availableProcessors()));
        this.reactorSpin = Boolean.parseBoolean(System.getProperty("reactor.spin", "false"));
        String reactorType = System.getProperty("reactor.type", "nio");
        this.channelCount = Integer.parseInt(System.getProperty("reactor.channels", "" + Runtime.getRuntime().availableProcessors()));
        this.threadAffinity = ThreadAffinity.newSystemThreadAffinity("reactor.cpu-affinity");
        printReactorInfo(reactorType);
        this.reactors = new Reactor[reactorCount];
        this.thisAddress = nodeEngine.getThisAddress();
        this.channelConfig = new ChannelConfig();
        this.managers = new Managers();
        //hack
        managers.tableManager = new TableManager(271);


        for (int reactor = 0; reactor < reactors.length; reactor++) {
            int port = toPort(thisAddress, reactor);
            if (reactorType.equals("io_uring") || reactorType.equals("iouring")) {
                reactors[reactor] = new IO_UringReactor(this, channelConfig, thisAddress, port, reactorSpin);
            } else if (reactorType.equals("nio")) {
                reactors[reactor] = new NioReactor(this, channelConfig, thisAddress, port, reactorSpin);
            } else {
                throw new RuntimeException("Unrecognized 'reactor.type' " + reactorType);
            }
            reactors[reactor].setThreadAffinity(threadAffinity);
        }

        this.monitorThread = new MonitorThread(reactors);
        this.responseThread = new ResponseThread();
    }

    private void printReactorInfo(String reactorType) {
        System.out.println("reactor.count:" + reactorCount);
        System.out.println("reactor.spin:" + reactorSpin);
        System.out.println("reactor.channels:" + channelCount);
        System.out.println("reactor.type:" + reactorType);
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

        for (Reactor r : reactors) {
            r.start();
        }

        monitorThread.start();
        responseThread.start();
    }

    public void shutdown() {
        logger.info("Shutting down ReactorFrontend");

        shuttingdown = true;

        for (Invocations invocations : invocationsPerMember.values()) {
            for (Invocation i : invocations.map.values()) {
                i.future.completeExceptionally(new RuntimeException("Shutting down"));
            }
        }

        responseThread.shutdown();
        monitorThread.shutdown();
    }

    public CompletableFuture invoke(Invocation inv) {
        if (shuttingdown) {
            throw new RuntimeException("Can't make invocation, frontend shutting down");
        }

        try {
            int partitionId = inv.partitionId;

            if (partitionId >= 0) {
                Address address = nodeEngine.getPartitionService().getPartitionOwner(partitionId);

                if (address.equals(thisAddress)) {
                    //System.out.println("local invoke");
                    // todo: hack with the assignment of a partition to a local cpu.
                    reactors[partitionIdToChannel(partitionId) % reactorCount].schedule(inv);
                } else {
                    Invocations invocations = getInvocations(address);
                    long callId = invocations.callId.incrementAndGet();
                    inv.callId = callId;
                    inv.out.writeLong(Invocation.OFFSET_CALL_ID, callId);
                    invocations.map.put(callId, inv);
                    //System.out.println("remove invoke");
                    TcpServerConnection connection = getConnection(address);
                    Channel channel = connection.channels[partitionIdToChannel(partitionId)];

                    Packet packet = inv.toPacket();
                    ByteBuffer buffer = ByteBuffer.allocate(packet.totalSize() + 30);
                    new PacketIOHelper().writeTo(packet, buffer);
                    buffer.flip();
                    channel.writeAndFlush(buffer);
                }

                return inv.future;
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
                        futures.add(reactors[hashToIndex(channelIndex, reactors.length)].schedule(reactorAddress, connection));
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

                System.out.println("channels to " + address + " established");
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

    public static class Invocations {
        private final ConcurrentMap<Long, Invocation> map = new ConcurrentHashMap<>();
        private final AtomicLong callId = new AtomicLong(500);
    }

    public void handleResponse(Packet packet) {
        if (packet.next != null) {
            responseThread.queue.add(packet);
            return;
        }

        try {
            Address remoteAddress = packet.getConn().getRemoteAddress();
            Invocations invocations = invocationsPerMember.get(remoteAddress);
            if (invocations == null) {
                System.out.println("Dropping response " + packet + ", invocations not found");
                return;
            }

            ByteArrayObjectDataInput in = new ByteArrayObjectDataInput(packet.toByteArray(), ss, BIG_ENDIAN);

            long callId = in.readLong();
            Invocation request = invocations.map.remove(callId);
            if (request == null) {
                System.out.println("Dropping response " + packet + ", invocation with id " + callId + " not found");
            } else {
                request.future.complete(packet);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public class ResponseThread extends Thread {
        public final BlockingQueue<Packet> queue = new LinkedBlockingQueue();

        public ResponseThread() {
            super("ResponseThread");
        }

        @Override
        public void run() {
            try {
                while (!shuttingdown) {
                    Packet packet = queue.take();
                    do {
                        Packet next = packet.next;
                        packet.next = null;
                        handleResponse(packet);
                        packet = next;
                    } while (packet != null);
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
