package com.hazelcast.spi.impl.reactor;

import com.hazelcast.cluster.Address;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.nio.PacketIOHelper;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.server.ServerConnectionManager;
import com.hazelcast.internal.server.tcp.TcpServerConnection;
import com.hazelcast.internal.util.HashUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static com.hazelcast.internal.nio.Packet.FLAG_OP_RESPONSE;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ReactorFrontEnd implements Consumer<Packet> {

    private final NodeEngineImpl nodeEngine;
    public final SerializationService ss;
    public final ILogger logger;
    private final Address thisAddress;
    private volatile ServerConnectionManager connectionManager;
    public volatile boolean shuttingdown = false;
    private final Reactor[] reactorThreads;
    public final Managers managers = new Managers();

    public ReactorFrontEnd(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(ReactorFrontEnd.class);
        this.ss = nodeEngine.getSerializationService();
        this.reactorThreads = new Reactor[2];
        this.thisAddress = nodeEngine.getThisAddress();

        for (int cpu = 0; cpu < reactorThreads.length; cpu++) {
            int port = toPort(thisAddress, cpu);
            reactorThreads[cpu] = new Reactor(this, port);
        }
    }

    public int toPort(Address address, int cpu) {
        return (address.getPort() - 5701) * 100 + 11000 + cpu;
    }

    public int partitionIdToCpu(int partitionId) {
        return HashUtil.hashToIndex(partitionId, reactorThreads.length);
    }

    public void start() {
        logger.finest("Starting ReactorServicee");

        for (Reactor t : reactorThreads) {
            t.start();
        }
    }

    public void shutdown() {
        shuttingdown = true;
    }


    public CompletableFuture invoke(Request request) {
        int partitionId = request.partitionId;

        if (partitionId >= 0) {
            System.out.println("Blabla");
            Address targetAddress = nodeEngine.getPartitionService().getPartitionOwner(partitionId);
            TargetInvocations invocations = getTargetInvocations(targetAddress);
            Invocation invocation = new Invocation();
            invocation.callId = invocations.counter.incrementAndGet();
            invocation.request = request;
            request.invocation = invocation;
            invocations.map.put(invocation.callId, invocation);

            if (targetAddress.equals(thisAddress)) {
                System.out.println("local invoke");
                reactorThreads[partitionIdToCpu(partitionId)].enqueue(request);
            } else {
                System.out.println("remove invoke");
                if (connectionManager == null) {
                    connectionManager = nodeEngine.getNode().getServer().getConnectionManager(EndpointQualifier.MEMBER);
                }

                TcpServerConnection connection = getConnection(targetAddress);
                Channel[] channels = (Channel[]) connection.junk;
                Channel channel = channels[partitionIdToCpu(partitionId)];
                Packet packet = request.toPacket();
                ByteBuffer buffer = ByteBuffer.allocate(packet.totalSize()+8);
                new PacketIOHelper().writeTo(packet, buffer);
                channel.enqueueAndFlush(buffer);
            }

            return invocation.completableFuture;
        } else {
            throw new RuntimeException();
        }
    }

    private TcpServerConnection getConnection(Address targetAddress) {
        TcpServerConnection connection = (TcpServerConnection) connectionManager.get(targetAddress);
        if (connection == null) {
            connectionManager.getOrConnect(targetAddress);
            try {
                if (!connectionManager.blockOnConnect(thisAddress, SECONDS.toMillis(10), 0)) {
                    throw new RuntimeException();
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        if (connection.junk == null) {
            synchronized (connection) {
                if (connection.junk == null) {
                    Channel[] channels = new Channel[reactorThreads.length];
                    connection.junk = channels;
                    Address remoteAddress = connection.getRemoteAddress();

                    for (int cpu = 0; cpu < channels.length; cpu++) {
                        SocketAddress socketAddress = new InetSocketAddress(remoteAddress.getHost(), toPort(remoteAddress, cpu));
                        Future<Channel> f = reactorThreads[cpu].enqueue(socketAddress);
                        try {
                            Channel channel = f.get();
                            channels[cpu] = channel;
                        } catch (Exception e) {
                            throw new RuntimeException();
                        }
                        //todo: assignment of the socket to the channels.
                    }
                }
            }
        }
        return connection;
    }

    public TargetInvocations getTargetInvocations(Address address) {
        TargetInvocations invocations = invocationsPerMember.get(address);
        if (invocations != null) {
            return invocations;
        }

        TargetInvocations newInvocations = new TargetInvocations(address);
        TargetInvocations foundInvocations = invocationsPerMember.putIfAbsent(address, newInvocations);
        return foundInvocations == null ? newInvocations : foundInvocations;
    }

    private final ConcurrentMap<Address, TargetInvocations> invocationsPerMember = new ConcurrentHashMap<>();

    private class TargetInvocations {
        private final Address target;
        private final ConcurrentMap<Long, Invocation> map = new ConcurrentHashMap<>();
        private final AtomicLong counter = new AtomicLong(0);

        public TargetInvocations(Address target) {
            this.target = target;
        }

    }


    //todo: add option to bypass offloading to thread for thread per core versionn
    @Override
    public void accept(Packet packet) {
        if (packet.isFlagRaised(FLAG_OP_RESPONSE)) {
            Address remoteAddress = packet.getConn().getRemoteAddress();
            TargetInvocations targetInvocations = invocationsPerMember.get(remoteAddress);
            if (targetInvocations == null) {
                System.out.println("Dropping response " + packet + ", targetInvocations not found");
                return;
            }

            long callId = 0;
            Invocation invocation = targetInvocations.map.get(callId);
            if (invocation == null) {
                System.out.println("Dropping response " + packet + ", invocation not found");
                invocation.completableFuture.complete(null);
            }
        } else {
            int index = HashUtil.hashToIndex(packet.getPartitionHash(), reactorThreads.length);
            reactorThreads[index].enqueue(packet);
        }
    }

//    class EngineThread extends Thread {
//        private final Reactor engine = new Reactor(ReactorService.this);
//
//        public void run() {
//            try {
//                loop();
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }
//
//        private void loop() throws InterruptedException {
//            while (!shuttingdown) {
//                engine.tick();
//            }
//        }
//    }
}
