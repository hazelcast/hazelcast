/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.tpc.server;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.ConnectionListener;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.server.Server;
import com.hazelcast.internal.server.ServerConnection;
import com.hazelcast.internal.server.ServerConnectionManager;
import com.hazelcast.internal.server.tcp.TcpServerConnection;
import com.hazelcast.internal.tpc.FrameCodec;
import com.hazelcast.internal.tpc.RequestFuture;
import com.hazelcast.internal.tpc.RpcCore;
import com.hazelcast.internal.tpcengine.Reactor;
import com.hazelcast.internal.tpcengine.TpcEngine;
import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;
import com.hazelcast.internal.tpcengine.net.AsyncServerSocket;
import com.hazelcast.internal.tpcengine.net.AsyncSocket;
import com.hazelcast.internal.tpcengine.net.AsyncSocketReader;
import com.hazelcast.internal.tpcengine.util.BindRandomPort;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.internal.util.concurrent.MPSCQueue;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.hazelcast.instance.EndpointQualifier.MEMBER;
import static com.hazelcast.internal.cluster.impl.MemberHandshake.OPTION_TPC_PORTS;
import static com.hazelcast.internal.tpc.FrameCodec.FLAG_RES_CTRL;
import static com.hazelcast.internal.tpc.FrameCodec.OFFSET_PARTITION_ID;
import static com.hazelcast.internal.tpc.FrameCodec.OFFSET_RES_CALL_ID;
import static com.hazelcast.internal.tpc.FrameCodec.OFFSET_RES_PAYLOAD;
import static com.hazelcast.internal.tpc.FrameCodec.RES_CTRL_TYPE_EXCEPTION;
import static com.hazelcast.internal.tpc.FrameCodec.RES_CTRL_TYPE_OVERLOAD;
import static com.hazelcast.internal.tpcengine.net.AsyncSocketOptions.SO_KEEPALIVE;
import static com.hazelcast.internal.tpcengine.net.AsyncSocketOptions.SO_RCVBUF;
import static com.hazelcast.internal.tpcengine.net.AsyncSocketOptions.SO_REUSEADDR;
import static com.hazelcast.internal.tpcengine.net.AsyncSocketOptions.SO_SNDBUF;
import static com.hazelcast.internal.tpcengine.net.AsyncSocketOptions.TCP_NODELAY;
import static com.hazelcast.internal.tpcengine.util.BitUtil.SIZEOF_UUID;
import static com.hazelcast.internal.util.HashUtil.hashToIndex;
import static com.hazelcast.internal.util.StringUtil.toIntegerList;
import static java.lang.Boolean.parseBoolean;
import static java.lang.Integer.parseInt;
import static java.lang.System.getProperty;

/**
 * The {@link ServerRpcCore} takes care of RPC between members.
 * <p>
 * todo: Should also handle redirect messages.
 */
public final class ServerRpcCore implements RpcCore {
    private final boolean writeThrough = parseBoolean(getProperty("hazelcast.tpc.write-through", "false"));
    private final boolean regularSchedule = parseBoolean(getProperty("hazelcast.tpc.regular-schedule", "true"));
    private final int concurrentRequestLimit = parseInt(getProperty("hazelcast.tpc.concurrent-request-limit", "-1"));
    private final int responseThreadCount = parseInt(getProperty("hazelcast.tpc.responsethread.count", "1"));
    private final boolean responseThreadSpin = parseBoolean(getProperty("hazelcast.tpc.responsethread.spin", "false"));

    private final InternalPartitionService partitionService;
    private final Address thisAddress;
    private final RequestsRegistry requestRegistry;
    private final ServerConnectionManager connectionManager;
    private final UUID thisUuid;
    private final Function<Reactor, AsyncSocketReader> frameDecoderConstructor;
    private final ResponseThread[] responseThreads;
    private TpcEngine tpcEngine;
    private Server classicServer;

    public ServerRpcCore(ServerConnectionManager connectionManager,
                         InternalPartitionService partitionService,
                         Address thisAddress,
                         UUID thisUuid,
                         Function<Reactor, AsyncSocketReader> frameDecoderConstructor) {
        this.connectionManager = connectionManager;
        this.partitionService = partitionService;
        this.thisAddress = thisAddress;
        this.thisUuid = thisUuid;
        this.requestRegistry = new RequestsRegistry(concurrentRequestLimit, partitionService.getPartitionCount());
        this.frameDecoderConstructor = frameDecoderConstructor;
        this.responseThreads = new ResponseThread[responseThreadCount];
        for (int k = 0; k < responseThreadCount; k++) {
            responseThreads[k] = new ResponseThread(k, responseThreadSpin);
        }
    }

    public void setTpcEngine(TpcEngine tpcEngine) {
        this.tpcEngine = tpcEngine;
    }

    public void setClassicServer(Server classicServer) {
        this.classicServer = classicServer;
    }

    @Override
    public RequestFuture<IOBuffer> invoke(int partitionId, IOBuffer request) {
        RequestFuture future = new RequestFuture(request);

        Requests requests = requestRegistry.getByPartitionId(partitionId);
        requests.put(future);

        Address address = partitionService.getPartitionOwner(partitionId);
        if (address == null) {
            throw new RuntimeException("Address is still null (we need to deal with this situation better)");
        }

        boolean accepted;

        if (address.equals(thisAddress)) {
            // It is a local request

            request.socket = null;

            int reactorIndex = hashToIndex(partitionId, tpcEngine.reactorCount());
            accepted = tpcEngine.reactor(reactorIndex).offer(request);
        } else {
            // It is a remote request.

            //  System.out.println("remote request");
            // todo: this should in theory not be needed. We could use the last
            // address and only in case of a redirect, we update.
            TcpServerConnection connection = getConnection(address);
            AsyncSocket[] sockets = connection.getTpcSockets();
            AsyncSocket socket = sockets[hashToIndex(partitionId, sockets.length)];

            // we need to acquire the frame because storage will release it once written
            // and we need to keep the frame around for the response.
            request.acquire();

            accepted = socket.writeAndFlush(request);
        }

        if (!accepted) {
            // todo: we need to deregister the request
            //requests.slots.remove()
            future.completeExceptionally(new RuntimeException());
        }

        return future;
    }

    // TODO: Blocking part sucks
    private TcpServerConnection getConnection(Address address) {
        TcpServerConnection connection = (TcpServerConnection) connectionManager.get(address);
        if (connection == null) {
            connectionManager.getOrConnect(address);
            for (int k = 0; k < 600; k++) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    //todo
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

        openMemberTpcSockets(connection);
        return connection;
    }

    // todo: blocking sucks.
    // todo: bind also blocks; so no good to be happening on tpc thread.
    void openMemberTpcSockets(TcpServerConnection connection) {
        if (connection.getTpcSockets() != null) {
            return;
        }

        synchronized (connection) {
            if (connection.getTpcSockets() != null) {
                return;
            }

            Address remoteAddress = connection.getRemoteAddress();
            long start = System.currentTimeMillis();

            // If the remote side doesn't have TPC ports, then no sockets need to be opened.
            List<Integer> remoteTpcPorts = toIntegerList((String) connection.attributeMap().get(OPTION_TPC_PORTS));
            if (remoteTpcPorts.isEmpty()) {
                return;
            }
            // todo:connection should also be marked that no such attempts should be made again

            System.out.println("MemberTpcRuntime received remote tpc ports:" + remoteTpcPorts);

            // todo: we can get imbalance. So imagine that 2 member has 2 reactors and
            // all other members in the cluster have 1. Then all the communication with the
            // remote members will be done by 1 reactor instead of all.
            // We need to think if this actually is a problem.
            AsyncSocket[] sockets = new AsyncSocket[remoteTpcPorts.size()];
            for (int k = 0; k < remoteTpcPorts.size(); k++) {
                int reactorIndex = hashToIndex(k, tpcEngine.reactorCount());
                Reactor reactor = tpcEngine.reactor(reactorIndex);

                AsyncSocket socket = reactor.newAsyncSocketBuilder()
                        .setReader(frameDecoderConstructor.apply(reactor))
                        .set(SO_SNDBUF, 256 * 1024)
                        .set(SO_RCVBUF, 256 * 1024)
                        .set(TCP_NODELAY, true)
                        .build();
                socket.start();
                sockets[k] = socket;
            }

            CompletableFuture[] futures = new CompletableFuture[remoteTpcPorts.size()];
            for (int k = 0; k < remoteTpcPorts.size(); k++) {
                SocketAddress eventloopAddress = new InetSocketAddress(remoteAddress.getHost(), remoteTpcPorts.get(k));
                futures[k] = sockets[k].connect(eventloopAddress);
            }

            for (int k = 0; k < futures.length; k++) {
                futures[k].join();
                System.out.println(sockets[k] + " connected.");
            }

            for (AsyncSocket socket : sockets) {
                sendTpcHandshake(socket);
            }

            connection.setTpcSockets(sockets);
            System.out.println("Duration to make all connections:" + (System.currentTimeMillis() - start) + " ms");
        }
    }

    private void startMemberPlane() {
        List<Integer> tpcPorts = new ArrayList<>();

        BindRandomPort bindRandomPort = new BindRandomPort(11000, 12000);

        List<AsyncServerSocket> serverSockets = new ArrayList<>();
        for (int k = 0; k < tpcEngine.reactorCount(); k++) {
            Reactor reactor = tpcEngine.reactor(k);

            AsyncServerSocket serverSocket = reactor.newAsyncServerBuilder()
                    .set(SO_RCVBUF, 256 * 1024)
                    .set(SO_REUSEADDR, true)
                    .setAcceptConsumer(acceptRequest -> {
                        AsyncSocket socket = reactor.newAsyncSocketBuilder(acceptRequest)
                                .set(SO_RCVBUF, 256 * 1024)
                                .set(SO_SNDBUF, 256 * 1024)
                                .set(TCP_NODELAY, true)
                                .set(SO_KEEPALIVE, true)
                                .setReader(frameDecoderConstructor.apply(reactor))
                                .build();

                        socket.start();
                        sendTpcHandshake(socket);
                    })
                    .build();
            serverSockets.add(serverSocket);

            // we bind on all interfaces for now.
            tpcPorts.add(bindRandomPort.bind(serverSocket, null));
        }

        // We signal the tpcPorts to the server so that the appropriate memberHandshake is send.
        classicServer.getContext()
                .getExtraHandshakeOptions()
                .put(OPTION_TPC_PORTS, StringUtil.toString(tpcPorts, ","));

        // We register a listener on every connection created to open the TpcPorts if needed.
        classicServer.getConnectionManager(MEMBER).addConnectionListener(new ConnectionListener<>() {
            @Override
            public void connectionAdded(ServerConnection c) {
                openMemberTpcSockets((TcpServerConnection) c);
            }

            @Override
            public void connectionRemoved(ServerConnection connection) {

            }
        });

        for (AsyncServerSocket serverSocket : serverSockets) {
            serverSocket.start();
        }
    }

    void sendTpcHandshake(AsyncSocket socket) {
        IOBuffer handshake = new IOBuffer(SIZEOF_UUID);
        handshake.writeLong(thisUuid.getMostSignificantBits());
        handshake.writeLong(thisUuid.getLeastSignificantBits());
        handshake.flip();
        socket.writeAndFlush(handshake);
    }

    public void start() {
        for (ResponseThread t : responseThreads) {
            t.start();
        }

        startMemberPlane();
    }

    public void shutdown() {
        requestRegistry.shutdown();

        for (ResponseThread t : responseThreads) {
            t.shutdown();
        }
    }

    @Override
    public void accept(IOBuffer response) {
        try {
            if (response.next != null && responseThreadCount > 0) {
                int index = hashToIndex(response.getLong(OFFSET_RES_CALL_ID), responseThreadCount);
                responseThreads[index].queue.add(response);
            } else {
                handleResponse(response);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void handleResponse(IOBuffer response) {
        int partitionId = response.getInt(OFFSET_PARTITION_ID);
        Requests requests;
        if (partitionId >= 0) {
            requests = requestRegistry.getByPartitionId(partitionId);
        } else {
            requests = requestRegistry.getByAddress(response.socket.getRemoteAddress());
            if (requests == null) {
                System.out.println("Dropping response " + response + ", requests not found");
                response.release();
                return;
            }
        }

        long callId = response.getLong(OFFSET_RES_CALL_ID);

        RequestFuture future = requests.remove(callId);
        if (future == null) {
            System.out.println("Dropping response " + response + ", invocation with id " + callId
                    + ", partitionId: " + partitionId + " not found");
            return;
        }

        requests.complete();

        IOBuffer request = future.request;
        future.request = null;

        int flags = FrameCodec.flags(response);
        if ((flags & FLAG_RES_CTRL) == 0) {
            response.position(OFFSET_RES_PAYLOAD);
            // it is a normal response
            future.complete(response);
        } else {
            // it is a response that requires control
            response.position(OFFSET_RES_PAYLOAD);

            int responseType = response.readInt();
            switch (responseType) {
                case RES_CTRL_TYPE_OVERLOAD:
                    // we need to find better solution
                    future.completeExceptionally(new RuntimeException("Server is overloaded"));
                    response.release();
                    break;
                case RES_CTRL_TYPE_EXCEPTION:
                    // todo: stacktrace of remote problem and all that.
                    // todo: probably change the type of exception
                    future.completeExceptionally(new RuntimeException(response.readString()));
                    response.release();
                    break;
                default:
                    throw new RuntimeException("Unknown response responseType:" + responseType);
            }
        }

        if (request.socket != null) {
            //   System.out.println("request.socket was not null");
            request.release();
        }
    }

    private class ResponseThread extends Thread {

        // todo: create litter
        // todo: unbounded
        private final MPSCQueue<IOBuffer> queue;
        private final boolean spin;
        private volatile boolean shuttingdown = false;

        private ResponseThread(int index, boolean spin) {
            super("ResponseThread-" + index);
            this.queue = new MPSCQueue<>(this, null);
            this.spin = spin;
        }

        @Override
        public void run() {
            try {
                while (!shuttingdown) {
                    IOBuffer buf;
                    if (spin) {
                        do {
                            buf = queue.poll();
                        } while (buf == null);
                    } else {
                        buf = queue.take();
                    }

                    do {
                        IOBuffer next = buf.next;
                        buf.next = null;
                        handleResponse(buf);
                        buf = next;
                    } while (buf != null);
                }
            } catch (InterruptedException e) {
                System.out.println("ResponseThread stopping due to interrupt");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private void shutdown() {
            shuttingdown = true;
            interrupt();
        }
    }
}
