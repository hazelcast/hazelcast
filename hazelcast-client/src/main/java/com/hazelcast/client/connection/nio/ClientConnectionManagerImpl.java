/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.connection.nio;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.LoadBalancer;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.SocketOptions;
import com.hazelcast.client.connection.Authenticator;
import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.connection.Router;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.*;
import com.hazelcast.nio.serialization.SerializationContext;
import com.hazelcast.nio.serialization.SerializationService;

import java.io.IOException;
import java.net.Socket;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author ali 14/12/13
 */
public class ClientConnectionManagerImpl implements ClientConnectionManager {

    private static final ILogger logger = Logger.getLogger(ClientConnectionManagerImpl.class);

    static final int KILO_BYTE = 1024;
    private static final int BUFFER_SIZE = 16 << 10; // 32k

    public static final int socketReceiveBufferSize = 32 * KILO_BYTE;
    public static final int socketSendBufferSize = 32 * KILO_BYTE;

    private final AtomicInteger connectionIdGen = new AtomicInteger();
    private final Authenticator authenticator;
    private final HazelcastClient client;
    private final Router router;
    private final SocketInterceptor socketInterceptor;
    private final SocketOptions socketOptions;
    private final IOSelector inSelector;
    private final IOSelector outSelector;

    private final ConcurrentMap<Address, ClientConnection> connections = new ConcurrentHashMap<Address, ClientConnection>();

    private volatile boolean live = false;

    public ClientConnectionManagerImpl(HazelcastClient client, Authenticator authenticator, LoadBalancer loadBalancer) {
        this.client = client;
        this.authenticator = authenticator;
        ClientConfig config = client.getClientConfig();
        router = new Router(loadBalancer);
        final ThreadGroup threadGroup = new ThreadGroup("IOThreads");
        inSelector = new ClientInSelectorImpl(threadGroup);
        outSelector = new ClientOutSelectorImpl(threadGroup);

        //init socketInterceptor
        SocketInterceptorConfig sic = config.getSocketInterceptorConfig();
        if (sic != null && sic.isEnabled()) {
            SocketInterceptor implementation = (SocketInterceptor) sic.getImplementation();
            if (implementation == null && sic.getClassName() != null) {
                try {
                    implementation = (SocketInterceptor) Class.forName(sic.getClassName()).newInstance();
                } catch (Throwable e) {
                    logger.severe("SocketInterceptor class cannot be instantiated!" + sic.getClassName(), e);
                }
            }
            if (implementation != null) {
                if (!(implementation instanceof MemberSocketInterceptor)) {
                    logger.severe("SocketInterceptor must be instance of " + MemberSocketInterceptor.class.getName());
                    implementation = null;
                } else {
                    logger.info("SocketInterceptor is enabled");
                }
            }
            if (implementation != null) {
                socketInterceptor = implementation;
                socketInterceptor.init(sic.getProperties());
            } else {
                socketInterceptor = null;
            }
        } else {
            socketInterceptor = null;
        }

//        int connectionTimeout = config.getConnectionTimeout(); //TODO
        socketOptions = config.getSocketOptions();

    }

    public SerializationContext getSerializationContext(){
        return client.getSerializationService().getSerializationContext();
    }

    public SerializationService getSerializationService(){
        return client.getSerializationService();
    }

    public synchronized void start(){
        if (live) {
            return;
        }
        live = true;
        inSelector.start();
        outSelector.start();
    }

    public synchronized void shutdown() {
        if (!live){
            return;
        }
        live = false;
        for (ClientConnection connection : connections.values()) {
            connection.close();
        }
        inSelector.shutdown();
        outSelector.shutdown();
    }

    public ClientConnection getRandomConnection() throws IOException {
        final Address address = router.next();
        return getOrConnect(address, authenticator);
    }

    public ClientConnection getOrConnect(Address address) throws IOException {
        return getOrConnect(address, authenticator);
    }

    public ClientConnection ownerConnection(Address address, Authenticator authenticator) throws IOException {
        ClientConnection clientConnection = connect(address, authenticator, true);
        return clientConnection;
    }

    private ClientConnection getOrConnect(Address address, Authenticator authenticator) throws IOException {
        if (address == null) {
            throw new Error("TODO");
        }
        ClientConnection clientConnection = connections.get(address);
        if (clientConnection == null) {
            synchronized (this) {
                clientConnection = connections.get(address);
                if (clientConnection == null) {
                    clientConnection = connect(address, authenticator, false);
                    connections.put(clientConnection.getRemoteEndpoint(), clientConnection);
                }
            }
        }
        return clientConnection;
    }

    private ClientConnection connect(Address address, Authenticator authenticator, boolean isBlock) throws IOException {
        if (!live) {
            throw new HazelcastException("ConnectionManager is not active!!!");
        }
        SocketChannel socketChannel = SocketChannel.open();
        Socket socket = socketChannel.socket();
        socket.setKeepAlive(socketOptions.isKeepAlive());
        socket.setTcpNoDelay(socketOptions.isTcpNoDelay());
        socket.setReuseAddress(socketOptions.isReuseAddress());
        if (socketOptions.getLingerSeconds() > 0) {
            socket.setSoLinger(true, socketOptions.getLingerSeconds());
        }
        socket.setSoTimeout(5000);
        int bufferSize = socketOptions.getBufferSize() * KILO_BYTE;
        if (bufferSize < 0) {
            bufferSize = BUFFER_SIZE;
        }
        socket.setSendBufferSize(bufferSize);
        socket.setReceiveBufferSize(bufferSize);
        socketChannel.connect(address.getInetSocketAddress());
        final ClientConnection clientConnection = new ClientConnection(this, inSelector, outSelector, connectionIdGen.incrementAndGet(), socketChannel);
        socketChannel.configureBlocking(true);
        authenticator.auth(clientConnection);
        socketChannel.configureBlocking(isBlock);
        socket.setSoTimeout(0);
        if (!isBlock) {
            clientConnection.getReadHandler().register();
        }
        if (socketInterceptor != null) {
            socketInterceptor.onConnect(socket);
        }
        return clientConnection;
    }


    public void destroyConnection(ClientConnection clientConnection) {
        Address endpoint = clientConnection.getRemoteEndpoint();
        if (endpoint != null) {
            connections.remove(clientConnection.getRemoteEndpoint());
        }
        client.getClientClusterService().removeConnectionCalls(clientConnection);
    }

    public void handlePacket(ClientPacket packet) {
        client.getClientClusterService().handlePacket(packet);
    }




}
