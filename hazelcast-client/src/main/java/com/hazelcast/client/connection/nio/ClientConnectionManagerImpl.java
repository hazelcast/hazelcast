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

import com.hazelcast.client.*;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.SocketOptions;
import com.hazelcast.client.connection.Authenticator;
import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.connection.Router;
import com.hazelcast.client.spi.ClientClusterService;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.spi.impl.ClientCallFuture;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ClientPacket;
import com.hazelcast.nio.IOSelector;
import com.hazelcast.nio.SocketInterceptor;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationContext;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.security.Credentials;
import com.hazelcast.spi.exception.RetryableIOException;
import com.hazelcast.spi.impl.SerializableCollection;
import com.hazelcast.util.ExceptionUtil;

import java.io.IOException;
import java.net.Socket;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author ali 14/12/13
 */
public class ClientConnectionManagerImpl implements ClientConnectionManager {

    private static final ILogger logger = Logger.getLogger(ClientConnectionManagerImpl.class);

    private static int RETRY_COUNT = 20;

    static final int KILO_BYTE = 1024;
    private static final int BUFFER_SIZE = 16 << 10; // 32k

    public static final int SOCKET_RECEIVE_BUFFER_SIZE = BUFFER_SIZE;
    public static final int SOCKET_SEND_BUFFER_SIZE = BUFFER_SIZE;

    private final AtomicInteger connectionIdGen = new AtomicInteger();
    private final HazelcastClient client;
    private final Router router;
    private final SocketInterceptor socketInterceptor;
    private final SocketOptions socketOptions;
    private final IOSelector inSelector;
    private final IOSelector outSelector;
    private final boolean smartRouting;
    private volatile Address ownerConnectionAddress = null;

    private final Credentials credentials;
    private volatile ClientPrincipal principal;
    private final AtomicInteger callIdIncrementer = new AtomicInteger();

    private final ConcurrentMap<Address, ClientConnection> connections = new ConcurrentHashMap<Address, ClientConnection>();

    private volatile boolean live = false;

    public ClientConnectionManagerImpl(HazelcastClient client, LoadBalancer loadBalancer, boolean smartRouting) {
        this.client = client;
        this.smartRouting = smartRouting;
        this.credentials = client.getClientConfig().getCredentials();
        ClientConfig config = client.getClientConfig();
        router = new Router(loadBalancer);
        inSelector = new ClientInSelectorImpl(client.getThreadGroup());
        outSelector = new ClientOutSelectorImpl(client.getThreadGroup());
        //init socketInterceptor
        SocketInterceptorConfig sic = config.getSocketInterceptorConfig();
        SocketInterceptor implementation = null;
        if (sic != null && sic.isEnabled()) {
            implementation = (SocketInterceptor) sic.getImplementation();
            if (implementation == null && sic.getClassName() != null) {
                try {
                    implementation = (SocketInterceptor) Class.forName(sic.getClassName()).newInstance();
                } catch (Throwable e) {
                    logger.severe("SocketInterceptor class cannot be instantiated!" + sic.getClassName(), e);
                }
            }
        }

        socketInterceptor = implementation;
        if (socketInterceptor != null) {
            logger.info("SocketInterceptor is enabled");
            socketInterceptor.init(sic.getProperties());
        }

//        int connectionTimeout = config.getConnectionTimeout(); //TODO
        socketOptions = config.getSocketOptions();

    }

    public boolean isLive() {
        return live;
    }

    public SerializationContext getSerializationContext() {
        return client.getSerializationService().getSerializationContext();
    }

    public SerializationService getSerializationService() {
        return client.getSerializationService();
    }

    public synchronized void start() {
        if (live) {
            return;
        }
        live = true;
        inSelector.start();
        outSelector.start();
    }

    public synchronized void shutdown() {
        if (!live) {
            return;
        }
        live = false;
        for (ClientConnection connection : connections.values()) {
            connection.close();
        }
        inSelector.shutdown();
        outSelector.shutdown();
    }

    public ClientConnection ownerConnection(Address address) throws IOException {
        ClientConnection clientConnection = connect(address, new ManagerAuthenticator(), true);
        ownerConnectionAddress = clientConnection.getRemoteEndpoint();
        return clientConnection;
    }

    public ClientConnection tryToConnect(Address target) throws IOException {
        final Authenticator authenticator = new ClusterAuthenticator();
        int count = 0;
        IOException lastError = null;
        while (count < RETRY_COUNT) {
            try {
                if (target == null || !isMember(target)) {
                    final Address address = router.next();
                    return getOrConnect(address, authenticator);
                } else {
                    return getOrConnect(target, authenticator);
                }
            } catch (IOException e) {
                lastError = e;
            }
            target = null;
            count++;
        }
        throw lastError;
    }

    public ClientPrincipal getPrincipal() {
        return principal;
    }

    private boolean isMember(Address target) {
        final ClientClusterService clientClusterService = client.getClientClusterService();
        return clientClusterService.getMember(target) != null;
    }

    private ClientConnection getOrConnect(Address address, Authenticator authenticator) throws IOException {
        if (address == null) {
            throw new NullPointerException("Address is required!");
        }
        if (!smartRouting) {
            address = ownerConnectionAddress;
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
        final ClientConnection clientConnection = new ClientConnection(this, inSelector, outSelector,
                connectionIdGen.incrementAndGet(), socketChannel);
        socketChannel.configureBlocking(true);
        if (socketInterceptor != null) {
            socketInterceptor.onConnect(socket);
        }
        authenticator.auth(clientConnection);
        socketChannel.configureBlocking(isBlock);
        socket.setSoTimeout(0);
        if (!isBlock) {
            clientConnection.getReadHandler().register();
        }
        return clientConnection;
    }


    public void destroyConnection(ClientConnection clientConnection) {
        Address endpoint = clientConnection.getRemoteEndpoint();
        if (endpoint != null) {
            connections.remove(clientConnection.getRemoteEndpoint());
        }
    }

    public boolean removeEventHandler(Integer callId) {
        if (callId != null) {
            for (ClientConnection clientConnection : connections.values()) {
                if (clientConnection.deRegisterEventHandler(callId) != null) {
                    return true;
                }
            }
        }
        return false;
    }

    public void handlePacket(ClientPacket packet) {
        client.getClientExecutionService().execute(new ClientPacketProcessor(packet));
    }

    public int newCallId() {
        return callIdIncrementer.incrementAndGet();
    }

    class ClientPacketProcessor implements Runnable {

        ClientPacket packet;

        ClientPacketProcessor(ClientPacket packet) {
            this.packet = packet;
        }

        public void run() {
            final ClientConnection conn = (ClientConnection) packet.getConn();
            final ClientResponse clientResponse = getSerializationService().toObject(packet.getData());
            final int callId = clientResponse.getCallId();
            final Object response = clientResponse.getResponse();
            if (clientResponse.isEvent()) {
                handleEvent(response, callId, conn);
            } else {
                handlePacket(response, callId, conn);
            }
        }

        private void handlePacket(Object response, int callId, ClientConnection conn) {
            final ClientCallFuture future = conn.deRegisterCallId(callId);
            if (future == null) {
                logger.warning("No call for callId: " + callId + ", response: " + response);
                return;
            }
            future.notify(response);
        }

        private void handleEvent(Object event, int callId, ClientConnection conn) {
            final EventHandler eventHandler = conn.getEventHandler(callId);
            final Object eventObject = getSerializationService().toObject(event);
            if (eventHandler == null) {
                logger.warning("No eventHandler for callId: " + callId + ", event: " + eventObject + ", conn: " + conn);
                return;
            }
            eventHandler.handle(eventObject);

        }
    }


    public class ManagerAuthenticator implements Authenticator {

        public void auth(ClientConnection connection) throws AuthenticationException, IOException {
            final Object response = authenticate(connection, credentials, principal, true, true);
            principal = (ClientPrincipal) response;
        }
    }

    private class ClusterAuthenticator implements Authenticator {
        public void auth(ClientConnection connection) throws AuthenticationException, IOException {
            authenticate(connection, credentials, principal, false, false);
        }
    }

    private Object authenticate(ClientConnection connection, Credentials credentials, ClientPrincipal principal, boolean reAuth, boolean firstConnection) throws IOException {
        final SerializationService ss = getSerializationService();
        AuthenticationRequest auth = new AuthenticationRequest(credentials, principal);
        connection.init();
        auth.setReAuth(reAuth);
        auth.setFirstConnection(firstConnection);
        SerializableCollection collectionWrapper; //contains remoteAddress and principal
        try {
            collectionWrapper = (SerializableCollection) sendAndReceive(auth, connection);
        } catch (Exception e) {
            throw new RetryableIOException(e);
        }
        final Iterator<Data> iter = collectionWrapper.iterator();
        if (iter.hasNext()) {
            final Data addressData = iter.next();
            final Address address = ss.toObject(addressData);
            connection.setRemoteEndpoint(address);
            if (iter.hasNext()) {
                final Data principalData = iter.next();
                return ss.toObject(principalData);
            }
        }
        throw new AuthenticationException();
    }

    public Object sendAndReceive(ClientRequest request, ClientConnection connection) throws Exception {
        final SerializationService ss = client.getSerializationService();
        connection.write(ss.toData(request));
        final Data data = connection.read();
        ClientResponse clientResponse = ss.toObject(data);
        Object response = ss.toObject(clientResponse.getResponse());
        if (response instanceof Throwable) {
            Throwable t = (Throwable) response;
            ExceptionUtil.fixRemoteStackTrace(t, Thread.currentThread().getStackTrace());
            throw new Exception(t);
        }
        return response;
    }

}
