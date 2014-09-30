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

import com.hazelcast.client.AuthenticationException;
import com.hazelcast.client.ClientExtension;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.LoadBalancer;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.config.ClientProperties;
import com.hazelcast.client.config.ClientSecurityConfig;
import com.hazelcast.client.config.SocketOptions;
import com.hazelcast.client.connection.AddressTranslator;
import com.hazelcast.client.connection.Authenticator;
import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.connection.Router;
import com.hazelcast.client.impl.client.AuthenticationRequest;
import com.hazelcast.client.impl.client.ClientPrincipal;
import com.hazelcast.client.impl.client.ClientRequest;
import com.hazelcast.client.impl.client.ClientResponse;
import com.hazelcast.client.spi.ClientClusterService;
import com.hazelcast.client.spi.impl.ClientExecutionServiceImpl;
import com.hazelcast.client.spi.impl.ClientInvocationServiceImpl;
import com.hazelcast.client.spi.impl.ClientListenerServiceImpl;
import com.hazelcast.cluster.client.ClientPingRequest;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.Member;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.SocketInterceptor;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.nio.tcp.IOSelector;
import com.hazelcast.nio.tcp.IOSelectorOutOfMemoryHandler;
import com.hazelcast.nio.tcp.InSelectorImpl;
import com.hazelcast.nio.tcp.OutSelectorImpl;
import com.hazelcast.nio.tcp.SocketChannelWrapper;
import com.hazelcast.nio.tcp.SocketChannelWrapperFactory;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.UsernamePasswordCredentials;
import com.hazelcast.spi.exception.RetryableIOException;
import com.hazelcast.spi.impl.SerializableCollection;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;

import java.io.IOException;
import java.net.Socket;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.client.config.ClientProperties.PROP_HEARTBEAT_INTERVAL_DEFAULT;
import static com.hazelcast.client.config.ClientProperties.PROP_HEARTBEAT_TIMEOUT_DEFAULT;
import static com.hazelcast.client.config.SocketOptions.DEFAULT_BUFFER_SIZE_BYTE;
import static com.hazelcast.client.config.SocketOptions.KILO_BYTE;

public class ClientConnectionManagerImpl implements ClientConnectionManager {

    private static final int RETRY_COUNT = 20;
    private static final ILogger LOGGER = Logger.getLogger(ClientConnectionManagerImpl.class);

    private final static IOSelectorOutOfMemoryHandler OUT_OF_MEMORY_HANDLER = new IOSelectorOutOfMemoryHandler() {
        @Override
        public void handle(OutOfMemoryError error) {
            LOGGER.severe(error);
        }
    };

    private final int connectionTimeout;
    private final int heartBeatInterval;
    private final int heartBeatTimeout;

    private final ConcurrentMap<Address, Object> connectionLockMap = new ConcurrentHashMap<Address, Object>();

    private final AtomicInteger connectionIdGen = new AtomicInteger();
    private final HazelcastClient client;
    private final Router router;
    private SocketInterceptor socketInterceptor;
    private final SocketOptions socketOptions;
    private final IOSelector inSelector;
    private final IOSelector outSelector;
    private final boolean smartRouting;
    private final OwnerConnectionFuture ownerConnectionFuture = new OwnerConnectionFuture();

    private final Credentials credentials;
    private volatile ClientPrincipal principal;
    private final AtomicInteger callIdIncrementer = new AtomicInteger();
    private final SocketChannelWrapperFactory socketChannelWrapperFactory;
    private final ClientExecutionServiceImpl executionService;
    private ClientInvocationServiceImpl invocationService;
    private final AddressTranslator addressTranslator;

    private final ConcurrentMap<Address, ClientConnection> connections
            = new ConcurrentHashMap<Address, ClientConnection>();

    private volatile boolean live;

    public ClientConnectionManagerImpl(HazelcastClient client,
                                       LoadBalancer loadBalancer,
                                       AddressTranslator addressTranslator) {
        this.client = client;
        this.addressTranslator = addressTranslator;
        final ClientConfig config = client.getClientConfig();
        final ClientNetworkConfig networkConfig = config.getNetworkConfig();

        final int connTimeout = networkConfig.getConnectionTimeout();
        connectionTimeout = connTimeout == 0 ? Integer.MAX_VALUE : connTimeout;

        final ClientProperties clientProperties = client.getClientProperties();
        int timeout = clientProperties.getHeartbeatTimeout().getInteger();
        this.heartBeatTimeout = timeout > 0 ? timeout : Integer.parseInt(PROP_HEARTBEAT_TIMEOUT_DEFAULT);

        int interval = clientProperties.getHeartbeatInterval().getInteger();
        heartBeatInterval = interval > 0 ? interval : Integer.parseInt(PROP_HEARTBEAT_INTERVAL_DEFAULT);

        smartRouting = networkConfig.isSmartRouting();
        executionService = (ClientExecutionServiceImpl) client.getClientExecutionService();
        credentials = initCredentials(config);
        router = new Router(loadBalancer);

        inSelector = new InSelectorImpl(
                client.getThreadGroup(),
                "InSelector",
                Logger.getLogger(InSelectorImpl.class),
                OUT_OF_MEMORY_HANDLER);
        outSelector = new OutSelectorImpl(
                client.getThreadGroup(),
                "OutSelector",
                Logger.getLogger(OutSelectorImpl.class),
                OUT_OF_MEMORY_HANDLER);

        socketOptions = networkConfig.getSocketOptions();
        ClientExtension clientExtension = client.getClientExtension();
        socketChannelWrapperFactory = clientExtension.getSocketChannelWrapperFactory();
        socketInterceptor = initSocketInterceptor(networkConfig.getSocketInterceptorConfig());
    }

    private Credentials initCredentials(ClientConfig config) {
        final GroupConfig groupConfig = config.getGroupConfig();
        final ClientSecurityConfig securityConfig = config.getSecurityConfig();
        Credentials c = securityConfig.getCredentials();
        if (c == null) {
            final String credentialsClassname = securityConfig.getCredentialsClassname();
            if (credentialsClassname != null) {
                try {
                    c = ClassLoaderUtil.newInstance(config.getClassLoader(), credentialsClassname);
                } catch (Exception e) {
                    throw ExceptionUtil.rethrow(e);
                }
            }
        }
        if (c == null) {
            c = new UsernamePasswordCredentials(groupConfig.getName(), groupConfig.getPassword());
        }
        return c;
    }

    private SocketInterceptor initSocketInterceptor(SocketInterceptorConfig sic) {
        if (sic != null && sic.isEnabled()) {
            ClientExtension clientExtension = client.getClientExtension();
            return clientExtension.getSocketInterceptor();
        }
        return null;
    }

    @Override
    public boolean isLive() {
        return live;
    }

    private SerializationService getSerializationService() {
        return client.getSerializationService();
    }

    @Override
    public synchronized void start() {
        if (live) {
            return;
        }
        live = true;
        inSelector.start();
        outSelector.start();
        invocationService = (ClientInvocationServiceImpl) client.getInvocationService();
        HeartBeat heartBeat = new HeartBeat();
        executionService.scheduleWithFixedDelay(heartBeat, heartBeatInterval, heartBeatInterval, TimeUnit.MILLISECONDS);
    }

    @Override
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
        connectionLockMap.clear();
    }

    @Override
    public void onCloseOwnerConnection() {
        //mark the owner connection as closed so that operations requiring owner connection can be waited.
        ownerConnectionFuture.markAsClosed();
    }

    @Override
    public ClientConnection ownerConnection(Address address) throws Exception {
        final Address translatedAddress = addressTranslator.translate(address);
        if (translatedAddress == null) {
            throw new RetryableIOException(address + " can not be translated! ");
        }
        return ownerConnectionFuture.createNew(translatedAddress);
    }

    @Override
    public ClientConnection connectToAddress(Address target) throws Exception {
        Authenticator authenticator = new ClusterAuthenticator();
        int count = 0;
        IOException lastError = null;
        while (count < RETRY_COUNT) {
            try {
                return getOrConnect(target, authenticator);
            } catch (IOException e) {
                lastError = e;
            }
            count++;
        }
        throw lastError;
    }

    @Override
    public ClientConnection tryToConnect(Address target) throws Exception {
        Authenticator authenticator = new ClusterAuthenticator();
        int count = 0;
        IOException lastError = null;
        while (count < RETRY_COUNT) {
            try {
                if (target == null || !isMember(target)) {
                    Address address = getAddressFromLoadBalancer();
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

    private Address getAddressFromLoadBalancer() {
        Address address = router.next();
        if (address == null) {
            Set<Member> members = client.getCluster().getMembers();
            String msg;
            if (members.isEmpty()) {
                msg = "No address was return by the LoadBalancer since there are no members in the cluster";
            } else {
                msg = "No address was return by the LoadBalancer. "
                        + "But the cluster contains the following members:" + members;
            }
            throw new IllegalStateException(msg);
        }

        return address;
    }

    @Override
    public String getUuid() {
        final ClientPrincipal cp = principal;
        return cp != null ? cp.getUuid() : null;
    }

    private boolean isMember(Address target) {
        final ClientClusterService clientClusterService = client.getClientClusterService();
        return clientClusterService.getMember(target) != null;
    }

    private ClientConnection getOrConnect(Address target, Authenticator authenticator) throws Exception {
        if (!smartRouting) {
            target = ownerConnectionFuture.getOrWaitForCreation().getEndPoint();
        }

        Address address = addressTranslator.translate(target);

        if (address == null) {
            throw new IOException("Address is required!");
        }

        ClientConnection clientConnection = connections.get(address);
        if (clientConnection == null) {
            final Object lock = getLock(address);
            synchronized (lock) {
                clientConnection = connections.get(address);
                if (clientConnection == null) {
                    final ConnectionProcessor connectionProcessor = new ConnectionProcessor(address, authenticator, false);
                    final ICompletableFuture<ClientConnection> future = executionService.submitInternal(connectionProcessor);
                    try {
                        clientConnection = future.get(connectionTimeout, TimeUnit.MILLISECONDS);
                    } catch (Exception e) {
                        future.cancel(true);
                        throw new RetryableIOException(e);
                    }
                    ClientConnection current = connections.putIfAbsent(address, clientConnection);
                    if (current != null) {
                        clientConnection.close();
                        clientConnection = current;
                    }
                }
            }
        }
        return clientConnection;
    }

    private final class ConnectionProcessor implements Callable<ClientConnection> {

        final Address address;
        final Authenticator authenticator;
        final boolean isBlock;

        private ConnectionProcessor(final Address address, final Authenticator authenticator, final boolean isBlock) {
            this.address = address;
            this.authenticator = authenticator;
            this.isBlock = isBlock;
        }

        @Override
        public ClientConnection call() throws Exception {
            if (!live) {
                throw new HazelcastException("ConnectionManager is not active!!!");
            }
            SocketChannel socketChannel = null;
            try {
                socketChannel = SocketChannel.open();
                Socket socket = socketChannel.socket();
                socket.setKeepAlive(socketOptions.isKeepAlive());
                socket.setTcpNoDelay(socketOptions.isTcpNoDelay());
                socket.setReuseAddress(socketOptions.isReuseAddress());
                if (socketOptions.getLingerSeconds() > 0) {
                    socket.setSoLinger(true, socketOptions.getLingerSeconds());
                }
                int bufferSize = socketOptions.getBufferSize() * KILO_BYTE;
                if (bufferSize < 0) {
                    bufferSize = DEFAULT_BUFFER_SIZE_BYTE;
                }
                socket.setSendBufferSize(bufferSize);
                socket.setReceiveBufferSize(bufferSize);
                socketChannel.socket().connect(address.getInetSocketAddress(), connectionTimeout);
                SocketChannelWrapper socketChannelWrapper = socketChannelWrapperFactory.wrapSocketChannel(socketChannel, true);
                final ClientConnection clientConnection = new ClientConnection(ClientConnectionManagerImpl.this, inSelector,
                        outSelector, connectionIdGen.incrementAndGet(), socketChannelWrapper,
                        executionService, invocationService, client.getSerializationService());
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
            } catch (Exception e) {
                if (socketChannel != null) {
                    socketChannel.close();
                }
                throw ExceptionUtil.rethrow(e);
            }
        }
    }

    @Override
    public void onConnectionClose(ClientConnection clientConnection) {
        Address endpoint = clientConnection.getRemoteEndpoint();
        if (endpoint != null) {
            connections.remove(clientConnection.getRemoteEndpoint());
            ownerConnectionFuture.closeIfAddressMatches(endpoint);
        }
    }

    @Override
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

    @Override
    public void handlePacket(Packet packet) {
        final ClientConnection conn = (ClientConnection) packet.getConn();
        conn.incrementPacketCount();
        if (packet.isHeaderSet(Packet.HEADER_EVENT)) {
            final ClientListenerServiceImpl listenerService = (ClientListenerServiceImpl) client.getListenerService();
            listenerService.handleEventPacket(packet);
        } else {
            invocationService.handlePacket(packet);
        }
    }

    @Override
    public int newCallId() {
        return callIdIncrementer.incrementAndGet();
    }

    public class ManagerAuthenticator implements Authenticator {

        @Override
        public void auth(ClientConnection connection) throws AuthenticationException, IOException {
            final Object response = authenticate(connection, credentials, principal, true);
            principal = (ClientPrincipal) response;
        }
    }

    private class ClusterAuthenticator implements Authenticator {
        @Override
        public void auth(ClientConnection connection) throws AuthenticationException, IOException {
            authenticate(connection, credentials, principal, false);
        }
    }

    private Object authenticate(ClientConnection connection, Credentials credentials, ClientPrincipal principal
            , boolean firstConnection) throws IOException {
        final SerializationService ss = getSerializationService();
        AuthenticationRequest auth = new AuthenticationRequest(credentials, principal);
        connection.init();
        auth.setOwnerConnection(firstConnection);
        //contains remoteAddress and principal
        SerializableCollection collectionWrapper;
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

    @Override
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

    private Object getLock(Address address) {
        Object lock = connectionLockMap.get(address);
        if (lock == null) {
            lock = new Object();
            Object current = connectionLockMap.putIfAbsent(address, lock);
            if (current != null) {
                lock = current;
            }
        }
        return lock;
    }

    class HeartBeat implements Runnable {

        public void run() {
            if (!live) {
                return;
            }
            final long now = Clock.currentTimeMillis();
            for (ClientConnection connection : connections.values()) {
                if (now - connection.lastReadTime() > heartBeatTimeout) {
                    connection.heartBeatingFailed();
                }
                if (now - connection.lastReadTime() > heartBeatInterval) {
                    final ClientPingRequest request = new ClientPingRequest();
                    invocationService.send(request, connection);
                } else {
                    connection.heartBeatingSucceed();
                }
            }
        }
    }

    public void removeEndpoint(Address address) {
        final ClientConnection clientConnection = connections.get(address);
        if (clientConnection != null) {
            clientConnection.close();
        }
    }

    @Override
    public void onDetectingUnresponsiveConnection(ClientConnection connection) {
        if (smartRouting) {
            //closing the owner connection if unresponsive so that it can be switched to a healthy one.
            ownerConnectionFuture.closeIfAddressMatches(connection.getEndPoint());
            // we do not close connection itself since we will continue to send heartbeat ping to this connection.
            // IOUtil.closeResource(connection);
            return;
        }

        //close both owner and operation connection
        ownerConnectionFuture.close();
        IOUtil.closeResource(connection);
    }

    private class OwnerConnectionFuture {

        private final Object ownerConnectionLock = new Object();
        private volatile ClientConnection ownerConnection;

        private ClientConnection getOrWaitForCreation() throws IOException {
            ClientNetworkConfig networkConfig = client.getClientConfig().getNetworkConfig();
            long connectionAttemptLimit = networkConfig.getConnectionAttemptLimit();
            long connectionAttemptPeriod = networkConfig.getConnectionAttemptPeriod();
            long waitTime = connectionAttemptLimit * connectionAttemptPeriod * 2;
            if (waitTime < 0) {
                waitTime = Long.MAX_VALUE;
            }

            final ClientConnection currentOwnerConnection = ownerConnection;
            if (currentOwnerConnection != null) {
                return currentOwnerConnection;
            }
            synchronized (ownerConnectionLock) {
                long endTime = System.currentTimeMillis() + waitTime;
                while (ownerConnection == null && endTime > System.currentTimeMillis()) {
                    try {
                        ownerConnectionLock.wait(waitTime);
                    } catch (InterruptedException e) {
                        throw new IOException(e);
                    }
                }
                if (ownerConnection == null) {
                    LOGGER.warning("Wait for owner connection is timed out");
                    throw new IOException("Wait for owner connection is timed out");
                }
                return ownerConnection;
            }
        }

        private ClientConnection createNew(Address address) throws RetryableIOException {
            final ManagerAuthenticator authenticator = new ManagerAuthenticator();
            final ConnectionProcessor connectionProcessor = new ConnectionProcessor(address, authenticator, true);
            ICompletableFuture<ClientConnection> future = executionService.submitInternal(connectionProcessor);
            try {
                ClientConnection conn = future.get(connectionTimeout, TimeUnit.MILLISECONDS);
                synchronized (ownerConnectionLock) {
                    ownerConnection = conn;
                    ownerConnectionLock.notifyAll();
                }
                return conn;
            } catch (Exception e) {
                future.cancel(true);
                throw new RetryableIOException(e);
            }
        }

        private void markAsClosed() {
            ownerConnection = null;
        }

        private void closeIfAddressMatches(Address address) {
            final ClientConnection currentOwnerConnection = ownerConnection;
            if (currentOwnerConnection == null || !currentOwnerConnection.live()) {
                return;
            }
            if (address.equals(currentOwnerConnection.getRemoteEndpoint())) {
                close();
            }
        }

        private void close() {
            final ClientConnection currentOwnerConnection = ownerConnection;
            if (currentOwnerConnection == null) {
                return;
            }

            IOUtil.closeResource(currentOwnerConnection);
            markAsClosed();
        }
    }
}
