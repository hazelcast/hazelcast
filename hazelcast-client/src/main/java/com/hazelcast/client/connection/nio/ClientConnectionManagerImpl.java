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
import com.hazelcast.client.AuthenticationRequest;
import com.hazelcast.client.ClientPrincipal;
import com.hazelcast.client.ClientRequest;
import com.hazelcast.client.ClientResponse;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.LoadBalancer;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.config.ClientSecurityConfig;
import com.hazelcast.client.config.SocketOptions;
import com.hazelcast.client.connection.Authenticator;
import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.connection.Router;
import com.hazelcast.client.spi.ClientClusterService;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.spi.impl.ClientCallFuture;
import com.hazelcast.client.spi.impl.ClientExecutionServiceImpl;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.Member;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.ClientPacket;
import com.hazelcast.nio.DefaultSocketChannelWrapper;
import com.hazelcast.nio.IOSelector;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.SocketChannelWrapper;
import com.hazelcast.nio.SocketInterceptor;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationContext;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.nio.ssl.BasicSSLContextFactory;
import com.hazelcast.nio.ssl.SSLContextFactory;
import com.hazelcast.nio.ssl.SSLSocketChannelWrapper;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.UsernamePasswordCredentials;
import com.hazelcast.spi.exception.RetryableIOException;
import com.hazelcast.spi.impl.SerializableCollection;
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

public class ClientConnectionManagerImpl implements ClientConnectionManager {

    private static final ILogger logger = Logger.getLogger(ClientConnectionManagerImpl.class);

    private int RETRY_COUNT = 20;
    private final ConcurrentMap<Address, Object> connectionLockMap = new ConcurrentHashMap<Address, Object>();

    static final int KILO_BYTE = 1024;
    public static final int BUFFER_SIZE = 16 << 10; // 32k


    private final AtomicInteger connectionIdGen = new AtomicInteger();
    private final HazelcastClient client;
    private final Router router;
    private final SocketInterceptor socketInterceptor;
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
    private final ConcurrentMap<Address, ClientConnection> connections
            = new ConcurrentHashMap<Address, ClientConnection>();

    private volatile boolean live = false;

    public ClientConnectionManagerImpl(HazelcastClient client, LoadBalancer loadBalancer) {
        this.client = client;
        final ClientConfig config = client.getClientConfig();
        final ClientNetworkConfig networkConfig = config.getNetworkConfig();
        final GroupConfig groupConfig = config.getGroupConfig();
        final ClientSecurityConfig securityConfig = config.getSecurityConfig();
        Credentials c = securityConfig.getCredentials();
        if (c == null) {
            final String credentialsClassname = securityConfig.getCredentialsClassname();
            //todo: Should be moved to a reflection utility.
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

        this.smartRouting = networkConfig.isSmartRouting();
        this.executionService = (ClientExecutionServiceImpl) client.getClientExecutionService();
        this.credentials = c;
        router = new Router(loadBalancer);
        inSelector = new ClientInSelectorImpl(client.getThreadGroup());
        outSelector = new ClientOutSelectorImpl(client.getThreadGroup());
        //init socketInterceptor
        SocketInterceptorConfig sic = networkConfig.getSocketInterceptorConfig();
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

        socketOptions = networkConfig.getSocketOptions();

        SSLConfig sslConfig = networkConfig.getSSLConfig(); //ioService.getSSLConfig(); TODO
        if (sslConfig != null && sslConfig.isEnabled()) {
            socketChannelWrapperFactory = new SSLSocketChannelWrapperFactory(sslConfig);
            logger.info("SSL is enabled");
        } else {
            socketChannelWrapperFactory = new DefaultSocketChannelWrapperFactory();
        }

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

    @Override
    public synchronized void start() {
        if (live) {
            return;
        }
        live = true;
        inSelector.start();
        outSelector.start();
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


    public void markOwnerAddressAsClosed() {
        ownerConnectionFuture.markAsClosed();
    }

    @Override
    public ClientConnection ownerConnection(Address address) throws Exception {
        return ownerConnectionFuture.createNew(address);
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
                msg = "No address was return by the LoadBalancer. " +
                        "But the cluster contains the following members:" + members;
            }
            throw new IllegalStateException(msg);
        }

        return address;
    }

    public ClientPrincipal getPrincipal() {
        return principal;
    }

    private boolean isMember(Address target) {
        final ClientClusterService clientClusterService = client.getClientClusterService();
        return clientClusterService.getMember(target) != null;
    }

    private ClientConnection getOrConnect(Address address, Authenticator authenticator) throws Exception {
        if (address == null) {
            throw new NullPointerException("Address is required!");
        }
        if (!smartRouting) {
            address = ownerConnectionFuture.getOrWaitForCreation().getEndPoint();
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
                        clientConnection = future.get(5, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        future.cancel(true);
                        throw new RetryableIOException(e);
                    }
                    ClientConnection current = connections.putIfAbsent(clientConnection.getRemoteEndpoint(), clientConnection);
                    if (current != null) {
                        clientConnection.innerClose();
                        clientConnection = current;
                    }
                }
            }
        }
        return clientConnection;
    }

    private class ConnectionProcessor implements Callable<ClientConnection> {

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
                    bufferSize = BUFFER_SIZE;
                }
                socket.setSendBufferSize(bufferSize);
                socket.setReceiveBufferSize(bufferSize);
                socketChannel.socket().connect(address.getInetSocketAddress(), 5000);
                SocketChannelWrapper socketChannelWrapper = socketChannelWrapperFactory.wrapSocketChannel(socketChannel, true);
                final ClientConnection clientConnection = new ClientConnection(ClientConnectionManagerImpl.this, inSelector,
                        outSelector, connectionIdGen.incrementAndGet(), socketChannelWrapper, executionService);
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

    public void destroyConnection(ClientConnection clientConnection) {
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

    public void handlePacket(ClientPacket packet) {
        final ClientConnection conn = (ClientConnection) packet.getConn();
        conn.incrementPacketCount();
        executionService.executeInternal(new ClientPacketProcessor(packet));
    }

    public int newCallId() {
        return callIdIncrementer.incrementAndGet();
    }

    private class ClientEventProcessor implements Runnable {
        final int callId;
        final ClientConnection conn;
        final Data response;

        private ClientEventProcessor(int callId, ClientConnection conn, Data response) {
            this.callId = callId;
            this.conn = conn;
            this.response = response;
        }

        @Override
        public void run() {
            handleEvent(response, callId, conn);
        }

        private void handleEvent(Data event, int callId, ClientConnection conn) {
            final EventHandler eventHandler = conn.getEventHandler(callId);
            final Object eventObject = getSerializationService().toObject(event);
            if (eventHandler == null) {
                logger.warning("No eventHandler for callId: " + callId + ", event: " + eventObject + ", conn: " + conn);
                return;
            }
            eventHandler.handle(eventObject);
        }
    }

    private class ClientPacketProcessor implements Runnable {

        final ClientPacket packet;

        ClientPacketProcessor(ClientPacket packet) {
            this.packet = packet;
        }

        @Override
        public void run() {
            final ClientConnection conn = (ClientConnection) packet.getConn();
            final ClientResponse clientResponse = getSerializationService().toObject(packet.getData());
            final int callId = clientResponse.getCallId();
            final Data response = clientResponse.getResponse();
            if (clientResponse.isEvent()) {
                executionService.execute(new ClientEventProcessor(callId, conn, response));
            } else {
                handlePacket(response, clientResponse.isError(), callId, conn);
            }
            conn.decrementPacketCount();
        }

        private void handlePacket(Object response, boolean isError, int callId, ClientConnection conn) {
            final ClientCallFuture future = conn.deRegisterCallId(callId);
            if (future == null) {
                logger.warning("No call for callId: " + callId + ", response: " + response);
                return;
            }
            if (isError) {
                response = getSerializationService().toObject(response);
            }
            future.notify(response);
        }
    }


    public class ManagerAuthenticator implements Authenticator {

        @Override
        public void auth(ClientConnection connection) throws AuthenticationException, IOException {
            final Object response = authenticate(connection, credentials, principal, true, true);
            principal = (ClientPrincipal) response;
        }
    }

    private class ClusterAuthenticator implements Authenticator {
        @Override
        public void auth(ClientConnection connection) throws AuthenticationException, IOException {
            authenticate(connection, credentials, principal, false, false);
        }
    }

    private Object authenticate(ClientConnection connection, Credentials credentials, ClientPrincipal principal,
                                boolean reAuth, boolean firstConnection) throws IOException {
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

    interface SocketChannelWrapperFactory {
        SocketChannelWrapper wrapSocketChannel(SocketChannel socketChannel, boolean client) throws Exception;
    }

    static class DefaultSocketChannelWrapperFactory implements SocketChannelWrapperFactory {
        @Override
        public SocketChannelWrapper wrapSocketChannel(SocketChannel socketChannel, boolean client) throws Exception {
            return new DefaultSocketChannelWrapper(socketChannel);
        }
    }

    static class SSLSocketChannelWrapperFactory implements SocketChannelWrapperFactory {
        final SSLContextFactory sslContextFactory;

        SSLSocketChannelWrapperFactory(SSLConfig sslConfig) {
//            if (CipherHelper.isSymmetricEncryptionEnabled(ioService)) {
//                throw new RuntimeException("SSL and SymmetricEncryption cannot be both enabled!");
//            }
            SSLContextFactory sslContextFactoryObject = (SSLContextFactory) sslConfig.getFactoryImplementation();
            try {
                String factoryClassName = sslConfig.getFactoryClassName();
                if (sslContextFactoryObject == null && factoryClassName != null) {
                    sslContextFactoryObject = (SSLContextFactory) Class.forName(factoryClassName).newInstance();
                }
                if (sslContextFactoryObject == null) {
                    sslContextFactoryObject = new BasicSSLContextFactory();
                }
                sslContextFactoryObject.init(sslConfig.getProperties());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            sslContextFactory = sslContextFactoryObject;
        }

        @Override
        public SocketChannelWrapper wrapSocketChannel(SocketChannel socketChannel, boolean client) throws Exception {
            return new SSLSocketChannelWrapper(sslContextFactory.getSSLContext(), socketChannel, client);
        }
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

    private class OwnerConnectionFuture {

        private final Object ownerConnectionLock = new Object();
        private volatile ClientConnection ownerConnection;

        private ClientConnection getOrWaitForCreation() throws RetryableIOException {
            ClientNetworkConfig networkConfig = client.getClientConfig().getNetworkConfig();
            int connectionAttemptLimit = networkConfig.getConnectionAttemptLimit();
            int connectionAttemptPeriod = networkConfig.getConnectionAttemptPeriod();
            int waitTime = connectionAttemptLimit * connectionAttemptPeriod * 2;
            synchronized (ownerConnectionLock) {
                while (ownerConnection == null) {
                    try {
                        ownerConnectionLock.wait(waitTime);
                    } catch (InterruptedException e) {
                        logger.warning("Wait for owner connection is timed out");
                        throw new RetryableIOException(e);
                    }
                }
                return ownerConnection;
            }
        }

        private ClientConnection createNew(Address address) throws RetryableIOException {
            final ManagerAuthenticator authenticator = new ManagerAuthenticator();
            final ConnectionProcessor connectionProcessor = new ConnectionProcessor(address, authenticator, true);
            ICompletableFuture<ClientConnection> future = executionService.submitInternal(connectionProcessor);
            try {
                synchronized (ownerConnectionLock) {
                    ownerConnection = future.get(5, TimeUnit.SECONDS);
                    ownerConnectionLock.notifyAll();
                }
                return ownerConnection;
            } catch (Exception e) {
                future.cancel(true);
                throw new RetryableIOException(e);
            }
        }

        private void markAsClosed() {
            synchronized (ownerConnectionLock) {
                ownerConnection = null;
            }
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
