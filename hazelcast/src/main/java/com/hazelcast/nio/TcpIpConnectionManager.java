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

package com.hazelcast.nio;

import com.hazelcast.cluster.BindOperation;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationContext;
import com.hazelcast.nio.ssl.BasicSSLContextFactory;
import com.hazelcast.nio.ssl.SSLContextFactory;
import com.hazelcast.nio.ssl.SSLSocketChannelWrapper;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

import java.io.IOException;
import java.net.Socket;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.Collections;
import java.util.Set;
import java.util.LinkedList;
import java.util.Collection;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

public class TcpIpConnectionManager implements ConnectionManager {

    final int socketReceiveBufferSize;

    final IOService ioService;

    final int socketSendBufferSize;

    private final ConstructorFunction<Address, ConnectionMonitor> monitorConstructor
            = new ConstructorFunction<Address, ConnectionMonitor>() {
        public ConnectionMonitor createNew(Address endpoint) {
            return new ConnectionMonitor(TcpIpConnectionManager.this, endpoint);
        }
    };

    private final ILogger logger;

    private final int socketLingerSeconds;

    private final boolean socketKeepAlive;

    private final boolean socketNoDelay;

    private final ConcurrentMap<Address, Connection> connectionsMap = new ConcurrentHashMap<Address, Connection>(100);

    private final ConcurrentMap<Address, ConnectionMonitor> monitors = new ConcurrentHashMap<Address, ConnectionMonitor>(100);

    private final Set<Address> connectionsInProgress = Collections.newSetFromMap(new ConcurrentHashMap<Address, Boolean>());

    private final Set<ConnectionListener> connectionListeners = new CopyOnWriteArraySet<ConnectionListener>();

    private final Set<SocketChannelWrapper> acceptedSockets =
            Collections.newSetFromMap(new ConcurrentHashMap<SocketChannelWrapper, Boolean>());

    private final Set<TcpIpConnection> activeConnections =
            Collections.newSetFromMap(new ConcurrentHashMap<TcpIpConnection, Boolean>());

    private final AtomicInteger allTextConnections = new AtomicInteger();

    private final AtomicInteger connectionIdGen = new AtomicInteger();

    private volatile boolean live;

    private final ServerSocketChannel serverSocketChannel;

    private final int selectorThreadCount;

    private final IOSelector[] inSelectors;

    private final IOSelector[] outSelectors;

    private final AtomicInteger nextSelectorIndex = new AtomicInteger();

    private final MemberSocketInterceptor memberSocketInterceptor;

    private final SocketChannelWrapperFactory socketChannelWrapperFactory;

    private final int outboundPortCount;

    private final LinkedList<Integer> outboundPorts = new LinkedList<Integer>();
    // accessed only in synchronized block

    private final SerializationContext serializationContext;

    private volatile Thread socketAcceptorThread;
    // accessed only in synchronized block

    public TcpIpConnectionManager(IOService ioService, ServerSocketChannel serverSocketChannel) {
        this.ioService = ioService;
        this.serverSocketChannel = serverSocketChannel;
        this.logger = ioService.getLogger(TcpIpConnectionManager.class.getName());
        this.socketReceiveBufferSize = ioService.getSocketReceiveBufferSize() * IOService.KILO_BYTE;
        this.socketSendBufferSize = ioService.getSocketSendBufferSize() * IOService.KILO_BYTE;
        this.socketLingerSeconds = ioService.getSocketLingerSeconds();
        this.socketKeepAlive = ioService.getSocketKeepAlive();
        this.socketNoDelay = ioService.getSocketNoDelay();
        selectorThreadCount = ioService.getSelectorThreadCount();
        inSelectors = new IOSelector[selectorThreadCount];
        outSelectors = new IOSelector[selectorThreadCount];
        final Collection<Integer> ports = ioService.getOutboundPorts();
        outboundPortCount = ports == null ? 0 : ports.size();
        if (ports != null) {
            outboundPorts.addAll(ports);
        }
        SSLConfig sslConfig = ioService.getSSLConfig();
        if (sslConfig != null && sslConfig.isEnabled()) {
            socketChannelWrapperFactory = new SSLSocketChannelWrapperFactory(sslConfig);
            logger.info("SSL is enabled");
        } else {
            socketChannelWrapperFactory = new DefaultSocketChannelWrapperFactory();
        }

        SocketInterceptor implementation = null;
        SocketInterceptorConfig sic = ioService.getSocketInterceptorConfig();
        if (sic != null && sic.isEnabled()) {
            implementation = (SocketInterceptor) sic.getImplementation();
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
                }
            }
        }

        memberSocketInterceptor = (MemberSocketInterceptor) implementation;
        if (memberSocketInterceptor != null) {
            logger.info("SocketInterceptor is enabled");
            memberSocketInterceptor.init(sic.getProperties());
        }
        serializationContext = ioService.getSerializationContext();
    }

    @Override
    public int getActiveConnectionCount() {
        return activeConnections.size();
    }

    public int getAllTextConnections() {
        return allTextConnections.get();
    }

    @Override
    public int getConnectionCount() {
        return connectionsMap.size();
    }

    @Override
    public boolean isSSLEnabled() {
        return socketChannelWrapperFactory instanceof SSLSocketChannelWrapperFactory;
    }

    public void incrementTextConnections() {
        allTextConnections.incrementAndGet();
    }

    public SerializationContext getSerializationContext() {
        return serializationContext;
    }

    interface SocketChannelWrapperFactory {
        SocketChannelWrapper wrapSocketChannel(SocketChannel socketChannel, boolean client) throws Exception;
    }

    static class DefaultSocketChannelWrapperFactory implements SocketChannelWrapperFactory {
        public SocketChannelWrapper wrapSocketChannel(SocketChannel socketChannel, boolean client) throws Exception {
            return new DefaultSocketChannelWrapper(socketChannel);
        }
    }

    class SSLSocketChannelWrapperFactory implements SocketChannelWrapperFactory {
        final SSLContextFactory sslContextFactory;

        SSLSocketChannelWrapperFactory(SSLConfig sslConfig) {
            if (CipherHelper.isSymmetricEncryptionEnabled(ioService)) {
                throw new RuntimeException("SSL and SymmetricEncryption cannot be both enabled!");
            }
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

        public SocketChannelWrapper wrapSocketChannel(SocketChannel socketChannel, boolean client) throws Exception {
            return new SSLSocketChannelWrapper(sslContextFactory.getSSLContext(), socketChannel, client);
        }
    }

    public IOService getIOHandler() {
        return ioService;
    }

    public MemberSocketInterceptor getMemberSocketInterceptor() {
        return memberSocketInterceptor;
    }

    public void addConnectionListener(ConnectionListener listener) {
        connectionListeners.add(listener);
    }

    public boolean bind(TcpIpConnection connection, Address remoteEndPoint, Address localEndpoint, final boolean replyBack) {
        if (logger.isFinestEnabled()) {
            log(Level.FINEST, "Binding " + connection + " to " + remoteEndPoint + ", replyBack is " + replyBack);
        }
        final Address thisAddress = ioService.getThisAddress();
        if (!connection.isClient() && !thisAddress.equals(localEndpoint)) {
            log(Level.WARNING, "Wrong bind request from " + remoteEndPoint
                    + "! This node is not requested endpoint: " + localEndpoint);
            connection.close();
            return false;
        }
        connection.setEndPoint(remoteEndPoint);
        if (replyBack) {
            sendBindRequest(connection, remoteEndPoint, false);
        }
        final Connection existingConnection = connectionsMap.get(remoteEndPoint);
        if (existingConnection != null && existingConnection.live()) {
            if (existingConnection != connection) {
                if (logger.isFinestEnabled()) {
                    log(Level.FINEST, existingConnection + " is already bound  to "
                            + remoteEndPoint + ", new one is " + connection);
                }
                activeConnections.add(connection);
            }
            return false;
        }
        if (!remoteEndPoint.equals(thisAddress)) {
            if (!connection.isClient()) {
                connection.setMonitor(getConnectionMonitor(remoteEndPoint, true));
            }
            connectionsMap.put(remoteEndPoint, connection);
            connectionsInProgress.remove(remoteEndPoint);
            for (ConnectionListener listener : connectionListeners) {
                listener.connectionAdded(connection);
            }
            return true;
        }
        return false;
    }

    void sendBindRequest(final TcpIpConnection connection, final Address remoteEndPoint, final boolean replyBack) {
        connection.setEndPoint(remoteEndPoint);
        //make sure bind packet is the first packet sent to the end point.
        final BindOperation bind = new BindOperation(ioService.getThisAddress(), remoteEndPoint, replyBack);
        final Data bindData = ioService.toData(bind);
        final Packet packet = new Packet(bindData, serializationContext);
        packet.setHeader(Packet.HEADER_OP);
        connection.write(packet);
        //now you can send anything...
    }

    private int nextSelectorIndex() {
        return Math.abs(nextSelectorIndex.getAndIncrement()) % selectorThreadCount;
    }

    SocketChannelWrapper wrapSocketChannel(SocketChannel socketChannel, boolean client) throws Exception {
        final SocketChannelWrapper socketChannelWrapper = socketChannelWrapperFactory.wrapSocketChannel(socketChannel, client);
        acceptedSockets.add(socketChannelWrapper);
        return socketChannelWrapper;
    }

    TcpIpConnection assignSocketChannel(SocketChannelWrapper channel) {
        final int index = nextSelectorIndex();
        final TcpIpConnection connection = new TcpIpConnection(this, inSelectors[index],
                outSelectors[index], connectionIdGen.incrementAndGet(), channel);
        activeConnections.add(connection);
        acceptedSockets.remove(channel);
        connection.getReadHandler().register();
        log(Level.INFO, channel.socket().getLocalPort() + " accepted socket connection from "
                + channel.socket().getRemoteSocketAddress());
        return connection;
    }

    void failedConnection(Address address, Throwable t, boolean silent) {
        connectionsInProgress.remove(address);
        ioService.onFailedConnection(address);
        if (!silent) {
            getConnectionMonitor(address, false).onError(t);
        }
    }

    public Connection getConnection(Address address) {
        return connectionsMap.get(address);
    }

    public Connection getOrConnect(Address address) {
        return getOrConnect(address, false);
    }

    public Connection getOrConnect(final Address address, final boolean silent) {
        Connection connection = connectionsMap.get(address);
        if (connection == null && live) {
            if (connectionsInProgress.add(address)) {
                ioService.shouldConnectTo(address);
                ioService.executeAsync(new SocketConnector(this, address, silent));
            }
        }
        return connection;
    }


    private ConnectionMonitor getConnectionMonitor(Address endpoint, boolean reset) {
        final ConnectionMonitor monitor = ConcurrencyUtil.getOrPutIfAbsent(monitors, endpoint, monitorConstructor);
        if (reset) {
            monitor.reset();
        }
        return monitor;
    }

    public void destroyConnection(Connection connection) {
        if (connection == null) {
            return;
        }
        if (logger.isFinestEnabled()) {
            log(Level.FINEST, "Destroying " + connection);
        }
        activeConnections.remove(connection);
        final Address endPoint = connection.getEndPoint();
        if (endPoint != null) {
            connectionsInProgress.remove(endPoint);
            final Connection existingConn = connectionsMap.get(endPoint);
            if (existingConn == connection && live) {
                connectionsMap.remove(endPoint);
                for (ConnectionListener listener : connectionListeners) {
                    listener.connectionRemoved(connection);
                }
            }
        }
        if (connection.live()) {
            connection.close();
        }
    }

    protected void initSocket(Socket socket) throws Exception {
        if (socketLingerSeconds > 0) {
            socket.setSoLinger(true, socketLingerSeconds);
        }
        socket.setKeepAlive(socketKeepAlive);
        socket.setTcpNoDelay(socketNoDelay);
        socket.setReceiveBufferSize(socketReceiveBufferSize);
        socket.setSendBufferSize(socketSendBufferSize);
    }

    public synchronized void start() {
        if (live) {
            return;
        }
        live = true;
        log(Level.FINEST, "Starting ConnectionManager and IO selectors.");
        for (int i = 0; i < inSelectors.length; i++) {
            inSelectors[i] = new InSelectorImpl(ioService, i);
            outSelectors[i] = new OutSelectorImpl(ioService, i);
            inSelectors[i].start();
            outSelectors[i].start();
        }
        if (serverSocketChannel != null) {
            if (socketAcceptorThread != null) {
                logger.warning("SocketAcceptor thread is already live! Shutting down old acceptor...");
                shutdownSocketAcceptor();
            }
            Runnable acceptRunnable = new SocketAcceptor(serverSocketChannel, this);
            socketAcceptorThread = new Thread(ioService.getThreadGroup(), acceptRunnable,
                    ioService.getThreadPrefix() + "Acceptor");
            socketAcceptorThread.start();
        }
    }

    public synchronized void restart() {
        stop();
        start();
    }

    public synchronized void shutdown() {
        try {
            if (live) {
                stop();
                connectionListeners.clear();
            }
        } finally {
            if (serverSocketChannel != null) {
                try {
                    if (logger.isFinestEnabled()) {
                        log(Level.FINEST, "Closing server socket channel: " + serverSocketChannel);
                    }
                    serverSocketChannel.close();
                } catch (IOException ignore) {
                    logger.finest(ignore);
                }
            }
        }
    }

    private void stop() {
        live = false;
        log(Level.FINEST, "Stopping ConnectionManager");
        // interrupt acceptor thread after live=false
        shutdownSocketAcceptor();
        for (SocketChannelWrapper socketChannel : acceptedSockets) {
            IOUtil.closeResource(socketChannel);
        }
        for (Connection conn : connectionsMap.values()) {
            try {
                destroyConnection(conn);
            } catch (final Throwable ignore) {
                logger.finest(ignore);
            }
        }
        for (TcpIpConnection conn : activeConnections) {
            try {
                destroyConnection(conn);
            } catch (final Throwable ignore) {
                logger.finest(ignore);
            }
        }
        shutdownIOSelectors();
        connectionsInProgress.clear();
        connectionsMap.clear();
        monitors.clear();
        activeConnections.clear();
    }

    private synchronized void shutdownIOSelectors() {
        if (logger.isFinestEnabled()) {
            log(Level.FINEST, "Shutting down IO selectors... Total: " + selectorThreadCount);
        }
        for (int i = 0; i < selectorThreadCount; i++) {
            IOSelector ioSelector = inSelectors[i];
            if (ioSelector != null) {
                ioSelector.shutdown();
            }
            inSelectors[i] = null;

            ioSelector = outSelectors[i];
            if (ioSelector != null) {
                ioSelector.shutdown();
            }
            outSelectors[i] = null;
        }
    }

    private void shutdownSocketAcceptor() {
        log(Level.FINEST, "Shutting down SocketAcceptor thread.");
        Thread killingThread = socketAcceptorThread;
        if (killingThread == null) {
            return;
        }
        socketAcceptorThread = null;
        killingThread.interrupt();
        try {
            killingThread.join(1000 * 10);
        } catch (InterruptedException e) {
            logger.finest(e);
        }
    }

    public int getCurrentClientConnections() {
        int count = 0;
        for (TcpIpConnection conn : activeConnections) {
            if (conn.live()) {
                if (conn.isClient()) {
                    count++;
                }
            }
        }
        return count;
    }

    public boolean isLive() {
        return live;
    }

    public Map<Address, Connection> getReadonlyConnectionMap() {
        return Collections.unmodifiableMap(connectionsMap);
    }

    private void log(Level level, String message) {
        logger.log(level, message);
        ioService.getSystemLogService().logConnection(message);
    }

    boolean useAnyOutboundPort() {
        return outboundPortCount == 0;
    }

    int getOutboundPortCount() {
        return outboundPortCount;
    }

    int acquireOutboundPort() {
        if (useAnyOutboundPort()) {
            return 0;
        }
        synchronized (outboundPorts) {
            final Integer port = outboundPorts.removeFirst();
            outboundPorts.addLast(port);
            return port;
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Connections {");
        for (Connection conn : connectionsMap.values()) {
            sb.append("\n");
            sb.append(conn);
        }
        sb.append("\nlive=");
        sb.append(live);
        sb.append("\n}");
        return sb.toString();
    }
}
