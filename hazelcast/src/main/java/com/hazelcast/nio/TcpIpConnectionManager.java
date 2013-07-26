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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

public class TcpIpConnectionManager implements ConnectionManager {

    private final ILogger logger;

    final int socketReceiveBufferSize;

    final int socketSendBufferSize;

    private final int socketLingerSeconds;

    private final boolean socketKeepAlive;

    private final boolean socketNoDelay;

    private final ConcurrentMap<Address, Connection> mapConnections = new ConcurrentHashMap<Address, Connection>(100);

    private final ConcurrentMap<Address, ConnectionMonitor> mapMonitors = new ConcurrentHashMap<Address, ConnectionMonitor>(100);

    private final Set<Address> setConnectionInProgress = Collections.newSetFromMap(new ConcurrentHashMap<Address, Boolean>());

    private final Set<ConnectionListener> setConnectionListeners = new CopyOnWriteArraySet<ConnectionListener>();

    private final Set<TcpIpConnection> setActiveConnections = Collections.newSetFromMap(new ConcurrentHashMap<TcpIpConnection, Boolean>());

    private final AtomicInteger allTextConnections = new AtomicInteger();

    private final AtomicInteger connectionIdGen = new AtomicInteger();

    private volatile boolean live = false;

    final IOService ioService;

    private final ServerSocketChannel serverSocketChannel;

    private final int selectorThreadCount;

    private final IOSelector[] inSelectors;

    private final IOSelector[] outSelectors;

    private final AtomicInteger nextSelectorIndex = new AtomicInteger();

    private final MemberSocketInterceptor memberSocketInterceptor;

    private final SocketChannelWrapperFactory socketChannelWrapperFactory;

    private final int outboundPortCount;

    private final LinkedList<Integer> outboundPorts = new LinkedList<Integer>();  // accessed only in synchronized block

    private final SerializationContext serializationContext;

    private Thread socketAcceptorThread; // accessed only in synchronized block

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
        SocketInterceptorConfig sic = ioService.getSocketInterceptorConfig();
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
                    logger.severe( "SocketInterceptor must be instance of " + MemberSocketInterceptor.class.getName());
                    implementation = null;
                } else {
                    logger.info("SocketInterceptor is enabled");
                }
            }
            if (implementation != null) {
                memberSocketInterceptor = (MemberSocketInterceptor) implementation;
                memberSocketInterceptor.init(sic);
            } else {
                memberSocketInterceptor = null;
            }
        } else {
            memberSocketInterceptor = null;
        }
        serializationContext = ioService.getSerializationContext();
    }

    public SerializationContext getSerializationContext() {
        return serializationContext;
    }

    interface SocketChannelWrapperFactory {
        SocketChannelWrapper wrapSocketChannel(SocketChannel socketChannel, boolean client) throws Exception;
    }

    class DefaultSocketChannelWrapperFactory implements SocketChannelWrapperFactory {
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
        setConnectionListeners.add(listener);
    }

    public boolean bind(TcpIpConnection connection, Address remoteEndPoint, Address localEndpoint, final boolean replyBack) {
        log(Level.FINEST, "Binding " + connection + " to " + remoteEndPoint + ", replyBack is " + replyBack);
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
        final Connection existingConnection = mapConnections.get(remoteEndPoint);
        if (existingConnection != null && existingConnection.live()) {
            if (existingConnection != connection) {
                log(Level.INFO, existingConnection + " is already bound  to " + remoteEndPoint + ", new one is " + connection);
            }
            return false;
        }
        if (!remoteEndPoint.equals(thisAddress)) {
            if (!connection.isClient()) {
                connection.setMonitor(getConnectionMonitor(remoteEndPoint, true));
            }
            mapConnections.put(remoteEndPoint, connection);
            setConnectionInProgress.remove(remoteEndPoint);
            for (ConnectionListener listener : setConnectionListeners) {
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

    TcpIpConnection assignSocketChannel(SocketChannelWrapper channel) {
        final int index = nextSelectorIndex();
        final TcpIpConnection connection = new TcpIpConnection(this, inSelectors[index], outSelectors[index],
                connectionIdGen.incrementAndGet(), channel);
        setActiveConnections.add(connection);
        connection.getReadHandler().register();
        log(Level.INFO, channel.socket().getLocalPort() + " accepted socket connection from "
                + channel.socket().getRemoteSocketAddress());
        return connection;
    }

    SocketChannelWrapper wrapSocketChannel(SocketChannel socketChannel, boolean client) throws Exception {
        return socketChannelWrapperFactory.wrapSocketChannel(socketChannel, client);
    }

    void failedConnection(Address address, Throwable t, boolean silent) {
        setConnectionInProgress.remove(address);
        ioService.onFailedConnection(address);
        if (!silent) {
            getConnectionMonitor(address, false).onError(t);
        }
    }

    public Connection getConnection(Address address) {
        return mapConnections.get(address);
    }

    public Connection getOrConnect(Address address) {
        return getOrConnect(address, false);
    }

    public Connection getOrConnect(final Address address, final boolean silent) {
        Connection connection = mapConnections.get(address);
        if (connection == null && live) {
            if (setConnectionInProgress.add(address)) {
                ioService.shouldConnectTo(address);
                ioService.executeAsync(new SocketConnector(this, address, silent));
            }
        }
        return connection;
    }

    private final ConstructorFunction<Address, ConnectionMonitor> monitorConstructor
            = new ConstructorFunction<Address, ConnectionMonitor>() {
        public ConnectionMonitor createNew(Address endpoint) {
            return new ConnectionMonitor(TcpIpConnectionManager.this, endpoint);
        }
    };

    private ConnectionMonitor getConnectionMonitor(Address endpoint, boolean reset) {
        final ConnectionMonitor monitor = ConcurrencyUtil.getOrPutIfAbsent(mapMonitors, endpoint, monitorConstructor);
        if (reset) {
            monitor.reset();
        }
        return monitor;
    }

    public void destroyConnection(Connection connection) {
        if (connection == null)
            return;
        log(Level.FINEST, "Destroying " + connection);
        setActiveConnections.remove((TcpIpConnection) connection);
        final Address endPoint = connection.getEndPoint();
        if (endPoint != null) {
            setConnectionInProgress.remove(endPoint);
            final Connection existingConn = mapConnections.get(endPoint);
            if (existingConn == connection) {
                mapConnections.remove(endPoint);
                for (ConnectionListener listener : setConnectionListeners) {
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
        if (live) return;
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
                setConnectionListeners.clear();
            }
        } finally {
            if (serverSocketChannel != null) {
                try {
                    log(Level.FINEST, "Closing server socket channel: " + serverSocketChannel);
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
        shutdownSocketAcceptor(); // interrupt acceptor thread after live=false
        ioService.onShutdown();
        for (Connection conn : mapConnections.values()) {
            try {
                destroyConnection(conn);
            } catch (final Throwable ignore) {
                logger.finest(ignore);
            }
        }
        for (TcpIpConnection conn : setActiveConnections) {
            try {
                destroyConnection(conn);
            } catch (final Throwable ignore) {
                logger.finest(ignore);
            }
        }
        shutdownIOSelectors();
        setConnectionInProgress.clear();
        mapConnections.clear();
        mapMonitors.clear();
        setActiveConnections.clear();
    }

    private synchronized void shutdownIOSelectors() {
        log(Level.FINEST, "Shutting down IO selectors... Total: " + selectorThreadCount);
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

    private synchronized void shutdownSocketAcceptor() {
        log(Level.FINEST, "Shutting down SocketAcceptor thread.");
        socketAcceptorThread.interrupt();
        socketAcceptorThread = null;
    }

    public int getCurrentClientConnections() {
        int count = 0;
        for (TcpIpConnection conn : setActiveConnections) {
            if (conn.live()) {
                if (conn.isClient()) {
                    count++;
                }
            }
        }
        return count;
    }

    public int getAllTextConnections() {
        return allTextConnections.get();
    }

    public void incrementTextConnections() {
        allTextConnections.incrementAndGet();
    }

    public boolean isLive() {
        return live;
    }

    public Map<Address, Connection> getReadonlyConnectionMap() {
        return Collections.unmodifiableMap(mapConnections);
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
        for (Connection conn : mapConnections.values()) {
            sb.append("\n");
            sb.append(conn);
        }
        sb.append("\nlive=");
        sb.append(live);
        sb.append("\n}");
        return sb.toString();
    }
}
