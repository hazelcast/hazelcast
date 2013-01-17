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
import com.hazelcast.spi.Connection;
import com.hazelcast.spi.ConnectionManager;

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

    protected final ILogger logger;

    final int SOCKET_RECEIVE_BUFFER_SIZE;

    final int SOCKET_SEND_BUFFER_SIZE;

    final int SOCKET_LINGER_SECONDS;

    final boolean SOCKET_KEEP_ALIVE;

    final boolean SOCKET_NO_DELAY;

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

    private final InOutSelector[] selectors;

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
        this.SOCKET_RECEIVE_BUFFER_SIZE = ioService.getSocketReceiveBufferSize() * IOService.KILO_BYTE;
        this.SOCKET_SEND_BUFFER_SIZE = ioService.getSocketSendBufferSize() * IOService.KILO_BYTE;
        this.SOCKET_LINGER_SECONDS = ioService.getSocketLingerSeconds();
        this.SOCKET_KEEP_ALIVE = ioService.getSocketKeepAlive();
        this.SOCKET_NO_DELAY = ioService.getSocketNoDelay();
        int selectorCount = ioService.getSelectorThreadCount();
        selectors = new InOutSelector[selectorCount];
        final Collection<Integer> ports = ioService.getOutboundPorts();
        outboundPortCount = ports == null ? 0 : ports.size();
        if (ports != null) {
            outboundPorts.addAll(ports);
        }
        SSLConfig sslConfig = ioService.getSSLConfig();
        if (sslConfig != null && sslConfig.isEnabled()) {
            socketChannelWrapperFactory = new SSLSocketChannelWrapperFactory(sslConfig);
            logger.log(Level.INFO, "SSL is enabled");
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
                    logger.log(Level.SEVERE, "SocketInterceptor class cannot be instantiated!" + sic.getClassName(), e);
                }
            }
            if (implementation != null) {
                if (!(implementation instanceof MemberSocketInterceptor)) {
                    logger.log(Level.SEVERE, "SocketInterceptor must be instance of " + MemberSocketInterceptor.class.getName());
                    implementation = null;
                } else {
                    logger.log(Level.INFO, "SocketInterceptor is enabled");
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
            if (CipherHelper.isAsymmetricEncryptionEnabled(ioService)) {
                throw new RuntimeException("SSL and AsymmetricEncryption cannot be both enabled!");
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

    private InOutSelector nextSelector() {
        if (nextSelectorIndex.get() > 1000000) {
            nextSelectorIndex.set(0);
        }
        return selectors[Math.abs(nextSelectorIndex.incrementAndGet()) % selectors.length];
    }

    public void addConnectionListener(ConnectionListener listener) {
        setConnectionListeners.add(listener);
    }

    public boolean bind(Connection connection, Address remoteEndPoint, Address localEndpoint, final boolean replyBack) {
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
        if (existingConnection != null) {
            if (existingConnection != connection) {
                log(Level.FINEST, existingConnection + " is already bound  to " + remoteEndPoint);
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

    void sendBindRequest(final Connection connection, final Address remoteEndPoint, final boolean replyBack) {
        connection.setEndPoint(remoteEndPoint);
        //make sure bind packet is the first packet sent to the end point.
        final BindOperation bind = new BindOperation(ioService.getThisAddress(), remoteEndPoint, replyBack);
        final Data bindData = ioService.toData(bind);
        final Packet packet = new Packet(bindData, serializationContext);
        packet.setHeader(Packet.HEADER_OP, true);
        connection.write(packet);
        //now you can send anything...
    }

    TcpIpConnection assignSocketChannel(SocketChannelWrapper channel) {
        InOutSelector selectorAssigned = nextSelector();
        final TcpIpConnection connection = new TcpIpConnection(this, selectorAssigned, connectionIdGen.incrementAndGet(), channel);
        setActiveConnections.add(connection);
        selectorAssigned.addTask(connection.getReadHandler());
        selectorAssigned.selector.wakeup();
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

    private ConnectionMonitor getConnectionMonitor(Address endpoint, boolean reset) {
        ConnectionMonitor monitor = mapMonitors.get(endpoint);
        if (monitor == null) {
            monitor = new ConnectionMonitor(this, endpoint);
            final ConnectionMonitor monitorOld = mapMonitors.putIfAbsent(endpoint, monitor);
            if (monitorOld != null) {
                monitor = monitorOld;
            }
        }
        if (reset) {
            monitor.reset();
        }
        return monitor;
    }

    // for testing purposes only
    public Connection detachAndGetConnection(Address address) {
        return mapConnections.remove(address);
    }

    // for testing purposes only
    public void attachConnection(Address address, TcpIpConnection conn) {
        mapConnections.put(address, conn);
    }

    public void destroyConnection(Connection connection) {
        if (connection == null)
            return;
        log(Level.FINEST, "Destroying " + connection);
        setActiveConnections.remove(connection);
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
        if (SOCKET_LINGER_SECONDS > 0) {
            socket.setSoLinger(true, SOCKET_LINGER_SECONDS);
        }
        socket.setKeepAlive(SOCKET_KEEP_ALIVE);
        socket.setTcpNoDelay(SOCKET_NO_DELAY);
        socket.setReceiveBufferSize(SOCKET_RECEIVE_BUFFER_SIZE);
        socket.setSendBufferSize(SOCKET_SEND_BUFFER_SIZE);
    }

    public synchronized void start() {
        if (live) return;
        live = true;
        log(Level.FINEST, "Starting ConnectionManager and IO selectors.");
        for (int i = 0; i < selectors.length; i++) {
            selectors[i] = new InOutSelector(this, i);
            selectors[i].start();
        }
        if (serverSocketChannel != null) {
            if (socketAcceptorThread != null) {
                logger.log(Level.WARNING, "SocketAcceptor thread is already live! Shutting down old acceptor...");
                shutdownSocketAcceptor();
            }
            Runnable acceptRunnable = new SocketAcceptor(serverSocketChannel, this);
            socketAcceptorThread = new Thread(ioService.getThreadGroup(), acceptRunnable,
                    ioService.getThreadPrefix() + "Acceptor");
            socketAcceptorThread.start();
        }
    }

    public synchronized void onRestart() {
        stop();
        start();
    }

    public synchronized void shutdown() {
        try {
            if (live) {
                stop();
            }
        } finally {
            if (serverSocketChannel != null) {
                try {
                    log(Level.FINEST, "Closing server socket channel: " + serverSocketChannel);
                    serverSocketChannel.close();
                } catch (IOException ignore) {
                    logger.log(Level.FINEST, ignore.getMessage(), ignore);
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
                logger.log(Level.FINEST, ignore.getMessage(), ignore);
            }
        }
        for (TcpIpConnection conn : setActiveConnections) {
            try {
                destroyConnection(conn);
            } catch (final Throwable ignore) {
                logger.log(Level.FINEST, ignore.getMessage(), ignore);
            }
        }
        shutdownIOSelectors();
        setConnectionInProgress.clear();
        mapConnections.clear();
        mapMonitors.clear();
        setActiveConnections.clear();
    }

    private synchronized void shutdownIOSelectors() {
        log(Level.FINEST, "Shutting down IO selectors, total: " + selectors.length);
        for (int i = 0; i < selectors.length; i++) {
            InOutSelector ioSelector = selectors[i];
            if (ioSelector != null) {
                ioSelector.shutdown();
            }
            selectors[i] = null;
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
