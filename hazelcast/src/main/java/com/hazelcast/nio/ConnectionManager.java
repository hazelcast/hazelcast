/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.nio;

import com.hazelcast.cluster.Bind;
import com.hazelcast.impl.ClusterOperation;
import com.hazelcast.impl.ThreadContext;
import com.hazelcast.logging.ILogger;
import com.hazelcast.util.ConcurrentHashSet;

import java.io.IOException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import static com.hazelcast.impl.Constants.IO.KILO_BYTE;

public class ConnectionManager {

    protected final ILogger logger;

    final int SOCKET_RECEIVE_BUFFER_SIZE;

    final int SOCKET_SEND_BUFFER_SIZE;

    final int SOCKET_LINGER_SECONDS;

    final boolean SOCKET_KEEP_ALIVE;

    final boolean SOCKET_NO_DELAY;

    final int SOCKET_TIMEOUT;

    private final Map<Address, Connection> mapConnections = new ConcurrentHashMap<Address, Connection>(100);

    private final ConcurrentMap<Address, ConnectionMonitor> mapMonitors = new ConcurrentHashMap<Address, ConnectionMonitor>(100);

    private final Set<Address> setConnectionInProgress = new ConcurrentHashSet<Address>();

    private final Set<ConnectionListener> setConnectionListeners = new CopyOnWriteArraySet<ConnectionListener>();

    private final Set<Connection> setActiveConnections = new ConcurrentHashSet<Connection>();

    private final AtomicInteger allTextConnections = new AtomicInteger();

    private final AtomicInteger connectionIdGen = new AtomicInteger();

    private volatile boolean live = true;

    final IOService ioService;

    private final ServerSocketChannel serverSocketChannel;

    private final InOutSelector[] selectors;

    private final AtomicInteger nextSelectorIndex = new AtomicInteger();

    public ConnectionManager(IOService ioService, ServerSocketChannel serverSocketChannel) {
        this.ioService = ioService;
        this.serverSocketChannel = serverSocketChannel;
        this.logger = ioService.getLogger(ConnectionManager.class.getName());
        this.SOCKET_RECEIVE_BUFFER_SIZE = ioService.getSocketReceiveBufferSize() * KILO_BYTE;
        this.SOCKET_SEND_BUFFER_SIZE = ioService.getSocketSendBufferSize() * KILO_BYTE;
        this.SOCKET_LINGER_SECONDS = ioService.getSocketLingerSeconds();
        this.SOCKET_KEEP_ALIVE = ioService.getSocketKeepAlive();
        this.SOCKET_NO_DELAY = ioService.getSocketNoDelay();
        this.SOCKET_TIMEOUT = ioService.getSocketTimeoutSeconds() * 1000;
        int selectorCount = ioService.getSelectorThreadCount();
        selectors = new InOutSelector[selectorCount];
    }

    public InOutSelector nextSelector() {
        if (nextSelectorIndex.get() > 1000000) {
            nextSelectorIndex.set(0);
        }
        return selectors[Math.abs(nextSelectorIndex.incrementAndGet()) % selectors.length];
    }

    public void addConnectionListener(ConnectionListener listener) {
        setConnectionListeners.add(listener);
    }

    public boolean bind(Address endPoint, Connection connection,
                        boolean accept) {
        connection.setEndPoint(endPoint);
        if (mapConnections.containsKey(endPoint)) {
            return false;
        }
        if (!endPoint.equals(ioService.getThisAddress())) {
            if (!connection.isClient()) {
                connection.setMonitor(getConnectionMonitor(endPoint, true));
            }
            if (!accept) {
                //make sure bind packet is the first packet sent to the end point.
                Packet bindPacket = createBindPacket(new Bind(ioService.getThisAddress()));
                connection.getWriteHandler().enqueueSocketWritable(bindPacket);
                //now you can send anything...
            }
            mapConnections.put(endPoint, connection);
            setConnectionInProgress.remove(endPoint);
            for (ConnectionListener listener : setConnectionListeners) {
                listener.connectionAdded(connection);
            }
        } else {
            return false;
        }
        return true;
    }

    public Packet createBindPacket(Bind rp) {
        Data value = ThreadContext.get().toData(rp);
        Packet packet = new Packet();
        packet.set("remotelyProcess", ClusterOperation.REMOTELY_PROCESS, null, value);
        packet.client = ioService.isClient();
        return packet;
    }

    public void assignSocketChannel(SocketChannelWrapper channel) {
        InOutSelector selectorAssigned = nextSelector();
        createConnection(channel, selectorAssigned);
    }

    Connection createConnection(SocketChannelWrapper channel, InOutSelector selectorAssigned) {
        final Connection connection = new Connection(this, selectorAssigned, connectionIdGen.incrementAndGet(), channel);
        setActiveConnections.add(connection);
        selectorAssigned.addTask(connection.getReadHandler());
        selectorAssigned.selector.wakeup();
        logger.log(Level.INFO, channel.socket().getLocalPort()
                + " accepted socket connection from "
                + channel.socket().getRemoteSocketAddress());
        return connection;
    }

    public SocketChannelWrapper wrapSocketChannel(SocketChannel socketChannel, boolean client) throws Exception {
        return new DefaultSocketChannelWrapper(socketChannel);
//        return new SSLSocketChannelWrapper(socketChannel, client);
    }

    public void failedConnection(Address address, Throwable t) {
        setConnectionInProgress.remove(address);
        ioService.onFailedConnection(address);
        getConnectionMonitor(address, false).onError(t);
    }

    public Connection getConnection(Address address) {
        return mapConnections.get(address);
    }

    public Connection getOrConnect(Address address) {
        Connection connection = mapConnections.get(address);
        if (connection == null) {
            if (setConnectionInProgress.add(address)) {
                ioService.shouldConnectTo(address);
                nextSelector().connect(address);
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

    public Connection detachAndGetConnection(Address address) {
        return mapConnections.remove(address);
    }

    public void attachConnection(Address address, Connection conn) {
        mapConnections.put(address, conn);
    }

    public void destroyConnection(Connection connection) {
        if (connection == null)
            return;
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

    public void start() {
        live = true;
        for (int i = 0; i < selectors.length; i++) {
            InOutSelector s = new InOutSelector(this, (i == 0 ? serverSocketChannel : null));
            selectors[i] = s;
            new Thread(ioService.getThreadGroup(), s, "hz." + ioService.getThreadPrefix() + ".IOThread" + i).start();
        }
    }

    public void onRestart() {
        stop();
        start();
    }

    public int getTotalWriteQueueSize() {
        int count = 0;
        for (Connection conn : mapConnections.values()) {
            if (conn.live()) {
                count += conn.getWriteHandler().size();
            }
        }
        return count;
    }

    public void shutdown() {
        if (!live) return;
        live = false;
        stop();
        if (serverSocketChannel != null) {
            try {
                serverSocketChannel.close();
            } catch (IOException ignore) {
                logger.log(Level.FINEST, ignore.getMessage(), ignore);
            }
        }
    }

    private void stop() {
        for (Connection conn : mapConnections.values()) {
            try {
                destroyConnection(conn);
            } catch (final Throwable ignore) {
                logger.log(Level.FINEST, ignore.getMessage(), ignore);
            }
        }
        for (Connection conn : setActiveConnections) {
            try {
                destroyConnection(conn);
            } catch (final Throwable ignore) {
                logger.log(Level.FINEST, ignore.getMessage(), ignore);
            }
        }
        for (int i = 0; i < selectors.length; i++) {
            InOutSelector ioSelector = selectors[i];
            if (ioSelector != null) {
                ioSelector.shutdown();
            }
        }
        setConnectionInProgress.clear();
        mapConnections.clear();
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("Connections {");
        for (Connection conn : mapConnections.values()) {
            sb.append("\n");
            sb.append(conn);
        }
        sb.append("\nlive=");
        sb.append(live);
        sb.append("\n}");
        return sb.toString();
    }

    public int getCurrentClientConnections() {
        int count = 0;
        for (Connection conn : setActiveConnections) {
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

    public void appendState(StringBuffer sbState) {
        long now = System.currentTimeMillis();
        sbState.append("\nConnectionManager {");
        for (Connection conn : mapConnections.values()) {
            long wr = (now - conn.getWriteHandler().lastRegistration) / 1000;
            long wh = (now - conn.getWriteHandler().lastHandle) / 1000;
            long rr = (now - conn.getReadHandler().lastRegistration) / 1000;
            long rh = (now - conn.getReadHandler().lastHandle) / 1000;
            sbState.append("\n\tEndPoint: ").append(conn.getEndPoint());
            sbState.append("  ").append(conn.live());
            sbState.append("  ").append(conn.getWriteHandler().size());
            sbState.append("  w:").append(wr).append("/").append(wh);
            sbState.append("  r:").append(rr).append("/").append(rh);
        }
        sbState.append("\n}");
    }

    public IOService getIOHandler() {
        return ioService;
    }
}
