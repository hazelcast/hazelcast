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
import com.hazelcast.cluster.ClusterManager;
import com.hazelcast.impl.Node;
import com.hazelcast.impl.Processable;
import com.hazelcast.logging.ILogger;

import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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

    private final Map<Address, Connection> mapConnections = new ConcurrentHashMap<Address, Connection>(100);

    private final Set<Address> setConnectionInProgress = new CopyOnWriteArraySet<Address>();

    private final Set<ConnectionListener> setConnectionListeners = new CopyOnWriteArraySet<ConnectionListener>();

    private final Set<Connection> setActiveConnections = new CopyOnWriteArraySet<Connection>();

    private final AtomicInteger allTextConnections = new AtomicInteger();

    private final AtomicInteger connectionIdGen = new AtomicInteger();

    private boolean acceptTypeConnection = false;

    private volatile boolean live = true;

    final Node node;
    private final ServerSocketChannel serverSocketChannel;

    private final InOutSelector[] selectors;

    private final AtomicInteger nextSelectorIndex = new AtomicInteger();

    public ConnectionManager(Node node, ServerSocketChannel serverSocketChannel) {
        this.node = node;
        this.serverSocketChannel = serverSocketChannel;
        this.logger = node.getLogger(ConnectionManager.class.getName());
        this.SOCKET_RECEIVE_BUFFER_SIZE = this.node.getGroupProperties().SOCKET_RECEIVE_BUFFER_SIZE.getInteger() * KILO_BYTE;
        this.SOCKET_SEND_BUFFER_SIZE = this.node.getGroupProperties().SOCKET_SEND_BUFFER_SIZE.getInteger() * KILO_BYTE;
        this.SOCKET_LINGER_SECONDS = this.node.getGroupProperties().SOCKET_LINGER_SECONDS.getInteger();
        this.SOCKET_KEEP_ALIVE = this.node.getGroupProperties().SOCKET_KEEP_ALIVE.getBoolean();
        this.SOCKET_NO_DELAY = this.node.getGroupProperties().SOCKET_NO_DELAY.getBoolean();
        int selectorCount = 5;
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
        final Connection connExisting = mapConnections.get(endPoint);
        if (connExisting != null && connExisting != connection) {
            final String msg = "Two connections from the same endpoint " + endPoint
                    + ", acceptTypeConnection=" + acceptTypeConnection + ",  now accept="
                    + accept;
            if (node.joined() && node.isMaster()) {
                logger.log(Level.WARNING, msg);
                connExisting.closeSilently();
                final Address deadEndpoint = connExisting.getEndPoint();
                if (deadEndpoint != null) {
                    node.clusterManager.enqueueAndReturn(new Processable() {
                        public void process() {
                            node.clusterManager.disconnectExistingCalls(deadEndpoint);
                        }
                    });
                }
            } else {
                logger.log(Level.FINEST, msg);
                return true;
            }
        }
        if (!endPoint.equals(node.getThisAddress())) {
            acceptTypeConnection = accept;
            if (!accept) {
                //make sure bind packet is the first packet sent to the end point.
                ClusterManager clusterManager = node.clusterManager;
                Packet bindPacket = clusterManager.createRemotelyProcessablePacket(new Bind(clusterManager.getThisAddress()));
                connection.writeHandler.enqueueSocketWritable(bindPacket);
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

    public void assignSocketChannel(SocketChannel channel) {
        InOutSelector selectorAssigned = nextSelector();
        createConnection(channel, selectorAssigned);
    }

    Connection createConnection(SocketChannel channel, InOutSelector selectorAssigned) {
        final Connection connection = new Connection(this, selectorAssigned, connectionIdGen.incrementAndGet(), channel);
        setActiveConnections.add(connection);
        selectorAssigned.addTask(connection.getReadHandler());
        selectorAssigned.selector.wakeup();
        logger.log(Level.INFO, channel.socket().getLocalPort()
                + " accepted socket connection from "
                + channel.socket().getRemoteSocketAddress());
        return connection;
    }

    public void failedConnection(Address address) {
        setConnectionInProgress.remove(address);
        if (!node.joined()) {
            node.failedConnection(address);
        }
    }

    public Connection getConnection(Address address) {
        return mapConnections.get(address);
    }

    public Connection getOrConnect(Address address) {
        if (address.equals(node.getThisAddress()))
            throw new RuntimeException("Connecting to self! " + address);
        Connection connection = mapConnections.get(address);
        if (connection == null) {
            if (setConnectionInProgress.add(address)) {
                if (!node.clusterManager.shouldConnectTo(address))
                    throw new RuntimeException("Should not connect to " + address);
                nextSelector().connect(address);
            }
        }
        return connection;
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
        if (connection.getEndPoint() != null) {
            mapConnections.remove(connection.getEndPoint());
            setConnectionInProgress.remove(connection.getEndPoint());
            for (ConnectionListener listener : setConnectionListeners) {
                listener.connectionRemoved(connection);
            }
        }
        if (connection.live())
            connection.close();
    }

    public void start() {
        live = true;
        for (int i = 0; i < selectors.length; i++) {
            InOutSelector s = new InOutSelector(node, serverSocketChannel, (i == 0));
            selectors[i] = s;
            new Thread(s, node.getName() + ".IOThread" + i).start();
        }
    }

    public void onRestart() {
        shutdown();
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
        live = false;
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
            sbState.append("\n\tEndPoint: " + conn.getEndPoint());
            sbState.append("  " + conn.live());
            sbState.append("  " + conn.getWriteHandler().size());
            sbState.append("  w:").append(wr).append("/").append(wh);
            sbState.append("  r:").append(rr).append("/").append(rh);
        }
        sbState.append("\n}");
    }
}
