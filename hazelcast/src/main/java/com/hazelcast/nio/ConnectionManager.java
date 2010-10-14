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
import com.hazelcast.logging.ILogger;

import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

public class ConnectionManager {

    protected final ILogger logger;

    private final Map<Address, Connection> mapConnections = new ConcurrentHashMap<Address, Connection>(100);

    private final Set<Address> setConnectionInProgress = new CopyOnWriteArraySet<Address>();

    private final Set<ConnectionListener> setConnectionListeners = new CopyOnWriteArraySet<ConnectionListener>();

    private final Set<Connection> setActiveConnections = new CopyOnWriteArraySet<Connection>();

    private final AtomicInteger allTextConnections = new AtomicInteger();

    private final AtomicInteger connectionIdGen = new AtomicInteger();

    private boolean acceptTypeConnection = false;

    private volatile boolean live = true;

    final Node node;

    public ConnectionManager(Node node) {
        this.node = node;
        logger = node.getLogger(ConnectionManager.class.getName());
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
            logger.log(Level.FINEST, msg);
            return true;
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

    public Connection createConnection(SocketChannel socketChannel,
                                       boolean acceptor) {
        final Connection connection = new Connection(this, connectionIdGen.incrementAndGet(), socketChannel);
        setActiveConnections.add(connection);
        try {
            if (acceptor) {
                // do nothing. you will be registering for the
                // write operation when you have something to
                // write already in the outSelector thread.
            } else {
                node.inSelector.addTask(connection.getReadHandler());
                // socketChannel.register(inSelector.selector,
                // SelectionKey.OP_READ, readHandler);
            }
        } catch (final Exception e) {
            e.printStackTrace();
        }
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
                node.outSelector.connect(address);
            }
        }
        return connection;
    }

    public void remove(Connection connection) {
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
    }

    public void onRestart() {
        shutdown();
        start();
    }

    public void shutdown() {
        live = false;
        for (Connection conn : mapConnections.values()) {
            try {
                remove(conn);
            } catch (final Throwable ignore) {
                ignore.printStackTrace();
            }
        }
        for (Connection conn : setActiveConnections) {
            try {
                remove(conn);
            } catch (final Throwable ignore) {
                ignore.printStackTrace();
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
}
