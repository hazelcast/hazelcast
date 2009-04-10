/* 
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
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

import com.hazelcast.impl.ClusterManager;
import com.hazelcast.impl.Config;
import com.hazelcast.impl.Node;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class OutSelector extends SelectorBase {

    protected static Logger logger = Logger.getLogger(OutSelector.class.getName());

    private static final OutSelector instance = new OutSelector();

    private final Set<Integer> boundPorts = new HashSet<Integer>();

    private OutSelector() {
        super();
        super.waitTime = 1;
    }

    public static OutSelector get() {
        return instance;
    }

    public void connect(final Address address) {
        if (DEBUG)
            logger.log(Level.INFO, "connect to " + address);
        final Connector connector = new Connector(address);
        this.addTask(connector);
    }

    private class Connector implements Runnable, SelectionHandler {
        Address address;

        SocketChannel socketChannel = null;

        int localPort = 0;

        int numberOfConnectionError = 0;

        public Connector(final Address address) {
            super();
            this.address = address;
        }

        public void handle() {
            try {
                final boolean finished = socketChannel.finishConnect();
                if (!finished) {
                    socketChannel.register(selector, SelectionKey.OP_CONNECT, Connector.this);
                    return;
                }
                if (DEBUG) {
                    logger.log(Level.FINEST, "connected to " + address);
                }
                final Connection connection = initChannel(socketChannel, false);
                connection.localPort = localPort;
                ConnectionManager.get().bind(address, connection, false);
            } catch (final Exception e) {
                try {
                    if (DEBUG) {
                        final String msg = "Couldn't connect to " + address + ", cause: "
                                + e.getMessage();
                        logger.log(Level.FINEST, msg);
                        ClusterManager.get().publishLog(msg);
                        e.printStackTrace();
                    }
                    socketChannel.close();
                    if (numberOfConnectionError++ < 5) {
                        if (DEBUG) {
                            logger.log(Level.FINEST, "Couldn't finish connecting, will try again. cause: "
                                    + e.getMessage());
                        }
                        addTask(Connector.this);
                    } else {
                        ConnectionManager.get().failedConnection(address);
                    }
                } catch (final Exception ignored) {
                }
            }
        }

        public void run() {
            try {
                socketChannel = SocketChannel.open();
                final Address thisAddress = Node.get().getThisAddress();
                final int addition = (thisAddress.getPort() - Config.get().port);
                localPort = 10000 + addition;
                boolean bindOk = false;
                while (!bindOk) {
                    try {
                        localPort += 20;
                        if (boundPorts.size() > 2000 || localPort > 60000) {
                            boundPorts.clear();
                            final Connection[] conns = ConnectionManager.get().getConnections();
                            for (final Connection conn : conns) {
                                // conn is live or not, assume it is bounded
                                boundPorts.add(conn.localPort);
                            }
                        }
                        if (boundPorts.add(localPort)) {
                            socketChannel.socket().bind(
                                    new InetSocketAddress(thisAddress.getInetAddress(), localPort));
                            bindOk = true;
                            socketChannel.configureBlocking(false);
                            if (DEBUG)
                                logger.log(Level.FINEST, "connecting to " + address + ", localPort: " + localPort);
                            boolean connected = socketChannel.connect(new InetSocketAddress(address.getInetAddress(),
                                    address.getPort()));
                            if (DEBUG)
                                logger.log(Level.FINEST, "connection check. connected: " + connected + ", " + address + ", localPort: " + localPort);
                            if (connected) {
                                handle();
                                return;
                            }
                        }
                    } catch (final Throwable e) {
                        if (DEBUG)
                            logger.log(Level.FINEST, "ConnectionFailed. localPort: " + localPort, e);
                        // ignore
                    }
                }
                socketChannel.register(selector, SelectionKey.OP_CONNECT, Connector.this);
            } catch (final Exception e) {
                try {
                    socketChannel.close();
                } catch (final IOException ignored) {
                }
                if (numberOfConnectionError++ < 5) {
                    if (DEBUG) {
                        logger.log(Level.FINEST,
                                "Couldn't register connect! will trying again. cause: "
                                        + e.getMessage());
                    }
                    run();
                } else {
                    ConnectionManager.get().failedConnection(address);
                }
            }
        }

    }


}
