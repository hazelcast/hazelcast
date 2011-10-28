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

import com.hazelcast.impl.Node;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

public class InOutSelector extends SelectorBase {

    final ServerSocketChannel serverSocketChannel;

    final AtomicLong writeQueueSize = new AtomicLong();

    public InOutSelector(Node node, final ServerSocketChannel serverSocketChannel, boolean accept) {
        super(node, 1);
        this.serverSocketChannel = serverSocketChannel;
        if (accept) {
            addTask(new Runnable() {
                public void run() {
                    try {
                        serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT, new Acceptor());
                        selector.wakeup();
                    } catch (final ClosedChannelException e) {
                        logger.log(Level.WARNING, e.getMessage(), e);
                    }
                }
            });
        }
        logger.log(Level.FINEST, "Started Selector at "
                + serverSocketChannel.socket().getLocalPort());
    }

    @Override
    public void threadLocalShutdown() {
        try {
            if (serverSocketChannel != null) {
                serverSocketChannel.close();
            }
        } catch (IOException ignored) {
        }
    }

    public void connect(final Address address) {
        logger.log(Level.FINEST, "connect to " + address);
        final Connector connector = new Connector(address);
        this.addTask(connector);
    }

    @Override
    public void publishUtilization() {
    }

    public long getWriteQueueSize() {
        return writeQueueSize.get();
    }

    public void resetWriteQueueSize() {
        writeQueueSize.set(node.connectionManager.getTotalWriteQueueSize());
    }

    private class Connector implements Runnable, SelectionHandler {
        final Address address;

        SocketChannel socketChannel = null;

        long startTime = System.currentTimeMillis();

        long lastCheck = startTime;

        public Connector(Address address) {
            this.address = address;
        }

        public void handle() {
            try {
                final boolean finished = socketChannel.finishConnect();
                if (!finished) {
                    long now = System.currentTimeMillis();
                    if (now - lastCheck > 5000) {
                        logger.log(Level.WARNING, "Couldn't connect to " + address
                                + " for " + ((now - startTime) / 1000) + " seconds!");
                        lastCheck = now;
                    }
                    socketChannel.register(selector, SelectionKey.OP_CONNECT, Connector.this);
                    return;
                }
                logger.log(Level.FINEST, "connected to " + address);
                final Connection connection = node.connectionManager.createConnection(socketChannel, InOutSelector.this);
                node.connectionManager.bind(address, connection, false);
            } catch (Throwable e) {
                try {
                    final String msg = "Couldn't connect to " + address + ", cause: " + e.getMessage();
                    logger.log(Level.FINEST, msg, e);
                    socketChannel.close();
                    node.connectionManager.failedConnection(address);
                } catch (final Exception ignored) {
                }
            }
        }

        public void run() {
            try {
                socketChannel = SocketChannel.open();
                initSocket(socketChannel.socket());
                final Address thisAddress = node.getThisAddress();
                socketChannel.configureBlocking(false);
                socketChannel.socket().bind(new InetSocketAddress(thisAddress.getInetAddress(), 0));
                logger.log(Level.FINEST, "connecting to " + address);
                boolean connected = socketChannel.connect(new InetSocketAddress(address.getInetAddress(),
                        address.getPort()));
                logger.log(Level.FINEST, "connection check. connected: " + connected + ", " + address);
                handle();
            } catch (Throwable e) {
                logger.log(Level.WARNING, e.getMessage(), e);
                if (socketChannel != null) {
                    try {
                        socketChannel.close();
                    } catch (final IOException ignored) {
                    }
                }
                node.connectionManager.failedConnection(address);
            }
        }
    }

    private class Acceptor implements SelectionHandler {
        public void handle() {
            try {
                final SocketChannel channel = serverSocketChannel.accept();
                logger.log(Level.INFO, channel.socket().getLocalPort()
                        + " is accepting socket connection from "
                        + channel.socket().getRemoteSocketAddress());
                initSocket(channel.socket());
                channel.configureBlocking(false);
                node.connectionManager.assignSocketChannel(channel);
            } catch (final Exception e) {
                logger.log(Level.FINEST, e.getMessage(), e);
                try {
                    serverSocketChannel.close();
                } catch (final Exception ignore) {
                }
                node.shutdown(false, false);
            }
        }
    }
}
