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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.hazelcast.config.Config;
import com.hazelcast.impl.Node;

public class InSelector extends SelectorBase {

    protected static Logger logger = Logger.getLogger(InSelector.class.getName());

    private static final InSelector instance = new InSelector();

    private ServerSocketChannel serverSocketChannel;

    SelectionKey key = null;

    public InSelector() {
        super();
        this.waitTime = 64;
    }

    public static InSelector get() {
        return instance;
    }

    public void setServerSocketChannel(final ServerSocketChannel serverSocketChannel) {
        this.serverSocketChannel = serverSocketChannel;
        try {
            key = serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT, new Acceptor());
        } catch (final ClosedChannelException e) {
            e.printStackTrace();
        }
        if (DEBUG) {
            logger.log(Level.INFO, "Started Selector at "
                    + serverSocketChannel.socket().getLocalPort());
        }
        selector.wakeup();
    }

    @Override
    public void shutdown() {
        try {
            super.shutdown();
            serverSocketChannel.close();
        } catch (IOException ignored) {
        }
    }

    private class Acceptor implements SelectionHandler {
        public void handle() {
            try {
                final SocketChannel channel = serverSocketChannel.accept();
                if (DEBUG)
                    logger.log(Level.INFO, channel.socket().getLocalPort()
                            + " this socket is connected to "
                            + channel.socket().getRemoteSocketAddress());
                if (channel != null) {
                    final Connection connection = initChannel(channel, true);
                    final InetSocketAddress remoteSocket = (InetSocketAddress) channel.socket()
                            .getRemoteSocketAddress();
                    final int remoteRealPort = Config.get().getPort()
                            + ((remoteSocket.getPort() - 10000) % 20);
                    final Address endPoint = new Address(remoteSocket.getAddress(), remoteRealPort);
                    ConnectionManager.get().bind(endPoint, connection, true);
                    channel.register(selector, SelectionKey.OP_READ, connection.getReadHandler());
                }
                serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT, Acceptor.this);
                selector.wakeup();
            } catch (final Exception e) {
                e.printStackTrace();
                try {
                    serverSocketChannel.close();
                } catch (final Exception ignore) {
                }
                Node.get().shutdown();
            }
        }
    }
}
