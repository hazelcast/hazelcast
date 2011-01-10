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
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.logging.Level;

public final class InSelector extends SelectorBase {

    final ServerSocketChannel serverSocketChannel;

    final SelectionKey key;

    public InSelector(Node node, ServerSocketChannel serverSocketChannel) {
        super(node, 64);
        this.serverSocketChannel = serverSocketChannel;
        SelectionKey sKey = null;
        try {
            sKey = serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT, new Acceptor());
        } catch (final ClosedChannelException e) {
            e.printStackTrace();
        }
        key = sKey;
        logger.log(Level.FINEST, "Started Selector at "
                + serverSocketChannel.socket().getLocalPort());
        selector.wakeup();
    }

    @Override
    public void publishUtilization() {
        node.getCpuUtilization().inThread = threadWatcher.publish();
    }

    @Override
    public void threadLocalShutdown() {
        try {
            if (serverSocketChannel != null) {
                serverSocketChannel.close();
            }
            node.clientService.shutdown();
        } catch (IOException ignored) {
        }
    }

    private class Acceptor implements SelectionHandler {
        public void handle() {
            try {
                final SocketChannel channel = serverSocketChannel.accept();
                logger.log(Level.INFO, channel.socket().getLocalPort()
                        + " is accepting socket connection from "
                        + channel.socket().getRemoteSocketAddress());
                final Connection connection = initChannel(channel, true);
                channel.register(selector, SelectionKey.OP_READ, connection.getReadHandler());
                serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT, Acceptor.this);
                selector.wakeup();
                logger.log(Level.INFO, channel.socket().getLocalPort()
                        + " is accepted socket connection from "
                        + channel.socket().getRemoteSocketAddress());
            } catch (final Exception e) {
                logger.log(Level.FINEST, e.getMessage(), e);
                try {
                    serverSocketChannel.close();
                } catch (final Exception ignore) {
                }
                node.shutdown();
            }
        }
    }
}
