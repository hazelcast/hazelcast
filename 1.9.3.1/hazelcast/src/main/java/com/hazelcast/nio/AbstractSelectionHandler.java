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

import com.hazelcast.cluster.ClusterService;
import com.hazelcast.impl.Node;
import com.hazelcast.logging.ILogger;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.logging.Level;

abstract class AbstractSelectionHandler implements SelectionHandler {

    protected final ILogger logger;

    protected final SocketChannel socketChannel;

    protected final Connection connection;

    protected final InSelector inSelector;

    protected final OutSelector outSelector;

    protected final ClusterService clusterService;

    protected final Node node;

    protected SelectionKey sk = null;

    public AbstractSelectionHandler(final Connection connection) {
        super();
        this.connection = connection;
        this.socketChannel = connection.getSocketChannel();
        this.node = connection.connectionManager.node;
        this.logger = node.getLogger(this.getClass().getName());
        this.inSelector = node.inSelector;
        this.outSelector = node.outSelector;
        this.clusterService = node.clusterService;
    }

    protected void shutdown() {
    }

    final void handleSocketException(Throwable e) {
        if (e instanceof OutOfMemoryError) {
            node.onOutOfMemory((OutOfMemoryError) e);
        }
        if (sk != null) {
            sk.cancel();
        }
        connection.close();
        StringBuilder sb = new StringBuilder();
        sb.append(Thread.currentThread().getName());
        sb.append(" Closing socket to endpoint ");
        sb.append(connection.getEndPoint());
        sb.append(", Cause:").append(e);
        if (e instanceof IOException) {
            logger.log(Level.WARNING, sb.toString());
        } else {
            logger.log(Level.WARNING, sb.toString(), e);
        }
    }

    final void registerOp(final Selector selector, final int operation) {
        try {
            if (!connection.live())
                return;
            if (sk == null) {
                sk = socketChannel.register(selector, operation, this);
            } else {
                sk.interestOps(operation);
            }
        } catch (Throwable e) {
            handleSocketException(e);
        }
    }
}
