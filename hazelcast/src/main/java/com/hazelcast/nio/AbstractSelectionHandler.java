/* 
 * Copyright (c) 2008-2009, Hazel Ltd. All Rights Reserved.
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
import static com.hazelcast.impl.Constants.IO.BYTE_BUFFER_SIZE;
import com.hazelcast.impl.Node;

import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

abstract class AbstractSelectionHandler implements SelectionHandler {

    protected static Logger logger = Logger.getLogger(AbstractSelectionHandler.class.getName());

    public static final int RECEIVE_SOCKET_BUFFER_SIZE = 32 * BYTE_BUFFER_SIZE;

    public static final int SEND_SOCKET_BUFFER_SIZE = 32 * BYTE_BUFFER_SIZE;

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
        this.inSelector = node.inSelector;
        this.outSelector = node.outSelector;
        this.clusterService = node.clusterService;
    }

    protected void shutdown() {
    }

    final void handleSocketException(final Exception e) {
        logger.log(Level.FINEST,
                Thread.currentThread().getName() + " Closing Socket. cause:  ", e);
        if (sk != null)
            sk.cancel();
        connection.close();
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
        } catch (Exception e) {
            handleSocketException(e);
        }
    }
}
