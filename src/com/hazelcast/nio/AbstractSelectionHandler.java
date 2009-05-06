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

import com.hazelcast.impl.Build;
import com.hazelcast.impl.ClusterManager;

import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

abstract class AbstractSelectionHandler implements SelectionHandler {

    protected static Logger logger = Logger.getLogger(AbstractSelectionHandler.class.getName());

    public static final boolean DEBUG = Build.DEBUG;

    protected SocketChannel socketChannel;

    protected InSelector inSelector;

    protected OutSelector outSelector;

    protected Connection connection;

    protected SelectionKey sk = null;

    public AbstractSelectionHandler(final Connection connection) {
        super();
        this.connection = connection;
        this.socketChannel = connection.getSocketChannel();
        this.inSelector = InSelector.get();
        this.outSelector = OutSelector.get();
    }

    protected void shutdown() {

    }

    final void handleSocketException(final Exception e) {
        if (DEBUG) {
            logger.log(Level.FINEST,
                    Thread.currentThread().getName() + " Closing Socket. cause:  ", e);
        }

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
        } catch (final Exception e) {
            handleSocketException(e);
        }
    }

}
