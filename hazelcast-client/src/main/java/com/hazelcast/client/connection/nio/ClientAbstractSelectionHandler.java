/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
 */

package com.hazelcast.client.connection.nio;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.IOSelector;
import com.hazelcast.nio.SelectionHandler;

import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

/**
 * @author ali 14/12/13
 */
public abstract class ClientAbstractSelectionHandler implements SelectionHandler, Runnable {

    protected final ILogger logger;

    protected final SocketChannel socketChannel;

    protected final ClientConnection connection;

    protected final ClientConnectionManagerImpl connectionManager;

    protected final IOSelector ioSelector;

    private SelectionKey sk = null;

    public ClientAbstractSelectionHandler(final ClientConnection connection, IOSelector ioSelector) {
        this.connection = connection;
        this.ioSelector = ioSelector;
        this.socketChannel = connection.getSocketChannel();
        this.connectionManager = connection.getConnectionManager();
        this.logger = Logger.getLogger(getClass().getName());
    }

    protected void shutdown() {
    }

    final void handleSocketException(Throwable e) {
        if (sk != null) {
            sk.cancel();
        }
        connection.close(e);
        StringBuilder sb = new StringBuilder();
        sb.append(Thread.currentThread().getName());
        sb.append(" Closing socket to endpoint ");
        sb.append(connection.getEndPoint());
        sb.append(", Cause:").append(e);
        logger.warning(sb.toString());
    }

    final void registerOp(final int operation) {
        try {
            if (!connection.live())
                return;
            if (sk == null) {
                sk = socketChannel.keyFor(ioSelector.getSelector());
            }
            if (sk == null) {
                sk = socketChannel.register(ioSelector.getSelector(), operation, this);
            } else {
                sk.interestOps(sk.interestOps() | operation);
                if (sk.attachment() != this) {
                    sk.attach(this);
                }
            }
        } catch (Throwable e) {
            handleSocketException(e);
        }
    }

    public void register() {
        ioSelector.addTask(this);
        ioSelector.wakeup();
    }

}
