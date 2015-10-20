/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.tcp.nonblocking.NonBlockingIOThread;
import com.hazelcast.nio.tcp.nonblocking.SelectionHandler;
import com.hazelcast.nio.tcp.SocketChannelWrapper;

import java.nio.channels.SelectionKey;

public abstract class AbstractClientSelectionHandler implements SelectionHandler, Runnable {

    protected final ILogger logger;
    protected final SocketChannelWrapper socketChannel;
    protected final ClientConnection connection;
    protected final ClientConnectionManager connectionManager;
    private final NonBlockingIOThread ioThread;
    private SelectionKey sk;

    public AbstractClientSelectionHandler(final ClientConnection connection, NonBlockingIOThread ioThread) {
        this.connection = connection;
        this.ioThread = ioThread;
        this.socketChannel = connection.getSocketChannelWrapper();
        this.connectionManager = connection.getConnectionManager();
        this.logger = Logger.getLogger(getClass().getName());
    }

    protected void shutdown() {
    }

    @Override
    public void onFailure(Throwable e) {
        if (sk != null) {
            sk.cancel();
        }
        connectionManager.destroyConnection(connection);
        StringBuilder sb = new StringBuilder();
        sb.append(Thread.currentThread().getName());
        sb.append(" Closing socket to endpoint ");
        sb.append(connection.getEndPoint());
        sb.append(", Cause:").append(e);
        logger.warning(sb.toString());
    }

    final void registerOp(final int operation) {
        try {
            if (!connection.isAlive()) {
                return;
            }
            if (sk == null) {
                sk = socketChannel.keyFor(ioThread.getSelector());
            }
            if (sk == null) {
                sk = socketChannel.register(ioThread.getSelector(), operation, this);
            } else {
                sk.interestOps(sk.interestOps() | operation);
                if (sk.attachment() != this) {
                    sk.attach(this);
                }
            }
        } catch (Throwable e) {
            onFailure(e);
        }
    }

    public void register() {
        ioThread.addTaskAndWakeup(this);
    }

}
