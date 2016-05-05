/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.tcp.SocketChannelWrapper;
import com.hazelcast.nio.tcp.nonblocking.NonBlockingIOThread;
import com.hazelcast.nio.tcp.nonblocking.SelectionHandler;

import java.nio.channels.SelectionKey;

import static com.hazelcast.internal.metrics.ProbeLevel.DEBUG;
import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;

/**
 * The AbstractClientSelectionHandler gets called by an IO-thread when there is data available to read,
 * or space available to write.
 */
public abstract class AbstractClientSelectionHandler implements SelectionHandler, Runnable {

    protected final ILogger logger;
    protected final SocketChannelWrapper socketChannel;
    protected final ClientConnection connection;
    protected final ClientConnectionManager connectionManager;
    @Probe(name = "eventCount")
    protected final SwCounter eventCount = newSwCounter();
    private final NonBlockingIOThread ioThread;
    @Probe
    private final int ioThreadId;
    private volatile SelectionKey sk;

    public AbstractClientSelectionHandler(final ClientConnection connection, NonBlockingIOThread ioThread,
                                          LoggingService loggingService) {
        this.connection = connection;
        this.ioThread = ioThread;
        this.ioThreadId = ioThread.id;
        this.socketChannel = connection.getSocketChannelWrapper();
        this.connectionManager = connection.getConnectionManager();
        this.logger = loggingService.getLogger(getClass().getName());
    }

    @Probe(level = DEBUG)
    private long opsInterested() {
        SelectionKey selectionKey = this.sk;
        return selectionKey == null ? -1 : selectionKey.interestOps();
    }

    @Probe(level = DEBUG)
    private long opsReady() {
        SelectionKey selectionKey = this.sk;
        return selectionKey == null ? -1 : selectionKey.readyOps();
    }

    protected void shutdown() {
    }

    @Override
    public final void onFailure(Throwable e) {
        if (sk != null) {
            sk.cancel();
        }
        connectionManager.destroyConnection(connection, null, e);
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
