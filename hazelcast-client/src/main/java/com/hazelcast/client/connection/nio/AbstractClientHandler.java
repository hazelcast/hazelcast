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

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;

import static com.hazelcast.internal.metrics.ProbeLevel.DEBUG;
import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;

abstract class AbstractClientHandler implements SelectionHandler {

    protected final ILogger logger;
    protected final SocketChannelWrapper socketChannel;
    protected final ClientConnection connection;
    protected final ClientConnectionManager connectionManager;
    @Probe(name = "eventCount")
    protected final SwCounter eventCount = newSwCounter();
    protected final NonBlockingIOThread ioThread;
    @Probe
    private final int ioThreadId;
    private final int initialOps;
    private volatile SelectionKey selectionKey;

    AbstractClientHandler(ClientConnection connection,
                          NonBlockingIOThread ioThread,
                          LoggingService loggingService,
                          int initialOps) {
        this.connection = connection;
        this.ioThread = ioThread;
        this.ioThreadId = ioThread.id;
        this.initialOps = initialOps;
        this.socketChannel = connection.getSocketChannelWrapper();
        this.connectionManager = connection.getConnectionManager();
        this.logger = loggingService.getLogger(getClass().getName());
    }

    @Probe(level = DEBUG)
    long opsInterested() {
        SelectionKey selectionKey = this.selectionKey;
        return selectionKey == null ? -1 : selectionKey.interestOps();
    }

    @Probe(level = DEBUG)
    long opsReady() {
        SelectionKey selectionKey = this.selectionKey;
        return selectionKey == null ? -1 : selectionKey.readyOps();
    }

    final void unregisterOp(int operation) throws IOException {
        SelectionKey selectionKey = getSelectionKey();
        selectionKey.interestOps(selectionKey.interestOps() & ~operation);
    }

    final void registerOp(int operation) throws IOException {
        SelectionKey selectionKey = getSelectionKey();
        selectionKey.interestOps(selectionKey.interestOps() | operation);
    }

    public SelectionKey getSelectionKey() throws ClosedChannelException {
        if (selectionKey == null) {
            selectionKey = socketChannel.register(ioThread.getSelector(), initialOps, this);
        }
        return selectionKey;
    }

    public void init() {
        ioThread.addTaskAndWakeup(new Runnable() {
            @Override
            public void run() {
                try {
                    getSelectionKey();
                } catch (Exception e) {
                    onFailure(e);
                }
            }
        });
    }

    protected void close() {
    }

    @Override
    public final void onFailure(Throwable e) {
        if (selectionKey != null) {
            selectionKey.cancel();
        }

        connectionManager.destroyConnection(connection, null, e);
    }

}
