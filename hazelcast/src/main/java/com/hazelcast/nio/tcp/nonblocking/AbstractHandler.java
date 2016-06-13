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

package com.hazelcast.nio.tcp.nonblocking;

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.tcp.SocketChannelWrapper;
import com.hazelcast.nio.tcp.TcpIpConnection;
import com.hazelcast.nio.tcp.TcpIpConnectionManager;

import java.io.EOFException;
import java.io.IOException;
import java.nio.channels.SelectionKey;

import static com.hazelcast.internal.metrics.ProbeLevel.DEBUG;

public abstract class AbstractHandler implements MigratableHandler {

    protected final ILogger logger;
    protected final SocketChannelWrapper socketChannel;
    protected final TcpIpConnection connection;
    protected final TcpIpConnectionManager connectionManager;
    protected final IOService ioService;
    protected NonBlockingIOThread ioThread;
    protected SelectionKey selectionKey;
    private final int initialOps;

    // shows the id of the ioThread that is currently owning the handler
    @Probe
    private volatile int ioThreadId;

    // counts the number of migrations that have happened so far.
    @Probe
    private SwCounter migrationCount = SwCounter.newSwCounter();

    public AbstractHandler(TcpIpConnection connection, NonBlockingIOThread ioThread, int initialOps) {
        this.connection = connection;
        this.ioThread = ioThread;
        this.ioThreadId = ioThread.id;
        this.socketChannel = connection.getSocketChannelWrapper();
        this.connectionManager = connection.getConnectionManager();
        this.ioService = connectionManager.getIoService();
        this.logger = ioService.getLogger(this.getClass().getName());
        this.initialOps = initialOps;
    }

    @Probe(level = DEBUG)
    private long opsInterested() {
        SelectionKey selectionKey = this.selectionKey;
        return selectionKey == null ? -1 : selectionKey.interestOps();
    }

    @Probe(level = DEBUG)
    private long opsReady() {
        SelectionKey selectionKey = this.selectionKey;
        return selectionKey == null ? -1 : selectionKey.readyOps();
    }

    @Override
    public NonBlockingIOThread getOwner() {
        return ioThread;
    }

    protected SelectionKey getSelectionKey() throws IOException {
        if (selectionKey == null) {
            selectionKey = socketChannel.register(ioThread.getSelector(), initialOps, this);
        }
        return selectionKey;
    }

    final void setSelectionKey(SelectionKey selectionKey) {
        this.selectionKey = selectionKey;
    }

    final void registerOp(int operation) throws IOException {
        SelectionKey selectionKey = getSelectionKey();
        selectionKey.interestOps(selectionKey.interestOps() | operation);
    }

    final void unregisterOp(int operation) throws IOException {
        SelectionKey selectionKey = getSelectionKey();
        selectionKey.interestOps(selectionKey.interestOps() & ~operation);
    }

    @Override
    public void onFailure(Throwable e) {
        if (e instanceof OutOfMemoryError) {
            ioService.onOutOfMemory((OutOfMemoryError) e);
        }
        if (selectionKey != null) {
            selectionKey.cancel();
        }

        if (e instanceof EOFException) {
            connection.close("Connection closed by the other side", e);
        } else {
            connection.close("Exception in " + getClass().getSimpleName(), e);
        }
    }


    // This method run on the oldOwner NonBlockingIOThread
    void startMigration(final NonBlockingIOThread newOwner) throws IOException {
        assert ioThread == Thread.currentThread() : "startMigration can only run on the owning NonBlockingIOThread";
        assert ioThread != newOwner : "newOwner can't be the same as the existing owner";

        if (!socketChannel.isOpen()) {
            // if the channel is closed, we are done.
            return;
        }

        migrationCount.inc();

        unregisterOp(initialOps);
        ioThread = newOwner;
        ioThreadId = ioThread.id;
        selectionKey.cancel();
        selectionKey = null;

        newOwner.addTaskAndWakeup(new Runnable() {
            @Override
            public void run() {
                try {
                    completeMigration(newOwner);
                } catch (Throwable t) {
                    onFailure(t);
                }
            }
        });
    }

    private void completeMigration(NonBlockingIOThread newOwner) throws IOException {
        assert ioThread == newOwner;

        NonBlockingIOThreadingModel threadingModel =
                (NonBlockingIOThreadingModel) connection.getConnectionManager().getIoThreadingModel();
        threadingModel.getIOBalancer().signalMigrationComplete();

        if (!socketChannel.isOpen()) {
            return;
        }

        selectionKey = getSelectionKey();
        registerOp(initialOps);
    }
}
