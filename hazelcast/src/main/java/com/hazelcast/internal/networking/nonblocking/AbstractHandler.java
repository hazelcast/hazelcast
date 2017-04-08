/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.networking.nonblocking;

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.networking.SocketChannelWrapper;
import com.hazelcast.internal.networking.SocketConnection;
import com.hazelcast.internal.networking.nonblocking.iobalancer.IOBalancer;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.logging.ILogger;

import java.io.EOFException;
import java.io.IOException;
import java.nio.channels.SelectionKey;

import static com.hazelcast.internal.metrics.ProbeLevel.DEBUG;
import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;

public abstract class AbstractHandler
        implements SelectionHandler, MigratableHandler {

    // for the time being we configure using a int until we have decided which load strategy to use.
    protected static final int LOAD_TYPE = Integer.getInteger("io.load", 0);

    @Probe(name = "handleCount")
    protected final SwCounter handleCount = newSwCounter();
    protected final ILogger logger;
    protected final SocketChannelWrapper socketChannel;
    protected final SocketConnection connection;
    protected NonBlockingIOThread ioThread;
    protected SelectionKey selectionKey;
    private final int initialOps;
    private final IOBalancer ioBalancer;

    // shows the id of the ioThread that is currently owning the handler
    @Probe
    private volatile int ioThreadId;

    // counts the number of migrations that have happened so far.
    @Probe
    private final SwCounter migrationCount = newSwCounter();

    public AbstractHandler(SocketConnection connection,
                           NonBlockingIOThread ioThread,
                           int initialOps,
                           ILogger logger,
                           IOBalancer ioBalancer) {
        this.connection = connection;
        this.socketChannel = connection.getSocketChannel();
        this.ioThread = ioThread;
        this.ioThreadId = ioThread.id;
        this.logger = logger;
        this.initialOps = initialOps;
        this.ioBalancer = ioBalancer;
    }

    public SocketChannelWrapper getSocketChannel() {
        return socketChannel;
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
        int interestOps = selectionKey.interestOps();
        if ((interestOps & operation) != 0) {
            selectionKey.interestOps(interestOps & ~operation);
        }
    }

    protected abstract void publish();

    @Override
    public void onFailure(Throwable e) {
        if (e instanceof OutOfMemoryError) {
            ioThread.getOomeHandler().handle((OutOfMemoryError) e);
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

        ioBalancer.signalMigrationComplete();

        if (!socketChannel.isOpen()) {
            return;
        }

        selectionKey = getSelectionKey();
        registerOp(initialOps);
    }
}
