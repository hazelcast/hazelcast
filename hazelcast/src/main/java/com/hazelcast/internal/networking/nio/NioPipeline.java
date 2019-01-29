/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.networking.nio;

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.ChannelErrorHandler;
import com.hazelcast.internal.networking.nio.iobalancer.IOBalancer;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.logging.ILogger;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import static com.hazelcast.internal.metrics.ProbeLevel.DEBUG;
import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;

public abstract class NioPipeline implements MigratablePipeline, Closeable {

    protected static final int LOAD_BALANCING_HANDLE = 0;
    protected static final int LOAD_BALANCING_BYTE = 1;
    protected static final int LOAD_BALANCING_FRAME = 2;

    // for the time being we configure using a int until we have decided which load strategy to use.
    protected final int loadType = Integer.getInteger("hazelcast.io.load", LOAD_BALANCING_BYTE);

    @Probe
    protected final SwCounter processCount = newSwCounter();
    @Probe
    protected final SwCounter completedMigrations = newSwCounter();
    protected final ILogger logger;
    protected final Channel channel;
    protected NioThread owner;
    protected SelectionKey selectionKey;
    private final ChannelErrorHandler errorHandler;
    private final SocketChannel socketChannel;
    private final int initialOps;
    private final IOBalancer ioBalancer;

    // shows the ID of the owner that is currently owning the handler
    @Probe
    private volatile int ownerId;

    // counts the number of migrations that have happened so far.
    @Probe
    private final SwCounter migrationCount = newSwCounter();

    NioPipeline(NioChannel channel,
                NioThread owner,
                ChannelErrorHandler errorHandler,
                int initialOps,
                ILogger logger,
                IOBalancer ioBalancer) {
        this.channel = channel;
        this.socketChannel = channel.socketChannel();
        this.errorHandler = errorHandler;
        this.ownerId = owner.id;
        this.owner = owner;
        this.ownerId = owner.id;
        this.logger = logger;
        this.initialOps = initialOps;
        this.ioBalancer = ioBalancer;
    }

    public Channel getChannel() {
        return channel;
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
    public NioThread owner() {
            return owner;
    }

    public void start() {
        owner.addTaskAndWakeup(new Runnable() {
            @Override
            public void run() {
                try {
                    getSelectionKey();
                } catch (Throwable t) {
                    onFailure(t);
                }
            }
        });
    }

    SelectionKey getSelectionKey() throws IOException {
        if (selectionKey == null) {
            selectionKey = socketChannel.register(owner.getSelector(), initialOps, this);
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

    protected abstract void publishMetrics();

    /**
     * Called when there are bytes available for reading, or space available to write.
     *
     * Any exception that leads to a termination of the connection like an IOException should not be handled in the handle method
     * but should be propagated. The reason behind this is that the handle logic already is complicated enough and by pulling it
     * out the flow will be easier to understand.
     *
     * @throws Exception
     */
    public abstract void process() throws Exception;

    /**
     * Is called when the {@link #process()} throws an exception.
     *
     * The idiom to use a pipeline is:
     * <code>
     *     try{
     *         pipeline.handle();
     *     } catch(Throwable t) {
     *         pipeline.onFailure(t);
     *     }
     * </code>
     *
     * @param e
     */
    public void onFailure(Throwable e) {
        if (e instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }

        if (selectionKey != null) {
            selectionKey.cancel();
        }

        errorHandler.onError(channel, e);
    }

    // This method run on the oldOwner NioThread
    void startMigration(final NioThread newOwner) throws IOException {
        assert owner == Thread.currentThread() : "startMigration can only run on the owning NioThread";
        assert owner != newOwner : "newOwner can't be the same as the existing owner";

        if (!socketChannel.isOpen()) {
            // if the channel is closed, we are done.
            return;
        }

        migrationCount.inc();

        unregisterOp(initialOps);
        owner = newOwner;
        ownerId = owner.id;
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

    private void completeMigration(NioThread newOwner) throws IOException {
        assert owner == newOwner;

        completedMigrations.inc();
        ioBalancer.signalMigrationComplete();

        if (!socketChannel.isOpen()) {
            return;
        }

        selectionKey = getSelectionKey();
        registerOp(initialOps);
    }
}
