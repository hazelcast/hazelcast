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
import com.hazelcast.internal.networking.ChannelInboundHandler;
import com.hazelcast.internal.networking.nio.iobalancer.IOBalancer;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.IOUtil;

import java.io.Closeable;
import java.io.EOFException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import static com.hazelcast.internal.metrics.ProbeLevel.DEBUG;
import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;
import static com.hazelcast.nio.IOUtil.compactOrClear;
import static java.lang.System.currentTimeMillis;
import static java.nio.channels.SelectionKey.OP_READ;

/**
 * This {@link NioOutboundPipeline} is the pipeline when data is received on the socket and requires processing. Once
 * the data is read from the socket, it will push the data down the pipeline.
 */
public final class NioInboundPipeline extends ChannelInboundHandler implements NioPipeline, Closeable {

    @Probe(name = "bytesRead")
    private final SwCounter bytesRead = newSwCounter();
    @Probe(name = "normalFramesRead")
    private final SwCounter normalFramesRead = newSwCounter();
    @Probe(name = "priorityFramesRead")
    private final SwCounter priorityFramesRead = newSwCounter();
    private final NioThread ioThread;
    // shows the ID of the ioThread that is currently owning the handler
    @Probe
    private volatile int nioThreadId;
    @Probe
    private final SwCounter handleCount = newSwCounter();
    private final SocketChannel socketChannel;
    private final ILogger logger;
    private final IOBalancer balancer;

    private volatile long lastReadTime;
    private volatile long bytesReadLastPublish;
    private volatile long normalFramesReadLastPublish;
    private volatile long priorityFramesReadLastPublish;
    private volatile long handleCountLastPublish;
    private SelectionKey selectionKey;

    public NioInboundPipeline(NioChannel channel, NioThread ioThread, ILogger logger, IOBalancer balancer) {
        this.channel = channel;
        this.socketChannel = channel.socketChannel();
        this.balancer = balancer;
        this.ioThread = ioThread;
        this.nioThreadId = ioThread.id;
        this.logger = logger;
    }

    @Override
    public long getLoad() {
        switch (LOAD_TYPE) {
            case LOAD_BALANCING_HANDLE:
                return handleCount.get();
            case LOAD_BALANCING_BYTE:
                return bytesRead.get();
            case LOAD_BALANCING_FRAME:
                return normalFramesRead.get() + priorityFramesRead.get();
            default:
                throw new RuntimeException();
        }
    }

    @Override
    public void renewSelectionKey() throws ClosedChannelException {
        if (selectionKey != null) {
            selectionKey.cancel();
        }
        selectionKey = socketChannel.register(ioThread.getSelector(), OP_READ, this);
    }

    @Override
    public NioThread getOwner() {
        return null;
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

    @Probe(name = "idleTimeMs")
    private long idleTimeMs() {
        return Math.max(currentTimeMillis() - lastReadTime, 0);
    }

    public SwCounter getNormalFramesReadCounter() {
        return normalFramesRead;
    }

    public SwCounter getPriorityFramesReadCounter() {
        return priorityFramesRead;
    }

    public long lastReadTimeMillis() {
        return lastReadTime;
    }

    /**
     * Migrates this handler to a new NioThread.
     * The migration logic is rather simple:
     * <p><ul>
     * <li>Submit a de-registration task to a current NioThread</li>
     * <li>The de-registration task submits a registration task to the new NioThread</li>
     * </ul></p>
     *
     * @param newOwner target NioThread this handler migrates to
     */
    @Override
    public void requestMigration(NioThread newOwner) {
        ioThread.addTaskAndWakeup(new StartMigrationTask(newOwner));
    }

    @Override
    public void onRead() throws Exception {
        //Thread.sleep(100);

        System.out.println(channel + " Inbound: First " + IOUtil.toDebug("src", src));
        //System.out.println(channel + " Inbound: next.class:" + next.getClass());

        handleCount.inc();
        // we are going to set the timestamp even if the channel is going to fail reading. In that case
        // the connection is going to be closed anyway.
        lastReadTime = currentTimeMillis();

        int readBytes = socketChannel.read(src);

        //System.out.println(channel + " Inbound: Bytes read:" + readBytes);

        if (readBytes <= 0) {
            if (readBytes == -1) {
                throw new EOFException("Remote socket closed!");
            }
            return;
        }

        bytesRead.inc(readBytes);
        src.flip();

        //  System.out.println();
        next.onRead();
        compactOrClear(src);
    }

    public void publish() {
        if (Thread.currentThread() != ioThread) {
            return;
        }

        ioThread.bytesTransceived += bytesRead.get() - bytesReadLastPublish;
        ioThread.framesTransceived += normalFramesRead.get() - normalFramesReadLastPublish;
        ioThread.priorityFramesTransceived += priorityFramesRead.get() - priorityFramesReadLastPublish;
        ioThread.handleCount += handleCount.get() - handleCountLastPublish;

        bytesReadLastPublish = bytesRead.get();
        normalFramesReadLastPublish = normalFramesRead.get();
        priorityFramesReadLastPublish = priorityFramesRead.get();
        handleCountLastPublish = handleCount.get();
    }

    @Override
    public void close() {
        //no-op
    }

    @Override
    public String toString() {
        return channel + ".channelReader";
    }

    public void start() {
        ioThread.addTaskAndWakeup(new Runnable() {
            @Override
            public void run() {
                try {
                    renewSelectionKey();
                } catch (Throwable t) {
                    onFailure(t);
                }
            }
        });
    }

    @Override
    public void onFailure(Throwable e) {
        if (e instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }

        //todo: should this stay here?
        // can be done in the close
        //todo: should this be done silently?
        if (selectionKey != null) {
            selectionKey.cancel();
        }

        ioThread.getErrorHandler().onError(channel, e);
    }

    private class StartMigrationTask implements Runnable {
        private final NioThread newOwner;

        StartMigrationTask(NioThread newOwner) {
            this.newOwner = newOwner;
        }

        @Override
        public void run() {
            // if there is no change, we are done
            if (ioThread == newOwner) {
                return;
            }

            publish();

            try {
                //todo
                //startMigration(newOwner);
            } catch (Throwable t) {
                onFailure(t);
            }
        }
    }
}
