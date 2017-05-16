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

package com.hazelcast.internal.networking.nio;

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.networking.ChannelConnection;
import com.hazelcast.internal.networking.ChannelInboundHandler;
import com.hazelcast.internal.networking.ChannelInitializer;
import com.hazelcast.internal.networking.ChannelReader;
import com.hazelcast.internal.networking.InitResult;
import com.hazelcast.internal.networking.nio.iobalancer.IOBalancer;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.logging.ILogger;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;
import static java.lang.System.currentTimeMillis;
import static java.nio.channels.SelectionKey.OP_READ;

/**
 * A {@link ChannelReader} tailored for non blocking IO.
 *
 * When the {@link NioThread} receives a read event from the {@link java.nio.channels.Selector}, then the
 * {@link #handle()} is called to read out the data from the socket into a bytebuffer and hand it over to the
 * {@link ChannelInboundHandler} to get processed.
 */
public final class NioChannelReader
        extends AbstractHandler
        implements ChannelReader {

    protected ByteBuffer inputBuffer;

    @Probe(name = "bytesRead")
    private final SwCounter bytesRead = newSwCounter();
    @Probe(name = "normalFramesRead")
    private final SwCounter normalFramesRead = newSwCounter();
    @Probe(name = "priorityFramesRead")
    private final SwCounter priorityFramesRead = newSwCounter();
    private final ChannelInitializer initializer;
    private ChannelInboundHandler inboundHandler;
    private volatile long lastReadTime;

    private long bytesReadLastPublish;
    private long normalFramesReadLastPublish;
    private long priorityFramesReadLastPublish;
    private long handleCountLastPublish;

    public NioChannelReader(
            ChannelConnection connection,
            NioThread ioThread,
            ILogger logger,
            IOBalancer balancer,
            ChannelInitializer initializer) {
        super(connection, ioThread, OP_READ, logger, balancer);
        this.initializer = initializer;
    }

    @Override
    public long getLoad() {
        switch (LOAD_TYPE) {
            case 0:
                return handleCount.get();
            case 1:
                return bytesRead.get();
            case 2:
                return normalFramesRead.get() + priorityFramesRead.get();
            default:
                throw new RuntimeException();
        }
    }

    @Probe(name = "idleTimeMs")
    private long idleTimeMs() {
        return Math.max(currentTimeMillis() - lastReadTime, 0);
    }

    @Override
    public SwCounter getNormalFramesReadCounter() {
        return normalFramesRead;
    }

    @Override
    public SwCounter getPriorityFramesReadCounter() {
        return priorityFramesRead;
    }

    @Override
    public long lastReadTimeMillis() {
        return lastReadTime;
    }

    @Override
    public void init() {
        ioThread.addTaskAndWakeup(new Runnable() {
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
    public void handle() throws Exception {
        handleCount.inc();
        // we are going to set the timestamp even if the channel is going to fail reading. In that case
        // the connection is going to be closed anyway.
        lastReadTime = currentTimeMillis();

        if (inboundHandler == null) {
            InitResult<ChannelInboundHandler> init = initializer.initInbound(connection, this);
            if (init == null) {
                // when using SSL, we can read 0 bytes since data read from socket can be handshake frames.
                return;
            }
            this.inboundHandler = init.getHandler();
            this.inputBuffer = init.getByteBuffer();
        }

        int readBytes = channel.read(inputBuffer);
        if (readBytes <= 0) {
            if (readBytes == -1) {
                throw new EOFException("Remote socket closed!");
            }
            return;
        }

        bytesRead.inc(readBytes);

        inputBuffer.flip();
        inboundHandler.onRead(inputBuffer);
        if (inputBuffer.hasRemaining()) {
            inputBuffer.compact();
        } else {
            inputBuffer.clear();
        }
    }

    @Override
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
        ioThread.addTaskAndWakeup(new Runnable() {
            @Override
            public void run() {
                if (ioThread != Thread.currentThread()) {
                    // the NioChannelReader has migrated to a different IOThread after the close got called.
                    // so we need to send the task to the right ioThread. Otherwise multiple ioThreads could be accessing
                    // the same channel.
                    ioThread.addTaskAndWakeup(this);
                    return;
                }

                try {
                    channel.closeInbound();
                } catch (IOException e) {
                    logger.finest("Error while closing inbound", e);
                }
            }
        });
    }

    @Override
    public String toString() {
        return connection + ".channelReader";
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
                startMigration(newOwner);
            } catch (Throwable t) {
                onFailure(t);
            }
        }
    }
}
