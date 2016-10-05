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
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.tcp.ReadHandler;
import com.hazelcast.nio.tcp.SocketChannelWrapper;
import com.hazelcast.nio.tcp.SocketReader;
import com.hazelcast.nio.tcp.nonblocking.iobalancer.IOBalancer;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;
import static java.lang.System.currentTimeMillis;

public abstract class AbstractNonBlockingSocketReader<C extends Connection>
        extends AbstractHandler<C>
        implements SocketReader {

    protected ByteBuffer inputBuffer;

    @Probe(name = "bytesRead")
    private final SwCounter bytesRead = newSwCounter();
    @Probe(name = "normalFramesRead")
    private final SwCounter normalFramesRead = newSwCounter();
    @Probe(name = "priorityFramesRead")
    private final SwCounter priorityFramesRead = newSwCounter();

    private ReadHandler readHandler;
    private volatile long lastReadTime;

    public AbstractNonBlockingSocketReader(
            C connection,
            SocketChannelWrapper socketChannel,
            NonBlockingIOThread ioThread,
            ILogger logger,
            IOBalancer balancer) {
        super(connection, ioThread, SelectionKey.OP_READ, socketChannel, logger, balancer);
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
     * Migrates this handler to a new NonBlockingIOThread.
     * The migration logic is rather simple:
     * <p><ul>
     * <li>Submit a de-registration task to a current NonBlockingIOThread</li>
     * <li>The de-registration task submits a registration task to the new NonBlockingIOThread</li>
     * </ul></p>
     *
     * @param newOwner target NonBlockingIOThread this handler migrates to
     */
    @Override
    public void requestMigration(NonBlockingIOThread newOwner) {
        ioThread.addTaskAndWakeup(new StartMigrationTask(newOwner));
    }

    @Override
    public void handle() throws Exception {
        eventCount.inc();
        // we are going to set the timestamp even if the socketChannel is going to fail reading. In that case
        // the connection is going to be closed anyway.
        lastReadTime = currentTimeMillis();

        if (readHandler == null) {
            readHandler = initReadHandler();
            if (readHandler == null) {
                // when using SSL, we can read 0 bytes since data read from socket can be handshake frames.
                return;
            }
        }

        int readBytes = socketChannel.read(inputBuffer);
        if (readBytes <= 0) {
            if (readBytes == -1) {
                throw new EOFException("Remote socket closed!");
            }
            return;
        }

        bytesRead.inc(readBytes);

        inputBuffer.flip();
        readHandler.onRead(inputBuffer);
        if (inputBuffer.hasRemaining()) {
            inputBuffer.compact();
        } else {
            inputBuffer.clear();
        }
    }

    protected abstract ReadHandler initReadHandler() throws IOException;

    protected void configureInputBuffer(int size, boolean direct) {
        inputBuffer = IOUtil.newByteBuffer(size, direct);
    }

    @Override
    public void close() {
        ioThread.addTaskAndWakeup(new Runnable() {
            @Override
            public void run() {
                if (ioThread != Thread.currentThread()) {
                    // the NonBlockingSocketReader has migrated to a different IOThread after the close got called.
                    // so we need to send the task to the right ioThread. Otherwise multiple ioThreads could be accessing
                    // the same socketChannel.
                    ioThread.addTaskAndWakeup(this);
                    return;
                }

                try {
                    socketChannel.closeInbound();
                } catch (IOException e) {
                    logger.finest("Error while closing inbound", e);
                }
            }
        });
    }

    @Override
    public String toString() {
        return connection + ".socketReader";
    }

    private class StartMigrationTask implements Runnable {
        private final NonBlockingIOThread newOwner;

        StartMigrationTask(NonBlockingIOThread newOwner) {
            this.newOwner = newOwner;
        }

        @Override
        public void run() {
            // if there is no change, we are done
            if (ioThread == newOwner) {
                return;
            }

            try {
                startMigration(newOwner);
            } catch (Throwable t) {
                onFailure(t);
            }
        }
    }
}
