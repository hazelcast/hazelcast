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

package com.hazelcast.nio.tcp.spinning;

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.tcp.ReadHandler;
import com.hazelcast.nio.tcp.SocketChannelWrapper;
import com.hazelcast.nio.tcp.SocketReader;
import com.hazelcast.nio.tcp.SocketReaderInitializer;
import com.hazelcast.nio.tcp.TcpIpConnection;

import java.io.EOFException;
import java.nio.ByteBuffer;

import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;
import static java.lang.Math.max;
import static java.lang.System.currentTimeMillis;

public class SpinningSocketReader extends AbstractHandler implements SocketReader {

    @Probe(name = "bytesRead")
    private final SwCounter bytesRead = newSwCounter();
    @Probe(name = "normalFramesRead")
    private final SwCounter normalFramesRead = newSwCounter();
    @Probe(name = "priorityFramesRead")
    private final SwCounter priorityFramesRead = newSwCounter();
    private final SocketChannelWrapper socketChannel;
    private final SocketReaderInitializer initializer;
    private volatile long lastReadTime;
    private ReadHandler readHandler;
    private ByteBuffer inputBuffer;
    private final ByteBuffer protocolBuffer = ByteBuffer.allocate(3);

    public SpinningSocketReader(TcpIpConnection connection,
                                ILogger logger,
                                SocketChannelWrapper socketChannel,
                                SocketReaderInitializer initializer) {
        super(connection, logger);
        this.socketChannel = socketChannel;
        this.initializer = initializer;
    }

    @Override
    public ByteBuffer getProtocolBuffer() {
        return protocolBuffer;
    }

    @Override
    public void initInputBuffer(ByteBuffer inputBuffer) {
        this.inputBuffer = inputBuffer;
    }

    @Override
    public void initReadHandler(ReadHandler readHandler) {
        this.readHandler = readHandler;
    }

    @Override
    public long getLastReadTimeMillis() {
        return lastReadTime;
    }

    @Probe(name = "idleTimeMs")
    private long idleTimeMs() {
        return max(currentTimeMillis() - lastReadTime, 0);
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
    public SocketChannelWrapper getSocketChannel() {
        return socketChannel;
    }

    @Override
    public void init() {
        //no-op
    }

    @Override
    public void close() {
        //no-op
    }

    public void read() throws Exception {
        if (!connection.isAlive()) {
            socketChannel.closeInbound();
            return;
        }

        if (readHandler == null) {
            initializer.init(connection, this);
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

        lastReadTime = currentTimeMillis();
        bytesRead.inc(readBytes);
        inputBuffer.flip();
        readHandler.onRead(inputBuffer);
        if (inputBuffer.hasRemaining()) {
            inputBuffer.compact();
        } else {
            inputBuffer.clear();
        }
    }
}
