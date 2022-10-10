/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpc.nio;

import com.hazelcast.internal.tpc.ReadHandler;
import com.hazelcast.internal.tpc.SyncSocket;
import com.hazelcast.internal.tpc.iobuffer.IOBuffer;
import com.hazelcast.internal.tpc.util.CloseUtil;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import static com.hazelcast.internal.tpc.util.CloseUtil.closeQuietly;
import static com.hazelcast.internal.tpc.util.Preconditions.checkNotNull;
import static java.net.StandardSocketOptions.SO_KEEPALIVE;
import static java.net.StandardSocketOptions.SO_LINGER;
import static java.net.StandardSocketOptions.SO_RCVBUF;
import static java.net.StandardSocketOptions.SO_SNDBUF;
import static java.net.StandardSocketOptions.TCP_NODELAY;

/**
 * Nio implementation of the {@link SyncSocket}.
 */
public final class NioSyncSocket extends SyncSocket {

    private final SocketChannel socketChannel;
    private final IOVector ioVector = new IOVector();
    private ByteBuffer receiveBuffer;
    private NioSyncReadHandler readHandler;

    public static NioSyncSocket open() {
        return new NioSyncSocket();
    }

    private NioSyncSocket() {
        try {
            this.socketChannel = SocketChannel.open();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void readHandler(NioSyncReadHandler readHandler) {
        this.readHandler = checkNotNull(readHandler, "readHandler can't be null");
    }

    /**
     * Returns the underlying {@link SocketChannel}.
     *
     * @return the SocketChannel.
     */
    public SocketChannel socketChannel() {
        return socketChannel;
    }

    @Override
    public void soLinger(int soLinger) {
        try {
            socketChannel.setOption(SO_LINGER, soLinger);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public int soLinger() {
        try {
            return socketChannel.getOption(SO_LINGER);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void keepAlive(boolean keepAlive) {
        try {
            socketChannel.setOption(SO_KEEPALIVE, keepAlive);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public boolean isKeepAlive() {
        try {
            return socketChannel.getOption(SO_KEEPALIVE);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public boolean isTcpNoDelay() {
        try {
            return socketChannel.getOption(TCP_NODELAY);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void tcpNoDelay(boolean tcpNoDelay) {
        try {
            socketChannel.setOption(TCP_NODELAY, tcpNoDelay);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public int receiveBufferSize() {
        try {
            return socketChannel.getOption(SO_RCVBUF);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void receiveBufferSize(int size) {
        try {
            socketChannel.setOption(SO_RCVBUF, size);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public int sendBufferSize() {
        try {
            return socketChannel.getOption(SO_SNDBUF);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void sendBufferSize(int size) {
        try {
            socketChannel.setOption(SO_SNDBUF, size);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public IOBuffer read() {
        if (receiveBuffer == null) {
            receiveBuffer = ByteBuffer.allocateDirect(receiveBufferSize());
            receiveBuffer.flip();
        }

        try {
            for (; ; ) {
                IOBuffer buf = readHandler.decode(receiveBuffer);
                if (buf != null) {
                    return buf;
                }

                if (receiveBuffer.hasRemaining()) {
                    receiveBuffer.compact();
                } else {
                    receiveBuffer.clear();
                }

                int read = socketChannel.read(receiveBuffer);
                if (read == -1) {
                    close();
                    throw new IOException("Socket closed " + this);
                }

                bytesRead.inc(read);
                receiveBuffer.flip();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public IOBuffer tryRead() {
        if (receiveBuffer == null) {
            receiveBuffer = ByteBuffer.allocateDirect(receiveBufferSize());
            receiveBuffer.flip();
        }

        try {
            IOBuffer buf = readHandler.decode(receiveBuffer);
            if (buf != null) {
                return buf;
            }

            if (receiveBuffer.hasRemaining()) {
                receiveBuffer.compact();
            } else {
                receiveBuffer.clear();
            }

            int read = socketChannel.read(receiveBuffer);
            if (read == -1) {
                close();
                throw new IOException("Socket closed " + this);
            }
            bytesRead.inc(read);
            receiveBuffer.flip();

            return readHandler.decode(receiveBuffer);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void flush() {
        try {
            long written = ioVector.write(socketChannel);
            bytesWritten.inc(written);
            //System.out.println(this + " bytes written:" + written);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public boolean write(IOBuffer buf) {
        return ioVector.offer(checkNotNull(buf));
    }

    @Override
    public boolean writeAndFlush(IOBuffer buf) {
        boolean result = write(buf);
        flush();
        return result;
    }

    @Override
    public void connect(SocketAddress address) {
        try {
            socketChannel.connect(address);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to connect to [" + address + "]", e);
        }
    }

    @Override
    protected void doClose() throws IOException {
        closeQuietly(socketChannel);
    }
}
