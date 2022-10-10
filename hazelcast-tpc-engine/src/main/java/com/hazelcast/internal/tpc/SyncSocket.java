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

package com.hazelcast.internal.tpc;

import com.hazelcast.internal.tpc.iobuffer.IOBuffer;
import com.hazelcast.internal.tpc.logging.TpcLogger;
import com.hazelcast.internal.tpc.logging.TpcLoggerLocator;
import com.hazelcast.internal.tpc.util.ProgressIndicator;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * The SyncSocket is blocking; so therefor should not be used inside an {@link Eventloop}.
 * <p/>
 * Most methods in this class are not thread-safe.
 */
public abstract class SyncSocket implements Closeable {

    public final ConcurrentMap context = new ConcurrentHashMap();

    protected final TpcLogger logger = TpcLoggerLocator.getLogger(getClass());
    protected final AtomicBoolean closed = new AtomicBoolean();

    protected volatile SocketAddress remoteAddress;
    protected volatile SocketAddress localAddress;

    protected final ProgressIndicator ioBuffersWritten = new ProgressIndicator();

    protected final ProgressIndicator ioBuffersRead = new ProgressIndicator();

    protected final ProgressIndicator bytesRead = new ProgressIndicator();

    protected final ProgressIndicator bytesWritten = new ProgressIndicator();

    public final long ioBuffersWritten() {
        return ioBuffersWritten.get();
    }

    public final long bytesRead() {
        return bytesRead.get();
    }

    public final long bytesWritten() {
        return bytesWritten.get();
    }

    public final long ioBuffersRead() {
        return ioBuffersRead.get();
    }

    /**
     * Returns the remote address.
     * <p/>
     * If the SyncSocket isn't connected, null is returned.
     * <p/>
     * This method is thread-safe.
     *
     * @return the remote address.
     */
    public final SocketAddress remoteAddress() {
        return remoteAddress;
    }

    /**
     * Returns the local address.
     * <p/>
     * If the SyncSocket isn't connected, null is returned.
     * <p/>
     * This method is thread-safe.
     *
     * @return the local address.
     */
    public final SocketAddress localAddress() {
        return localAddress;
    }

    public abstract void soLinger(int soLinger);

    public abstract int soLinger();

    public abstract void keepAlive(boolean keepAlive);

    public abstract boolean isKeepAlive();

    public abstract void tcpNoDelay(boolean tcpNoDelay);

    public abstract boolean isTcpNoDelay();

    public abstract void receiveBufferSize(int size);

    public abstract int receiveBufferSize();

    public abstract void sendBufferSize(int size);

    public abstract int sendBufferSize();

    /**
     * TODO: Method needs to be redesigned; An IOBuffer should be passed as argument
     * and it should complete as soon as bytes are available.
     * <p>
     * Reads a single IOBuffer from this SyncSocket and will block if there is no frame.
     * <p>
     * This method is not thread-safe.
     *
     * @return the read Frame.
     * @throws java.io.UncheckedIOException if a problem happened while reading the frame.
     */
    public abstract IOBuffer read();

    /**
     * TODO: Method needs to be redesigned; An IOBuffer should be passed as argument
     * and it should complete as soon as bytes are available.
     * <p>
     * Tries to read a single Frame from this SyncSocket.
     * <p>
     * This method is not thread-safe.
     *
     * @return the read frame or null if not enough data was available to read a full frame.
     * @throws java.io.UncheckedIOException if a problem happened while reading the frame.
     */
    public abstract IOBuffer tryRead();

    /**
     * Writes any scheduled frames are flushed to the socket. THis call blocks until any
     * scheduled frame have been written to the socket.
     * <p>
     * This method is not thread-safe.
     */
    public abstract void flush();

    /**
     * Writes an IOBuffer to the SyncSocket. The IOBuffer isn't immediately written; it is just
     * buffered.
     * <p>
     * This call can be used to buffer a series of request and then call
     * {@link #flush()}.
     * <p>
     * This method is not thread-safe.
     * <p>
     * There is no guarantee that IOBuffer is actually going to be received by the caller if
     * the SyncSocket has accepted the IOBuffer. E.g. when the connection closes.
     *
     * @param buf the IOBuffer to write.
     * @return true if the IOBuffer was accepted, false otherwise.
     */
    public abstract boolean write(IOBuffer buf);

    /**
     * Writes a buf and writes it to the socket.
     * <p>
     * This is the same as calling {@link #write(IOBuffer)} followed by a {@link #flush()}.
     * <p>
     * There is no guarantee that buf is actually going to be received by the caller if
     * the SyncSocket has accepted the buf. E.g. when the connection closes.
     * <p>
     * This method is not thread-safe.
     * <p>
     * If there was no space, a {@link #flush()} is still triggered.
     *
     * @param buf the buf to write.
     * @return true if the buf was accepted, false if there was an overload.
     */
    public abstract boolean writeAndFlush(IOBuffer buf);

    /**
     * Connects synchronously to some address.
     *
     * @param address the address to connect to.
     * @throws UncheckedIOException if the connection could not be established.
     */
    public abstract void connect(SocketAddress address);

    /**
     * Closes this {@link SyncSocket}.
     * <p>
     * This method is thread-safe.
     * <p>
     * If the AsyncSocket is already closed, the call is ignored.
     */
    public final void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        try {
            doClose();
        } catch (Exception e) {
            logger.warning(e);
        }
    }

    protected abstract void doClose() throws IOException;

    /**
     * Checks if this SyncSocket is closed.
     * <p>
     * This method is thread-safe.
     *
     * @return true if closed, false otherwise.
     */
    public final boolean isClosed() {
        return closed.get();
    }

    @Override
    public final String toString() {
        return getClass().getSimpleName() + "[" + localAddress + "->" + remoteAddress + "]";
    }
}

