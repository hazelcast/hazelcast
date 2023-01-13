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
import com.hazelcast.internal.tpc.util.ProgressIndicator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.internal.tpc.util.Preconditions.checkNotNull;

/**
 * A 'client' Socket that is asynchronous. So reads and writes do not block,
 * but are executed on an {@link Eventloop}.
 */
@SuppressWarnings({"checkstyle:MethodCount", "checkstyle:VisibilityModifier"})
public abstract class AsyncSocket extends Socket {

    protected volatile SocketAddress remoteAddress;
    protected volatile SocketAddress localAddress;
    protected boolean clientSide;

    protected final ProgressIndicator ioBuffersWritten = new ProgressIndicator();
    protected final ProgressIndicator bytesRead = new ProgressIndicator();
    protected final ProgressIndicator bytesWritten = new ProgressIndicator();
    protected final ProgressIndicator ioBuffersRead = new ProgressIndicator();
    protected final ProgressIndicator writeEvents = new ProgressIndicator();
    protected final ProgressIndicator readEvents = new ProgressIndicator();

    protected ReadHandler readHandler;

    /**
     * Gets the number of bytes read.
     *
     * @return number of bytes read.
     */
    public final long getBytesRead() {
        return bytesRead.get();
    }

    /**
     * Gets the number of bytes written.
     *
     * @return number of bytes written.
     */
    public final long getBytesWritten() {
        return bytesWritten.get();
    }

    /**
     * Gets the number of IOBuffers read.
     *
     * @return the number of IOBuffers read.
     */
    public final long getIoBuffersRead() {
        return ioBuffersRead.get();
    }

    /**
     * Gets the number of IOBuffers written.
     *
     * @return the number of IOBuffers written.
     */
    public final long getIoBuffersWritten() {
        return ioBuffersWritten.get();
    }

    /**
     * Gets the number of write events.
     *
     * @return the number of write events.
     */
    public final long getWriteEvents() {
        return writeEvents.get();
    }

    /**
     * Gets the number of read events.
     *
     * @return the number of read events.
     */
    public final long getReadEvents() {
        return readEvents.get();
    }

    /**
     * Gets the {@link Eventloop} this {@link AsyncSocket} belongs to.
     *
     * @return the {@link Eventloop} this AsyncSocket belongs to or null if
     * the AsyncSocket has not been activated yet.
     */
    public abstract Eventloop eventloop();

    /**
     * Gets the remote address.
     * <p>
     * If the AsyncSocket isn't connected yet, null is returned.
     * <p>
     * This method is thread-safe.
     *
     * @return the remote address.
     */
    public final SocketAddress getRemoteAddress() {
        return remoteAddress;
    }

    /**
     * Gets the local address.
     * <p>
     * If the AsyncSocket isn't connected yet, null is returned.
     * <p>
     * This method is thread-safe.
     *
     * @return the local address.
     */
    public final SocketAddress getLocalAddress() {
        return localAddress;
    }

    /**
     * Set the SO_KEEPALIVE option.
     *
     * @param keepAlive a boolean indicating whether or not SO_KEEPALIVE is enabled.
     * @throws UncheckedIOException if something failed with configuring the socket
     */
    public abstract void setKeepAlive(boolean keepAlive);

    /**
     * Tests if SO_KEEPALIVE is enabled.
     *
     * @return a boolean indicating whether or not SO_KEEPALIVE is enabled.
     * @throws UncheckedIOException if something failed with configuring the socket
     */
    public abstract boolean isKeepAlive();

    /**
     * Sets the interval in seconds between the last data packet sent (simple ACKs are
     * not considered data) and the first keepalive probe; after the connection is marked
     * to need keepalive, this counter is not used any further.
     * <p/>
     * If the setting isn't supported, the call is ignored.
     *
     * @param keepAliveTime the keep alive time.
     * @throws IllegalArgumentException if keepAliveIntvl is smaller than 0.
     * @throws UncheckedIOException     if something failed with configuring the socket
     */
    public abstract void setTcpKeepAliveTime(int keepAliveTime);

    /**
     * Gets the interval in seconds between the last data packet sent (simple ACKs are not
     * considered data) and the first keepalive probe; after the connection is marked to need
     * keepalive, this counter is not used any further.
     * <p/>
     * If the setting isn't supported, 0 is returned.
     *
     * @return the interval.
     * @throws UncheckedIOException if something failed with configuring the socket
     */
    public abstract int getTcpKeepAliveTime();

    /**
     * Sets the interval in seconds between subsequent keepalive probes, regardless of what the
     * connection has exchanged in the meantime.
     * <p/>
     * If the setting isn't supported, the call is ignored.
     *
     * @param keepaliveIntvl the interval in seconds.
     * @throws IllegalArgumentException if keepAliveIntvl is smaller than 0.
     * @throws UncheckedIOException     if something failed with configuring the socket
     */
    public abstract void setTcpKeepaliveIntvl(int keepaliveIntvl);

    /**
     * Gets the interval in seconds between subsequent keepalive probes, regardless of what
     * the connection has exchanged in the meantime
     * <p/>
     * If the setting isn't supported, 0 is returned.
     *
     * @return the interval.
     * @throws UncheckedIOException if something failed with configuring the socket
     */
    public abstract int getTcpKeepaliveIntvl();

    /**
     * Sets the number of unacknowledged probes to send before considering the connection
     * dead and notifying the application layer.
     * <p/>
     * If the setting isn't supported, the call is ignored.
     *
     * @param keepAliveProbes the number of unacknowledged probes.
     * @throws UncheckedIOException if something failed with configuring the socket
     */
    public abstract void setTcpKeepAliveProbes(int keepAliveProbes);

    /**
     * Gets the number of unacknowledged probes to send before considering the connection
     * dead and notifying the application layer.
     * <p/>
     * If the setting isn't supported, 0 is returned.
     *
     * @return the number of unacknowledged probes.
     * @throws UncheckedIOException if something failed with configuring the socket
     */
    public abstract int getTcpKeepaliveProbes();

    /**
     * Sets the TCP_NODELAY option.
     *
     * @param tcpNoDelay a boolean indicating whether or not TCP_NODELAY is enabled.
     * @throws UncheckedIOException if something failed with configuring the socket
     */
    public abstract void setTcpNoDelay(boolean tcpNoDelay);

    /**
     * Tests if TCP_NODELAY is enabled.
     *
     * @return a boolean indicating whether or not TCP_NODELAY is enabled.
     * @throws UncheckedIOException if something failed with configuring the socket
     */
    public abstract boolean isTcpNoDelay();

    /**
     * Sets the receive buffer size in bytes.
     *
     * @param size the receive buffer size in bytes.
     * @throws IllegalArgumentException when the size isn't positive.
     * @throws UncheckedIOException     if something failed with configuring the socket
     */
    public abstract void setReceiveBufferSize(int size);

    /**
     * Gets the receive buffer size in bytes.
     *
     * @return the size of the receive buffer.
     * @throws UncheckedIOException if something failed with configuring the socket
     */
    public abstract int getReceiveBufferSize();

    /**
     * Sets the send buffer size in bytes.
     *
     * @param size the send buffer size in bytes.
     * @throws IllegalArgumentException when the size isn't positive.
     * @throws UncheckedIOException     if something failed with configuring the socket
     */
    public abstract void setSendBufferSize(int size);

    /**
     * Gets the send buffer size in bytes.
     *
     * @return the size of the send buffer.
     * @throws UncheckedIOException if something failed with configuring the socket
     */
    public abstract int getSendBufferSize();

    /**
     * Sets the read handler. Should be called before this AsyncSocket is activated.
     *
     * @param readHandler the ReadHandler
     * @throws NullPointerException if readHandler is null.
     */
    public final void setReadHandler(ReadHandler readHandler) {
        this.readHandler = checkNotNull(readHandler);
        this.readHandler.init(this);
    }

    /**
     * Activates an AsyncSocket by hooking it up to an EventLoop.
     * <p>
     * This method should only be called once.
     * <p>
     * This method is not thread-safe.
     *
     * @param eventloop the Eventloop this AsyncSocket belongs to.
     * @throws NullPointerException  if eventloop is null.
     * @throws IllegalStateException if this AsyncSocket is already activated.
     */
    public abstract void activate(Eventloop eventloop);

    /**
     * Ensures that any scheduled IOBuffers are flushed to the socket.
     * <p>
     * What happens under the hood is that the AsyncSocket is scheduled in the
     * {@link Eventloop} where at some point in the future the IOBuffers get written
     * to the socket.
     * <p>
     * This method is thread-safe.
     * <p>
     * This call is ignored when then AsyncSocket is already closed.
     */
    public abstract void flush();

    /**
     * Writes a IOBuffer to the AsyncSocket without scheduling the AsyncSocket
     * in the eventloop.
     * <p>
     * This call can be used to buffer a series of IOBuffers and then call
     * {@link #flush()} to trigger the actual writing to the socket.
     * <p>
     * There is no guarantee that IOBuffer is actually going to be received by the caller after
     * the AsyncSocket has accepted the IOBuffer. E.g. when the TCP/IP connection is dropped.
     * <p>
     * This method is thread-safe.
     *
     * @param buf the IOBuffer to write.
     * @return true if the IOBuffer was accepted, false otherwise.
     */
    public abstract boolean write(IOBuffer buf);

    public abstract boolean writeAll(Collection<IOBuffer> bufs);

    /**
     * Writes a IOBuffer and flushes it.
     * <p>
     * This is the same as calling {@link #write(IOBuffer)} followed by a {@link #flush()}.
     * <p>
     * There is no guarantee that IOBuffer is actually going to be received by the caller if
     * the AsyncSocket has accepted the IOBuffer. E.g. when the connection closes.
     * <p>
     * This method is thread-safe.
     *
     * @param buf the IOBuffer to write.
     * @return true if the IOBuffer was accepted, false otherwise.
     */
    public abstract boolean writeAndFlush(IOBuffer buf);

    /**
     * Writes a IOBuffer and ensure it gets written.
     * <p>
     * Should only be called from within the Eventloop.
     */
    public abstract boolean unsafeWriteAndFlush(IOBuffer buf);

    /**
     * Connects asynchronously to some address.
     *
     * @param address the address to connect to.
     * @return a {@link CompletableFuture}
     */
    public abstract CompletableFuture<Void> connect(SocketAddress address);

    @Override
    protected void close0() throws IOException {
        localAddress = null;
        remoteAddress = null;
    }

    @Override
    public final String toString() {
        return getClass().getSimpleName() + "[" + localAddress + "->" + remoteAddress + "]";
    }
}
