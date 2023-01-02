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
import com.hazelcast.internal.tpc.util.LongCounter;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.tpc.util.Preconditions.checkNotNull;

/**
 * A Socket that is asynchronous. So reads and writes do not block,
 * but are executed on an {@link Eventloop}.
 */
@SuppressWarnings({"checkstyle:MethodCount", "checkstyle:VisibilityModifier"})
public abstract class AsyncSocket implements Closeable {

    /**
     * Allows for objects to be bound to this AsyncSocket. Useful for the lookup of services and other dependencies.
     */
    @SuppressWarnings("")
    public final ConcurrentMap<?, ?> context = new ConcurrentHashMap<>();

    protected final TpcLogger logger = TpcLoggerLocator.getLogger(getClass());
    protected final AtomicBoolean closed = new AtomicBoolean();

    protected volatile SocketAddress remoteAddress;
    protected volatile SocketAddress localAddress;
    protected boolean clientSide;

    protected final LongCounter ioBuffersWritten = new LongCounter();
    protected final LongCounter bytesRead = new LongCounter();
    protected final LongCounter bytesWritten = new LongCounter();
    protected final LongCounter ioBuffersRead = new LongCounter();
    protected final LongCounter handleWriteCnt = new LongCounter();
    protected final LongCounter readEvents = new LongCounter();

    private CloseListener closeListener;
    private Executor closeExecutor;
    protected ReadHandler readHandler;

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

    public final long handleWriteCnt() {
        return handleWriteCnt.get();
    }

    public final long readEvents() {
        return readEvents.get();
    }

    /**
     * Returns the {@link Eventloop} this {@link AsyncSocket} belongs to.
     *
     * @return the {@link Eventloop} this AsyncSocket belongs to or null if
     * the AsyncSocket has not been activated yet.
     */
    public abstract Eventloop eventloop();

    /**
     * Returns the remote address.
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
     * Returns the local address.
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

    // TODO: This option only makes sense for blocking sockets according to StandardSocketOptions.SO_LINGER
    public abstract void setSoLinger(int soLinger);

    public abstract int getSoLinger();

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
     * Get the interval in seconds between the last data packet sent (simple ACKs are not considered data) and
     * the first keepalive probe; after the connection is marked to need keepalive, this counter is not used
     * any further.
     *
     * @param keepAliveTime the keep alive time.
     * @throws IllegalArgumentException if keepAliveIntvl is smaller than 0.
     * @throws UncheckedIOException if something failed with configuring the socket
     */
    public abstract void setTcpKeepAliveTime(int keepAliveTime);

    /**
     * Get the interval in seconds between the last data packet sent (simple ACKs are not considered data) and
     * the first keepalive probe; after the connection is marked to need keepalive, this counter is not used any
     * further.
     *
     * @return the interval.
     * @throws UncheckedIOException if something failed with configuring the socket
     */
    public abstract int getTcpKeepAliveTime();

    /**
     * Sets the interval in seconds between subsequent keepalive probes, regardless of what the connection
     * has exchanged in the meantime
     *
     * @param keepaliveIntvl the interval in seconds.
     * @throws IllegalArgumentException if keepAliveIntvl is smaller than 0.
     * @throws UncheckedIOException if something failed with configuring the socket
     */
    public abstract void setTcpKeepaliveIntvl(int keepaliveIntvl);

    /**
     * Gets the interval in seconds between subsequent keepalive probes, regardless of what the connection
     * has exchanged in the meantime
     *
     * @return the interval.
     * @throws UncheckedIOException if something failed with configuring the socket
     */
    public abstract int getTcpKeepaliveIntvl();

    /**
     * Sets the number of unacknowledged probes to send before considering the connection dead and notifying the
     * application layer.
     *
     * @param keepAliveProbes the number of unacknowledged probes.
     * @throws UncheckedIOException if something failed with configuring the socket
     */
    public abstract void setTcpKeepAliveProbes(int keepAliveProbes);

    /**
     * Gets the number of unacknowledged probes to send before considering the connection dead and notifying the
     * application layer.
     *
     * @return the number of unacknowledged probes.
     * @throws UncheckedIOException if something failed with configuring the socket
     */
    public abstract int getTcpKeepaliveProbes();

    /**
     * Set the TCP_NODELAY option.
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
     * Sets the receivebuffer size in bytes.
     *
     * @param size the receivebuffer size in bytes.
     * @throws IllegalArgumentException when the size isn't positive.
     * @throws UncheckedIOException     if something failed with configuring the socket
     */
    public abstract void setReceiveBufferSize(int size);

    /**
     * Gets the receivebuffer size in bytes.
     *
     * @return the size of the receive buffer.
     * @throws UncheckedIOException if something failed with configuring the socket
     */
    public abstract int getReceiveBufferSize();

    /**
     * Sets the sendbuffer size in bytes.
     *
     * @param size the sendbuffer size in bytes.
     * @throws IllegalArgumentException when the size isn't positive.
     * @throws UncheckedIOException     if something failed with configuring the socket
     */
    public abstract void setSendBufferSize(int size);

    /**
     * Gets the sendbuffer size in bytes.
     *
     * @return the size of the send buffer.
     * @throws UncheckedIOException if something failed with configuring the socket
     */
    public abstract int getSendBufferSize();

    /**
     * Sets  the ReadHandler. Should be called before this AsyncSocket is activated.
     *
     * @param readHandler the ReadHandler
     * @throws NullPointerException if readHandler is null.
     */
    public final void setReadHandler(ReadHandler readHandler) {
        this.readHandler = checkNotNull(readHandler);
        this.readHandler.init(this);
    }

    /**
     * Configures the CloseListener.
     * <p>
     * Call should be made before {@link #activate(Eventloop)} is called. This method is not
     * threadsafe.
     *
     * @param listener the CloseListener
     * @param executor the executor used to execute the {@link CloseListener#close()} method.
     * @throws NullPointerException if listener or executor is null.
     */
    public final void setCloseListener(CloseListener listener, Executor executor) {
        this.closeListener = checkNotNull(listener, "listener");
        this.closeExecutor = checkNotNull(executor, "executor");
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
    public abstract CompletableFuture<AsyncSocket> connect(SocketAddress address);

    /**
     * Closes this {@link AsyncSocket}.
     * <p>
     * This method is thread-safe.
     * <p>
     * If the AsyncSocket is already closed, the call is ignored.
     */
    public final void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        if (logger.isInfoEnabled()) {
            logger.info("Closing  " + this);
        }

        try {
            close0();
        } catch (Exception e) {
            logger.warning(e);
        }

        localAddress = null;
        remoteAddress = null;

        if (closeListener != null) {
            try {
                closeExecutor.execute(() -> {
                    try {
                        closeListener.onClose(AsyncSocket.this);
                    } catch (Throwable e) {
                        logger.warning(e);
                    }
                });
            } catch (Throwable e) {
                logger.warning(e);
            }
        }
    }

    /**
     * Does the actual closing. No guarantee is made on which thread this is called.
     * <p/>
     * Is guaranteed to be called at most once.
     *
     * @throws IOException
     */
    protected abstract void close0() throws IOException;

    /**
     * Checks if this AsyncSocket is closed.
     * <p>
     * This method is thread-safe.
     *
     * @return true if closed, false otherwise.
     */
    public final boolean isClosed() {
        return closed.get();
    }

    interface CloseListener {
        void onClose(AsyncSocket socket);
    }

    @Override
    public final String toString() {
        if (clientSide) {
            return getClass().getSimpleName() + "[" + localAddress + "->" + remoteAddress + "]";
        } else {
            return getClass().getSimpleName() + "[" + remoteAddress + "->" + localAddress + "]";
        }
    }
}
