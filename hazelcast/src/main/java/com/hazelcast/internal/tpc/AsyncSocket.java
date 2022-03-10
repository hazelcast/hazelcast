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
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;

/**
 * A Socket that is asynchronous. So reads and writes do not block, but are executed
 * on an {@link Eventloop}.
 */
public abstract class AsyncSocket implements Closeable {

    public final ConcurrentMap context = new ConcurrentHashMap();

    protected final ILogger logger = Logger.getLogger(getClass());
    protected final AtomicBoolean closed = new AtomicBoolean();

    protected volatile SocketAddress remoteAddress;
    protected volatile SocketAddress localAddress;
    private CloseListener closeListener;
    private Executor closeExecutor;

    protected final SwCounter ioBuffersWritten = newSwCounter();

    protected final SwCounter bytesRead = newSwCounter();

    protected final SwCounter bytesWritten = newSwCounter();

    protected final SwCounter ioBuffersRead = newSwCounter();

    protected final SwCounter handleWriteCnt = newSwCounter();

    protected final SwCounter readEvents = newSwCounter();

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
    public final SocketAddress remoteAddress() {
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

    public abstract void readHandler(ReadHandler readHandler);

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
        this.closeListener = checkNotNull(listener, "listener can't be null");
        this.closeExecutor = checkNotNull(executor, "executor can't be null");
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
            doClose();
        } catch (Exception e) {
            logger.warning(e);
        }

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
     * Takes care of the actual closing.
     */
    protected abstract void doClose() throws IOException;

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
        return getClass().getSimpleName() + "[" + localAddress + "->" + remoteAddress + "]";
    }
}
