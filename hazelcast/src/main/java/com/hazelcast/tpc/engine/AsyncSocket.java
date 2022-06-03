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

package com.hazelcast.tpc.engine;

import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.tpc.engine.frame.Frame;

import java.io.Closeable;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;

public abstract class AsyncSocket implements Closeable {
    protected final ILogger logger = Logger.getLogger(getClass());
    protected final AtomicBoolean closed = new AtomicBoolean();

    protected volatile SocketAddress remoteAddress;
    protected volatile SocketAddress localAddress;

    protected final SwCounter framesWritten = newSwCounter();

    protected final SwCounter bytesRead = newSwCounter();

    protected final SwCounter bytesWritten = newSwCounter();

    protected final SwCounter framesRead = newSwCounter();

    protected final SwCounter handleWriteCnt = newSwCounter();

    protected final SwCounter readEvents = newSwCounter();


    public long framesWritten() {
        return framesWritten.get();
    }

    public long bytesRead() {
        return bytesRead.get();
    }

    public long bytesWritten() {
        return bytesWritten.get();
    }

    public long framesRead() {
        return framesRead.get();
    }

    public long handleWriteCnt() {
        return handleWriteCnt.get();
    }

    public long readEvents() {
        return readEvents.get();
    }

    /**
     * Returns the {@link Eventloop} this AsyncSocket belongs to.
     *
     * @return the {@link Eventloop} this AsyncSocket belongs to or null if
     * the AsyncSocket has not been activated yet.
     */
    public abstract Eventloop eventloop();

    /**
     * Returns the remote address.
     *
     * If the AsyncSocket isn't connected, null is returned.
     *
     * This method is thread-safe.
     *
     * @return the remote address.
     */
    public final SocketAddress remoteAddress() {
        return remoteAddress;
    }

    /**
     * Returns the local address.
     *
     * If the AsyncSocket isn't connected, null is returned.
     *
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
     * Activates an AsyncSocket by hooking it up to an EventLoop.
     *
     * This method is not thread-safe.
     *
     * This method should only be called once.
     *
     * @param eventloop
     * @throws NullPointerException if eventloop is null.
     * @throws IllegalStateException if the AsyncSocket is already activated.
     */
    public abstract void activate(Eventloop eventloop);

    /**
     * Ensures that any scheduled frames are flushed to the socket.
     *
     * What happens under the hood is that the AsyncSocket is scheduled in the
     * {@link Eventloop} where at some point in the future the frames get written
     * to the socket.
     *
     * This method is thread-safe.
     */
    public abstract void flush();

    /**
     * Writes a frame to the AsyncSocket without scheduling the AsyncSocket
     * in the eventloop.
     *
     * This call can be used to buffer a series of request and then call
     * {@link #flush()} to trigger the actual writting to the socket.
     *
     * This method is thread-safe.
     *
     * There is no guarantee that frame is actually going to be received by the caller if
     * the AsyncSocket has accepted the frame. E.g. when the connection closes.
     *
     * @param frame the frame to write.
     * @return true if the frame was accepted, false if there was an overload.
     */
    public abstract boolean write(Frame frame);

    public abstract boolean writeAll(Collection<Frame> frames);

    /**
     * Writes a frame and flushes it.
     *
     * This is the same as calling {@link #write(Frame)} followed by a {@link #flush()}.
     *
     * There is no guarantee that frame is actually going to be received by the caller if
     * the AsyncSocket has accepted the frame. E.g. when the connection closes.
     *
     * This method is thread-safe.
     *
     * @param frame the frame to write.
     * @return true if the frame was accepted, false if there was an overload.
     */
    public abstract boolean writeAndFlush(Frame frame);

    /**
     * Writes a frame and ensure it gets written.
     *
     * Should only be called from within the Eventloop.
     */
    public abstract boolean unsafeWriteAndFlush(Frame frame);

    /**
     * Connects asynchronously to some address.
     *
     * @param address the address to connect to.
     * @return a {@link CompletableFuture}
     */
    public abstract CompletableFuture<AsyncSocket> connect(SocketAddress address);

    /**
     * Closes this {@link AsyncSocket}.
     *
     * This method is thread-safe.
     *
     * If the AsyncSocket is already closed, the call is ignored.
     */
    public abstract void close();

    /**
     * Checks if this AsyncSocket is closed.
     *
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
