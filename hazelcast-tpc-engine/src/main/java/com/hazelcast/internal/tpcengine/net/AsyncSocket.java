/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpcengine.net;

import com.hazelcast.internal.tpcengine.Reactor;
import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * A Socket that is asynchronous. So reads and writes do not block,
 * but are executed on an {@link Reactor}.
 */
@SuppressWarnings({"checkstyle:MethodCount", "checkstyle:VisibilityModifier"})
public abstract class AsyncSocket extends AbstractAsyncSocket {

    protected volatile SocketAddress remoteAddress;
    protected volatile SocketAddress localAddress;
    protected AsyncSocketMetrics metrics = new AsyncSocketMetrics();
    protected final boolean clientSide;

    protected AsyncSocket(boolean clientSide) {
        this.clientSide = clientSide;
    }

    /**
     * Return the {@link AsyncSocketMetrics} of this AsyncSocket.
     * <p/>
     * This call can always be made no matter the state of the socket.
     *
     * @return the metrics.
     */
    public final AsyncSocketMetrics metrics() {
        return metrics;
    }

    /**
     * Gets the {@link Reactor} this {@link AsyncSocket} belongs to.
     *
     * @return the {@link Reactor} this AsyncSocket belongs.
     */
    public abstract Reactor reactor();

    /**
     * Returns the {@link AsyncSocketOptions} of this AsyncSocket.
     *
     * @return the AsyncSocketOptions.
     */
    public abstract AsyncSocketOptions options();

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
     * Configures if this AsyncSocket is readable or not. If there is no change in the
     * readable status, the call is ignored.
     * <p/>
     * When an AsyncSocket is readable, it will schedule itself at the Reactor as soon as
     * data is received at the receive buffer of the socket, so that the received data gets
     * processed. When it isn't readable, data might be received at the receive buffer, but
     * the socket will not schedule itself.
     * <p/>
     * This functionality can be used to apply backpressure. So what happens is that the receive
     * buffer fills up. Once it fills up and the other side keeps sending data, the remote send
     * buffer fills up as well and the pressure get propagated upstream.
     * <p/>
     * This call can safely be made from any thread, but typically you want to call it from the
     * eventloop-thread. This call is blocking; this isn't an issue for the eventloop thread
     * because it is an instantaneous call. For any other thread this call is not cheap.
     *
     * @param readable the new readable status.
     * @throws RuntimeException if the readable status could not be set.
     */
    public abstract void setReadable(boolean readable);

    /**
     * Checks if this AsyncSocket is readable. For more information see {@link #setReadable(boolean)}.
     * <p/>
     * This call can safely be made from any thread, but typically you want to call it from the
     * eventloop-thread. This call is blocking; this isn't an issue for the eventloop thread
     * because it is an instantaneous call. For any other thread this call is not cheap.
     *
     * @return true if readable, false otherwise.
     * @throws RuntimeException if the readable status could not be retrieved.
     */
    public abstract boolean isReadable();

    /**
     * Start the AsyncSocket by scheduling it on the reactor. The Socket should be
     * started only once.
     * <p/>
     * Typically you do not want to share this AsyncSocket with other threads till this
     * method is called.
     *
     * @throws RuntimeException if the Socket could not be started.
     */
    public abstract void start();

    /**
     * Ensures that any scheduled IOBuffers are flushed to the socket.
     * <p>
     * What happens under the hood is that the AsyncSocket is scheduled in the
     * {@link Reactor} where at some point in the future the IOBuffers get written
     * to the socket.
     * <p>
     * This method is thread-safe.
     * <p>
     * This call is ignored when then AsyncSocket is already closed.
     */
    public abstract void flush();

    /**
     * Writes a {@link IOBuffer} to this AsyncSocket without scheduling the AsyncSocket
     * in the {@link Reactor}.
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
     * Writes a {@link IOBuffer} to this AsyncSocket and flushes it. Flushing causes the AsyncSocket
     * to be scheduled in the {@link Reactor}.
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
     * Writes an {@link IOBuffer} and ensure it gets written.
     * <p>
     * Should only be called from the reactor-thread.
     */
    public abstract boolean unsafeWriteAndFlush(IOBuffer buf);

    /**
     * Connects asynchronously to some address.
     * <p/>
     * This method is not thread-safe.
     * <p/>
     * This method should be called after {@link #start()}.
     *
     * @param address the address to connect to.
     * @return a {@link CompletableFuture}
     * @throws NullPointerException if address is null
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
        //if (clientSide) {
        //    return getClass().getSimpleName() + "[" + localAddress + "->" + remoteAddress + "]";
        //} else {
        //    return "               " + getClass().getSimpleName() + "[" + localAddress + "<-" + remoteAddress + "]";
        //}
    }
}
