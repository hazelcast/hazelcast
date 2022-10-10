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

import com.hazelcast.internal.tpc.util.ProgressIndicator;

import java.io.UncheckedIOException;
import java.net.SocketAddress;
import java.util.function.Consumer;

/**
 * A server socket that is asynchronous. So accepting incoming connections does not block,
 * but are executed on an {@link Reactor}.
 */
public abstract class AsyncServerSocket extends AbstractAsyncSocket {

    protected final ProgressIndicator accepted = new ProgressIndicator();

    protected AsyncServerSocket() {
    }

    public abstract AsyncSocketOptions options();

    /**
     * Returns the number of sockets that have been accepted.
     *
     * @return the number of accepted sockets.
     */
    public long getAccepted() {
        return accepted.get();
    }

    /**
     * Gets the local address: the socket address that this channel's socket is bound to.
     *
     * @return the local address.
     */
    public final SocketAddress getLocalAddress() {
        try {
            return getLocalAddress0();
        } catch (Error e) {
            throw e;
        } catch (Exception e) {
            return null;
        }
    }

    protected abstract SocketAddress getLocalAddress0() throws Exception;

    /**
     * Gets the {@link Reactor} this ServerSocket belongs to.
     * <p/>
     * The returned value will never be <code>null</code>
     *
     * @return the Reactor.
     */
    public abstract Reactor getReactor();

    /**
     * Gets the local port of the ServerSocketChannel.
     * <p/>
     * If {@link #bind(SocketAddress)} has not been called, then -1 is returned.
     *
     * @return the local port.
     * @throws UncheckedIOException if something failed while obtaining the local port.
     */
    public abstract int getLocalPort();

    /**
     * Binds this AsyncServerSocket to the localAddress address. This method is equivalent to calling
     * {@link #bind(SocketAddress, int)} with an Integer.MAX_VALUE backlog.
     * <p/>
     * This can be made on any thread, but it isn't threadsafe.
     *
     * @param localAddress the local address.
     * @throws UncheckedIOException if something failed while binding.
     * @throws NullPointerException if localAddress is null.
     */
    public void bind(SocketAddress localAddress) {
        bind(localAddress, Integer.MAX_VALUE);
    }

    /**
     * Binds this AsyncServerSocket to the localAddress address by assigning the local address to it.
     * <p/>
     * At a socket level, this method does 2 things:
     * <ol>
     *     <li>bind: assigning an address to the socket</li>
     *     <li>listen: marks the socket as a passive socket that waits for incoming connections.
     *     Because every AsyncServerSocket is such a passive socket, there is no point in adding a
     *     listen method to the AsyncServerSocket.</li>
     * </ol>
     * This can be made on any thread, but it isn't threadsafe.
     * <p>
     * This call needs to be made before {@link #start()}.
     * <p/>
     * Bind should only be called once, otherwise an UncheckedIOException is thrown.
     *
     * @param localAddress the local address.
     * @param backlog      the maximum number of pending connections. The backlog argument
     *                     doesn't need to be respected by the socket implementation.
     * @throws UncheckedIOException     if something failed while binding.
     * @throws NullPointerException     if localAddress is null.
     * @throws IllegalArgumentException if backlog smaller than 0.
     */
    public abstract void bind(SocketAddress localAddress, int backlog);

    /**
     * Start accepting incoming sockets asynchronously.
     * <p/>
     * This method can be called from any thread, but the actual processing will happen on the
     * reactor-thread.
     * <p/>
     * This method should only be called once and isn't threadsafe.
     * <p/>
     * Before accept is called, bind needs to be called.
     *
      */
    public abstract void start();

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + getLocalAddress() + "]";
    }
}
