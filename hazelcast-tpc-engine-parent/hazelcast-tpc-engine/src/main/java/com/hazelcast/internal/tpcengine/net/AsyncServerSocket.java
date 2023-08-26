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
import com.hazelcast.internal.tpcengine.logging.TpcLoggerLocator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.net.SocketAddress;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.hazelcast.internal.tpcengine.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;

/**
 * A server socket that is asynchronous. So accepting incoming connections does
 * not block, but are executed on a {@link Reactor}.
 */
public abstract class AsyncServerSocket extends AbstractAsyncSocket {

    protected final Metrics metrics;
    protected final Reactor reactor;
    protected final Thread eventloopThread;
    protected final AsyncSocket.Options options;
    protected final SocketAddress localAddress;
    protected final int localPort;
    protected boolean started;

    protected AsyncServerSocket(Builder builder, SocketAddress localAddress, int localPort) {
        super(builder);

        this.options = builder.options;
        this.metrics = builder.metrics;
        this.reactor = builder.reactor;
        this.localAddress = localAddress;
        this.localPort = localPort;
        this.eventloopThread = reactor.eventloopThread();
    }

    /**
     * Returns the {@link Metrics} of this AsyncServerSocket.
     * <p/>
     * This call can always be made no matter the state of the socket.
     *
     * @return the metrics.
     */
    public Metrics metrics() {
        return metrics;
    }

    /**
     * Gets the {@link AsyncSocket.Options} for this AsyncServerSocket.
     * <p/>
     * This call can always be made no matter the state of the socket.
     *
     * @return the options.
     */
    public final AsyncSocket.Options options() {
        return options;
    }

    /**
     * Gets the local address: the socket address that this channel's socket
     * is bound to.
     * <p/>
     * This method is threadsafe.
     * <p/>
     * This method will always return a not null value and will always be
     * exactly the same cached instance (so is very cheap).
     * <p/>
     * The value returned isn't impacted by closing of the socket.
     *
     * @return the local address.
     */
    public final SocketAddress getLocalAddress() {
        return localAddress;
    }

    @Override
    protected void close0() throws IOException {
        reactor.serverSockets().remove(this);
    }

    /**
     * Gets the local port of the {@link AsyncServerSocket}.
     * <p/>
     * The value returned isn't impacted by closing of the socket.
     *
     * @return the local port.
     * @throws UncheckedIOException if something failed while obtaining the
     *                              local port.
     */
    public final int getLocalPort() {
        return localPort;
    }


    /**
     * Gets the {@link Reactor} this ServerSocket belongs to.
     * <p/>
     * This can be made on any thread, but it isn't threadsafe.
     *
     * @return the Reactor.
     */
    public final Reactor getReactor() {
        return reactor;
    }

    /**
     * Start accepting incoming sockets asynchronously.
     * <p/>
     * This method can be called from any thread, but the actual processing
     * will happen on the eventloop-thread.
     * <p/>
     * This method should only be called once and isn't threadsafe.
     * <p/>
     * Before accept is called, {@link #bind(SocketAddress, int)} needs to be
     * called.
     *
     * @throws IllegalStateException if there are too many server sockets on
     *                               the reactor.
     */
    public final void start() {
        try {
            if (Thread.currentThread() == eventloopThread) {
                start0();
            } else {
                reactor.submit(this::start0).join();
            }
        } catch (Throwable t) {
            close("Problems during socket start", t);
            sneakyThrow(t);
        }
    }

    private void start0() {
        if (started) {
            throw new IllegalStateException(this + " is already started");
        }

        if (!reactor.serverSockets().add(this)) {
            String msg = "Exceeded the maximum number of server sockets on reactor [" + reactor + "]";
            close(msg, null);
            throw new IllegalStateException(msg);
        }

        started = true;

        start00();

        if (logger.isInfoEnabled()) {
            logger.info("ServerSocket listening at " + getLocalAddress());
        }
    }

    /**
     * Starts the actual server socket. Call is guaranteed to be made once and
     * always from the eventloop thread.
     */
    protected abstract void start00();

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + getLocalAddress() + "]";
    }

    /**
     * Contains the metrics for an {@link AsyncServerSocket}.
     * <p/>
     * The metrics should only be updated by the event loop thread, but can be read
     * by any thread.
     */
    @SuppressWarnings("checkstyle:ConstantName")
    public static final class Metrics {

        private static final VarHandle ACCEPTED;

        static {
            try {
                MethodHandles.Lookup l = MethodHandles.lookup();
                ACCEPTED = l.findVarHandle(Metrics.class, "accepted", long.class);
            } catch (ReflectiveOperationException e) {
                throw new ExceptionInInitializerError(e);
            }
        }

        private volatile long accepted;

        /**
         * Returns the number of accepted sockets.
         *
         * @return the number of accepted sockets.
         */
        public long accepted() {
            return (long) ACCEPTED.getOpaque(this);
        }

        /**
         * Increases the number of accepted sockets by 1.
         */
        public void incAccepted() {
            ACCEPTED.setOpaque(this, (long) ACCEPTED.getOpaque(this) + 1);
        }
    }

    /**
     * The Builder for an {@link AsyncServerSocket} instance.
     * <p/>
     * This Builder assumes TCP/IPv4. For different types of sockets new
     * configuration options on this Context need to be added.
     * <p/>
     * Cast to specific Builder for specialized options when available.
     */
    @SuppressWarnings({"checkstyle:VisibilityModifier"})
    public abstract static class Builder extends AbstractAsyncSocket.Builder<AsyncServerSocket> {

        /**
         * Sets the accept function to process accept requests.
         */
        public Consumer<AcceptRequest> acceptFn;

        /**
         * The Reactor the AsyncServerSocket belongs to.
         */
        public Reactor reactor;

        /**
         * The Options for this AsyncServerSocket.
         */
        public AsyncSocket.Options options;

        /**
         * The metrics of the AsyncSocket.
         */
        public Metrics metrics;

        /**
         * The backlog defines the maximum length to which the queue of pending
         * connections for socket may grow.
         * <p/>
         * Semantics are implementation specific. An implementation can impose
         * a maximum or ignore it completely.
         * <p/>
         * A value of 0 means that some kind of default will be used.
         * <p/>
         * https://man7.org/linux/man-pages/man2/listen.2.html
         */
        public int backlog;

        /**
         * The address to bind to. bindAddress should be set or
         */
        public SocketAddress bindAddress;

        /**
         * Provides a function that generates address to bind to. Useful if
         * you want to bind to a specific set of ports. As soon as null is
         * returned, no further attemps are made and the construction
         * will fail.
         */
        public Supplier<SocketAddress> bindAddressGenerator;

        @Override
        protected void conclude() {
            if (logger == null) {
                logger = TpcLoggerLocator.getLogger(AsyncServerSocket.class);
            }

            super.conclude();

            checkNotNegative(backlog, "backlog");
            checkNotNull(reactor, "reactor");
            checkNotNull(acceptFn, "acceptFn");
            checkNotNull(options, "options");

            if (bindAddress != null && bindAddressGenerator != null) {
                throw new IllegalArgumentException(
                        "bindAddress and bindAddressGenerator can't both be set");
            }

            if (bindAddress == null && bindAddressGenerator == null) {
                throw new IllegalArgumentException(
                        "Either bindAddress or bindAddressGenerator should be set.");
            }

            if (metrics == null) {
                metrics = new Metrics();
            }
        }
    }
}
