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

package com.hazelcast.internal.tpcengine.iouring;


import com.hazelcast.internal.tpcengine.net.AcceptRequest;
import com.hazelcast.internal.tpcengine.net.AsyncServerSocket;
import com.hazelcast.internal.tpcengine.net.AsyncSocketOptions;
import com.hazelcast.internal.tpcengine.util.ExceptionUtil;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static com.hazelcast.internal.tpcengine.iouring.CompletionQueue.newCompletionFailedException;
import static com.hazelcast.internal.tpcengine.iouring.IOUring.IORING_OP_ACCEPT;
import static com.hazelcast.internal.tpcengine.iouring.Linux.SOCK_CLOEXEC;
import static com.hazelcast.internal.tpcengine.iouring.Linux.SOCK_NONBLOCK;
import static com.hazelcast.internal.tpcengine.iouring.LinuxSocket.AF_INET;
import static com.hazelcast.internal.tpcengine.util.CloseUtil.closeQuietly;
import static com.hazelcast.internal.tpcengine.util.ExceptionUtil.newUncheckedIOException;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;

/**
 * The io_uring implementation of the {@link AsyncServerSocket}.
 */
@SuppressWarnings({"checkstyle:MethodName", "checkstyle:TypeName", "checkstyle:MemberName"})
public final class IOUringAsyncServerSocket extends AsyncServerSocket {

    private final LinuxSocket linuxSocket;

    private final IOUringReactor reactor;
    private final AcceptMemory acceptMemory = new AcceptMemory();
    private final IOUringEventloop eventloop;
    private final SubmissionQueue sq;
    private final Consumer<AcceptRequest> acceptConsumer;
    private final IOUringAsyncServerSocketOptions options;
    private final Thread eventloopThread;

    private long userdata_acceptHandler;
    private boolean bind;
    private boolean started;

    IOUringAsyncServerSocket(IOUringAsyncServerSocketBuilder builder) {
        this.reactor = builder.reactor;
        this.eventloop = (IOUringEventloop) reactor.eventloop();
        this.options = builder.options;
        this.linuxSocket = builder.nativeSocket;
        this.eventloopThread = reactor.eventloopThread();
        this.acceptConsumer = builder.acceptConsumer;
        this.sq = eventloop.sq;
        if (!reactor.registerCloseable(this)) {
            close();
            throw new IllegalStateException("Reactor is not running");
        }

        // todo: return value not checked.
        reactor.offer(() -> {
            // todo: on close we need to deregister
            this.userdata_acceptHandler = eventloop.nextPermanentHandlerId();
            eventloop.handlers.put(userdata_acceptHandler, new Handler_OP_ACCEPT());
        });
    }

    /**
     * Returns the underlying {@link LinuxSocket}.
     *
     * @return the {@link LinuxSocket}.
     */
    public LinuxSocket nativeSocket() {
        return linuxSocket;
    }

    @Override
    public int getLocalPort() {
        if (!bind) {
            return -1;
        } else {
            return linuxSocket.getLocalAddress().getPort();
        }
    }

    @Override
    public IOUringReactor getReactor() {
        return reactor;
    }

    @Override
    protected SocketAddress getLocalAddress0() {
        if (!bind) {
            return null;
        } else {
            return linuxSocket.getLocalAddress();
        }
    }

    @Override
    protected void close0() throws IOException {
        reactor.deregisterCloseable(this);
        linuxSocket.close();
    }

    @Override
    public void bind(SocketAddress localAddress, int backlog) {
        checkNotNull(localAddress, "localAddress");
        checkNotNegative(backlog, "backlog");

        try {
            boolean blocking = linuxSocket.isBlocking();
            linuxSocket.setBlocking(true);
            linuxSocket.bind(localAddress);
            linuxSocket.listen(backlog);
            linuxSocket.setBlocking(blocking);
            bind = true;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public AsyncSocketOptions options() {
        return options;
    }

    @Override
    public void start() {
        if (Thread.currentThread() == eventloopThread) {
            start0();
        } else {
            CompletableFuture<Void> future = new CompletableFuture<>();
            reactor.execute(() -> {
                try {
                    start0();
                    future.complete(null);
                } catch (Throwable t) {
                    future.completeExceptionally(t);
                    throw ExceptionUtil.sneakyThrow(t);
                }
            });

            future.join();
        }
    }

    private void start0() {
        if (started) {
            throw new IllegalStateException(this + " is already started");
        }
        started = true;

        if (!sq_offer_OP_ACCEPT()) {
            throw new IllegalStateException("Submission queue rejected the OP_ACCEPT");
        }
        if (logger.isInfoEnabled()) {
            logger.info("ServerSocket listening at " + getLocalAddress());
        }
    }

    private boolean sq_offer_OP_ACCEPT() {
        return sq.offer(
                IORING_OP_ACCEPT,
                0,
                SOCK_NONBLOCK | SOCK_CLOEXEC,
                linuxSocket.fd(),
                acceptMemory.memoryAddress,
                0,
                acceptMemory.lengthMemoryAddress,
                userdata_acceptHandler
        );
    }

    private class Handler_OP_ACCEPT implements IOCompletionHandler {

        @Override
        public void handle(int res, int flags, long userdata) {
            try {
                if (res < 0) {
                    throw newCompletionFailedException("Failed to accept a socket.", IORING_OP_ACCEPT, -res);
                }

                metrics.incAccepted();

                SocketAddress address = LinuxSocket.toInetSocketAddress(
                        acceptMemory.memoryAddress,
                        acceptMemory.lengthMemoryAddress);

                if (logger.isInfoEnabled()) {
                    logger.info(IOUringAsyncServerSocket.this + " new connected accepted: " + address);
                }

                // todo: ugly that AF_INET is hard configured.
                // We should use the address to determine the type
                LinuxSocket socket = new LinuxSocket(res, AF_INET);
                AcceptRequest acceptRequest = new IOUringAcceptRequest(socket);
                try {
                    acceptConsumer.accept(acceptRequest);
                } catch (Throwable t) {
                    // If for whatever reason the socket isn't consumed, we need
                    // to properly close the socket. Otherwise the socket remains
                    // under a CLOSE_WAIT state and the port doesn't get released.
                    closeQuietly(acceptRequest);
                    throw ExceptionUtil.sneakyThrow(t);
                }

                //todo: return value
            } catch (Exception e) {
                close(null, e);
            } finally {
                // we need to re-register for more accepts.
                sq_offer_OP_ACCEPT();
            }
        }
    }
}
