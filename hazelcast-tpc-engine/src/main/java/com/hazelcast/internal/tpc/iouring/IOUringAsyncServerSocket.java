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

package com.hazelcast.internal.tpc.iouring;

import com.hazelcast.internal.tpc.AcceptRequest;
import com.hazelcast.internal.tpc.AsyncServerSocket;
import com.hazelcast.internal.tpc.AsyncSocketOptions;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static com.hazelcast.internal.tpc.iouring.IOUring.IORING_OP_ACCEPT;
import static com.hazelcast.internal.tpc.iouring.Linux.SOCK_CLOEXEC;
import static com.hazelcast.internal.tpc.iouring.Linux.SOCK_NONBLOCK;
import static com.hazelcast.internal.tpc.iouring.Linux.strerror;
import static com.hazelcast.internal.tpc.iouring.NativeSocket.AF_INET;
import static com.hazelcast.internal.tpc.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.internal.tpc.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.tpc.util.Preconditions.checkNotNull;

/**
 * The io_uring implementation of the {@link AsyncServerSocket}.
 */
public final class IOUringAsyncServerSocket extends AsyncServerSocket {

    private final NativeSocket nativeSocket;

    private final IOUringReactor reactor;
    private final AcceptMemory acceptMemory = new AcceptMemory();
    private final IOUringEventloop eventloop;
    private final SubmissionQueue sq;
    private final Consumer<AcceptRequest> acceptRequestConsumer;
    private final IOUringAsyncServerSocketOptions options;
    private final Thread eventloopThread;

    private long userdata_acceptHandler;
    private boolean bind = false;
    private boolean started;

    IOUringAsyncServerSocket(IOUringAsyncServerSocketBuilder builder) {
        this.reactor = builder.reactor;
        this.eventloop = (IOUringEventloop) reactor.eventloop();
        this.options = builder.options;
        this.nativeSocket = builder.nativeSocket;
        this.eventloopThread = reactor.eventloopThread();
        this.acceptRequestConsumer = builder.acceptConsumer;
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
     * Returns the underlying {@link NativeSocket}.
     *
     * @return the {@link NativeSocket}.
     */
    public NativeSocket nativeSocket() {
        return nativeSocket;
    }

    @Override
    public int getLocalPort() {
        if (!bind) {
            return -1;
        } else {
            return nativeSocket.getLocalAddress().getPort();
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
            return nativeSocket.getLocalAddress();
        }
    }

    @Override
    protected void close0() throws IOException {
        reactor.deregisterCloseable(this);
        nativeSocket.close();
    }

    @Override
    public void bind(SocketAddress localAddress, int backlog) {
        checkNotNull(localAddress, "localAddress");
        checkNotNegative(backlog, "backlog");

        try {
            boolean blocking = nativeSocket.isBlocking();
            nativeSocket.setBlocking(true);
            nativeSocket.bind(localAddress);
            nativeSocket.listen(backlog);
            nativeSocket.setBlocking(blocking);
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
                    throw sneakyThrow(t);
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
                nativeSocket.fd(),
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
                    throw new UncheckedIOException(new IOException(strerror(-res)));
                }

                SocketAddress address = NativeSocket.toInetSocketAddress(acceptMemory.memoryAddress, acceptMemory.lengthMemoryAddress);

                if (logger.isInfoEnabled()) {
                    logger.info(IOUringAsyncServerSocket.this + " new connected accepted: " + address);
                }

                // todo: ugly that AF_INET is hard configured.
                // We should use the address to determine the type
                NativeSocket socket = new NativeSocket(res, AF_INET);
                AcceptRequest acceptRequest = new IOUringAcceptRequest(socket);
                acceptRequestConsumer.accept(acceptRequest);

                // we need to reregister for more accepts.
                sq_offer_OP_ACCEPT();
                //todo: return value
            } catch (Exception e) {
                close("Closing IOUringAsyncServerSocket due to exception", e);
            }
        }
    }

}
