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


import com.hazelcast.internal.tpcengine.logging.TpcLogger;
import com.hazelcast.internal.tpcengine.net.AbstractAsyncSocket;
import com.hazelcast.internal.tpcengine.net.AsyncServerSocket;
import com.hazelcast.internal.tpcengine.net.AsyncSocket;
import com.hazelcast.internal.tpcengine.util.Option;
import com.hazelcast.internal.tpcengine.util.UnsafeLocator;
import sun.misc.Unsafe;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.SocketAddress;
import java.util.function.Consumer;

import static com.hazelcast.internal.tpcengine.iouring.CompletionQueue.newCQEFailedException;
import static com.hazelcast.internal.tpcengine.iouring.Linux.SOCK_CLOEXEC;
import static com.hazelcast.internal.tpcengine.iouring.Linux.SOCK_NONBLOCK;
import static com.hazelcast.internal.tpcengine.iouring.LinuxSocket.AF_INET;
import static com.hazelcast.internal.tpcengine.iouring.SubmissionQueue.OFFSET_SQE_addr;
import static com.hazelcast.internal.tpcengine.iouring.SubmissionQueue.OFFSET_SQE_fd;
import static com.hazelcast.internal.tpcengine.iouring.SubmissionQueue.OFFSET_SQE_flags;
import static com.hazelcast.internal.tpcengine.iouring.SubmissionQueue.OFFSET_SQE_ioprio;
import static com.hazelcast.internal.tpcengine.iouring.SubmissionQueue.OFFSET_SQE_len;
import static com.hazelcast.internal.tpcengine.iouring.SubmissionQueue.OFFSET_SQE_off;
import static com.hazelcast.internal.tpcengine.iouring.SubmissionQueue.OFFSET_SQE_opcode;
import static com.hazelcast.internal.tpcengine.iouring.SubmissionQueue.OFFSET_SQE_rw_flags;
import static com.hazelcast.internal.tpcengine.iouring.SubmissionQueue.OFFSET_SQE_user_data;
import static com.hazelcast.internal.tpcengine.iouring.SubmissionQueue.SIZEOF_SQE;
import static com.hazelcast.internal.tpcengine.iouring.Uring.IORING_OP_ACCEPT;
import static com.hazelcast.internal.tpcengine.util.CloseUtil.closeQuietly;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;

/**
 * The io_uring implementation of the {@link AsyncServerSocket}.
 */
public final class UringAsyncServerSocket extends AsyncServerSocket {
    private static final Unsafe UNSAFE = UnsafeLocator.UNSAFE;

    private final LinuxSocket linuxSocket;
    private final AcceptHandler acceptHandler;
    private final Uring uring;
    private boolean bind;

    private UringAsyncServerSocket(Builder builder) {
        super(builder);
        this.uring = builder.uring;
        this.linuxSocket = builder.linuxSocket;
        this.acceptHandler = new AcceptHandler(builder, this);
    }

    @Override
    public int getLocalPort() {
        if (bind) {
            return linuxSocket.getLocalAddress().getPort();
        } else {
            return -1;
        }
    }

    @Override
    protected SocketAddress getLocalAddress0() {
        if (bind) {
            return linuxSocket.getLocalAddress();
        } else {
            return null;
        }
    }

    @Override
    protected void close0() throws IOException {
        super.close0();
        linuxSocket.close();

        reactor.offer(() -> {
            acceptHandler.closed = true;
        });
    }

    @Override
    public void bind(SocketAddress localAddress, int backlog) {
        checkNotNull(localAddress, "localAddress");
        checkNotNegative(backlog, "backlog");

        try {
            linuxSocket.bind(localAddress);
            linuxSocket.listen(backlog);
            bind = true;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected void start0() {
        CompletionQueue cq = uring.cq();
        acceptHandler.handlerId = cq.nextHandlerId();
        cq.register(acceptHandler.handlerId, acceptHandler);
        acceptHandler.prepareSqe();
    }

    private static final class AcceptHandler implements CompletionHandler {

        private final UringAsyncServerSocket socket;
        private final SubmissionQueue submissionQueue;
        private final LinuxSocket linuxSocket;
        private final AcceptMemory acceptMemory = new AcceptMemory();
        private final Metrics metrics;
        private final TpcLogger logger;
        private final Consumer<AbstractAsyncSocket.AcceptRequest> acceptFn;
        private final CompletionQueue completionQueue;
        private int handlerId;
        private boolean closed;

        private AcceptHandler(Builder builder, UringAsyncServerSocket socket) {
            this.socket = socket;
            this.completionQueue = builder.uring.cq();
            this.acceptFn = builder.acceptFn;
            this.metrics = builder.metrics;
            this.linuxSocket = builder.linuxSocket;
            this.submissionQueue = builder.uring.sq();
            this.logger = builder.logger;
        }

        private void prepareSqe() {
            int sqeIndex = submissionQueue.nextIndex();
            if (sqeIndex < 0) {
                throw new IllegalStateException("No space in submission queue");
            }

            long sqeAddr = submissionQueue.sqesAddr + sqeIndex * SIZEOF_SQE;
            UNSAFE.putByte(sqeAddr + OFFSET_SQE_opcode, IORING_OP_ACCEPT);
            UNSAFE.putByte(sqeAddr + OFFSET_SQE_flags, (byte) 0);
            UNSAFE.putShort(sqeAddr + OFFSET_SQE_ioprio, (short) 0);
            UNSAFE.putInt(sqeAddr + OFFSET_SQE_fd, linuxSocket.fd());
            UNSAFE.putLong(sqeAddr + OFFSET_SQE_off, acceptMemory.lenAddr);
            UNSAFE.putLong(sqeAddr + OFFSET_SQE_addr, acceptMemory.addr);
            UNSAFE.putInt(sqeAddr + OFFSET_SQE_len, 0);
            UNSAFE.putInt(sqeAddr + OFFSET_SQE_rw_flags, SOCK_NONBLOCK | SOCK_CLOEXEC);
            UNSAFE.putLong(sqeAddr + OFFSET_SQE_user_data, handlerId);
        }

        @Override
        public void completeRequest(int res, int flags, long userdata) {
            try {
                if (res >= 0) {
                    int fd = res;
                    metrics.incAccepted();

                    // re-register for more accepts.
                    prepareSqe();

                    SocketAddress address = LinuxSocket.toInetSocketAddress(
                            acceptMemory.addr,
                            acceptMemory.lenAddr);

                    if (logger.isInfoEnabled()) {
                        logger.info(socket + " new connected accepted: " + address);
                    }

                    // todo: ugly that AF_INET is hard configured.
                    // We should use the address to determine the type
                    LinuxSocket linuxSocket = new LinuxSocket(fd, AF_INET);
                    AbstractAsyncSocket.AcceptRequest acceptRequest = new AcceptRequest(linuxSocket);
                    try {
                        acceptFn.accept(acceptRequest);
                    } catch (Throwable t) {

                        // todo: logging mismatch between this and NioAsyncServersocket.
                        logger.severe(t);

                        // If for whatever reason the socket isn't consumed, we need
                        // to properly close the socket. Otherwise the socket remains
                        // under a CLOSE_WAIT state and the port doesn't get released.
                        closeQuietly(acceptRequest);

                        if (logger.isWarningEnabled()) {
                            logger.warning("Closing socket " + address + "->" + socket.getLocalAddress());
                        }
                    }
                } else {
                    throw newCQEFailedException("Failed to accept a socket.", "accept(2)", IORING_OP_ACCEPT, -res);
                }
            } catch (Exception e) {
                // todo: do we want to close the server socket on error of a handler??
                socket.close(null, e);
            } finally {
                if (closed) {
                    completionQueue.unregister((int) userdata);
                }
            }
        }
    }

    @SuppressWarnings("checkstyle:SimplifyBooleanReturn")
    public static class UringOptions implements AsyncSocket.Options {

        private final LinuxSocket nativeSocket;

        UringOptions(LinuxSocket nativeSocket) {
            this.nativeSocket = nativeSocket;
        }

        @Override
        public boolean isSupported(Option option) {
            checkNotNull(option, "option");

            if (AsyncSocket.Options.SO_RCVBUF.equals(option)) {
                return true;
            } else if (AsyncSocket.Options.SO_REUSEADDR.equals(option)) {
                return true;
            } else if (AsyncSocket.Options.SO_REUSEPORT.equals(option)) {
                return true;
            } else {
                return false;
            }
        }

        @Override
        public <T> boolean set(Option<T> option, T value) {
            checkNotNull(option, "option");
            checkNotNull(value, "value");

            try {
                if (AsyncSocket.Options.SO_RCVBUF.equals(option)) {
                    nativeSocket.setReceiveBufferSize((Integer) value);
                    return true;
                } else if (AsyncSocket.Options.SO_REUSEADDR.equals(option)) {
                    nativeSocket.setReuseAddress((Boolean) value);
                    return true;
                } else if (AsyncSocket.Options.SO_REUSEPORT.equals(option)) {
                    nativeSocket.setReusePort((Boolean) value);
                    return true;
                } else {
                    return false;
                }
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to set " + option.name() + " with value [" + value + "]", e);
            }
        }

        @Override
        public <T> T get(Option<T> option) {
            checkNotNull(option, "option");

            try {
                if (AsyncSocket.Options.SO_RCVBUF.equals(option)) {
                    return (T) (Integer) nativeSocket.getReceiveBufferSize();
                } else if (AsyncSocket.Options.SO_REUSEADDR.equals(option)) {
                    return (T) (Boolean) nativeSocket.isReuseAddress();
                } else if (AsyncSocket.Options.SO_REUSEPORT.equals(option)) {
                    return (T) (Boolean) nativeSocket.isReusePort();
                } else {
                    return null;
                }
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to get option " + option.name(), e);
            }
        }
    }

    /**
     * An {@link UringAsyncServerSocket} builder.
     */
    @SuppressWarnings({"checkstyle:VisibilityModifier"})
    public static class Builder extends AsyncServerSocket.Builder {

        public final LinuxSocket linuxSocket;
        public Uring uring;

        Builder() {
            // to conclude.
            this.linuxSocket = LinuxSocket.createNonBlockingTcpIpv4Socket();
            this.options = new UringOptions(linuxSocket);
        }

        @Override
        protected void conclude() {
            super.conclude();

            checkNotNull(uring, "uring");
            checkNotNull(linuxSocket, "linuxSocket");

            if (linuxSocket.isBlocking()) {
                throw new IllegalArgumentException("The linux socket should be non blocking");
            }
        }

        @Override
        public AsyncServerSocket construct() {
            return new UringAsyncServerSocket(this);
        }
    }

    public static class AcceptRequest implements AbstractAsyncSocket.AcceptRequest {

        final LinuxSocket linuxSocket;

        public AcceptRequest(LinuxSocket linuxSocket) {
            this.linuxSocket = linuxSocket;
        }

        @Override
        public void close() throws Exception {
            linuxSocket.close();
        }
    }
}
