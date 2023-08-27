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
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.function.Consumer;

import static com.hazelcast.internal.tpcengine.iouring.CompletionQueue.newCQEFailedException;
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
import static com.hazelcast.internal.tpcengine.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;

/**
 * The io_uring implementation of the {@link AsyncServerSocket}.
 */
public final class UringAsyncServerSocket extends AsyncServerSocket {
    private static final Unsafe UNSAFE = UnsafeLocator.UNSAFE;

    private final LinuxSocket linuxSocket;
    private final AcceptHandler acceptHandler;
    private final Uring uring;

    private UringAsyncServerSocket(Builder builder) throws RuntimeException {
        super(builder, builder.localAddress, builder.localPort);
        this.uring = builder.uring;
        this.linuxSocket = builder.linuxSocket;
        this.acceptHandler = new AcceptHandler(builder, this);
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
    protected void start0() {
        CompletionQueue cq = uring.completionQueue();
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
            this.completionQueue = builder.uring.completionQueue();
            this.acceptFn = builder.acceptFn;
            this.metrics = builder.metrics;
            this.linuxSocket = builder.linuxSocket;
            this.submissionQueue = builder.uring.submissionQueue();
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
            UNSAFE.putInt(sqeAddr + OFFSET_SQE_rw_flags, 0);
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
                    } catch (Throwable throwable) {
                        if (logger.isWarningEnabled()) {
                            logger.warning(socket + " rejected: " + linuxSocket.getRemoteAddress()
                                    + "->" + linuxSocket.getLocalAddress()
                                    + " due to unhandled throwable.", throwable);
                        }

                        closeQuietly(acceptRequest);

                        if (!(throwable instanceof Exception)) {
                            // Anything that isn't an exception should be propaged because we can't
                            // handle it here.
                            throw sneakyThrow(throwable);
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

        public LinuxSocket linuxSocket;
        public Uring uring;
        private InetSocketAddress localAddress;
        private int localPort;

        Builder() {
            // to conclude.
            this.linuxSocket = LinuxSocket.createNonBlockingTcpIpv4Socket();
            this.options = new UringOptions(linuxSocket);
        }

        @Override
        public void close() throws Exception {
            closeQuietly(linuxSocket);
        }

        @Override
        protected void conclude() {
            super.conclude();

            checkNotNull(uring, "uring");
            checkNotNull(linuxSocket, "linuxSocket");

            if (linuxSocket.isBlocking()) {
                throw new IllegalArgumentException("The linux socket should be non blocking");
            }

            if (bindAddress != null) {
                try {
                    linuxSocket.bind(bindAddress);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            } else {
                int attempts = 0;
                for (; ; ) {
                    SocketAddress address = bindAddressGenerator.get();
                    if (address == null) {
                        throw new UncheckedIOException(
                                new BindException(
                                        "Failed to find an address to bind to after " + attempts + " attempts"));
                    }
                    attempts++;
                    try {
                        linuxSocket.bind(address);
                        break;
                    } catch (BindException e) {
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
            }

            try {
                linuxSocket.listen(backlog);
            } catch (Throwable e) {
                throw sneakyThrow(e);
            }

            this.localAddress = linuxSocket.getLocalAddress();
            this.localPort = linuxSocket.getLocalAddress().getPort();
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
