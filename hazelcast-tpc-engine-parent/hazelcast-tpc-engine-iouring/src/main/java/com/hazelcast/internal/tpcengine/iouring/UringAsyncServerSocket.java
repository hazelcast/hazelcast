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

import static com.hazelcast.internal.tpcengine.iouring.CompletionQueue.TYPE_SERVER_SOCKET;
import static com.hazelcast.internal.tpcengine.iouring.CompletionQueue.encodeUserdata;
import static com.hazelcast.internal.tpcengine.iouring.CompletionQueue.newCQEFailedException;
import static com.hazelcast.internal.tpcengine.iouring.Linux.EINVAL;
import static com.hazelcast.internal.tpcengine.iouring.Linux.strerror;
import static com.hazelcast.internal.tpcengine.iouring.LinuxSocket.AF_INET;
import static com.hazelcast.internal.tpcengine.iouring.Uring.IORING_OP_ACCEPT;
import static com.hazelcast.internal.tpcengine.iouring.Uring.IORING_OP_CLOSE;
import static com.hazelcast.internal.tpcengine.iouring.Uring.IORING_OP_SHUTDOWN;
import static com.hazelcast.internal.tpcengine.iouring.Uring.opcodeToString;
import static com.hazelcast.internal.tpcengine.util.CloseUtil.closeQuietly;
import static com.hazelcast.internal.tpcengine.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;

/**
 * The io_uring implementation of the {@link AsyncServerSocket}.
 */
public final class UringAsyncServerSocket extends AsyncServerSocket {
    private static final Unsafe UNSAFE = UnsafeLocator.UNSAFE;

    private final LinuxSocket linuxSocket;
    private final Handler handler;
    private final Uring uring;
    private final UringNetworkScheduler networkScheduler;

    private UringAsyncServerSocket(Builder builder) throws RuntimeException {
        super(builder, builder.localAddress, builder.localPort);
        this.uring = builder.uring;
        this.linuxSocket = builder.linuxSocket;
        this.networkScheduler = builder.networkScheduler;
        this.handler = new Handler(builder, this);
    }

    @Override
    protected void close0() throws IOException {
        if (Thread.currentThread() == eventloopThread) {
            close00();
        } else {
            // todo: handle return
            reactor.offer(() -> {
                close00();
            });
        }
    }

    // Guaranteed to run from the eventloop thread.
    private void close00() {
        if (started) {
            handler.prepareShutdown();
        } else {
            linuxSocket.close();
        }
    }

    @Override
    protected void start00() {
        networkScheduler.register(handler);
        handler.prepareAccept();
    }

    @SuppressWarnings({"checkstyle:MemberName"})
    static final class Handler {

        int handlerIndex;
        private final UringAsyncServerSocket socket;
        private final LinuxSocket linuxSocket;
        private final SubmissionQueue submissionQueue;
        private final CompletionQueue completionQueue;
        private final AcceptMemory acceptMemory = new AcceptMemory();
        private final Metrics metrics;
        private final TpcLogger logger;
        private final Consumer<AbstractAsyncSocket.AcceptRequest> acceptFn;
        private boolean closing;
        private int pending;

        private Handler(Builder builder, UringAsyncServerSocket socket) {
            this.socket = socket;
            this.completionQueue = builder.uring.completionQueue();
            this.acceptFn = builder.acceptFn;
            this.metrics = builder.metrics;
            this.linuxSocket = builder.linuxSocket;
            this.submissionQueue = builder.uring.submissionQueue();
            this.logger = builder.logger;
        }

        private void prepareAccept() {
            try {
                if (closing) {
                    return;
                }

                long userdata = encodeUserdata(TYPE_SERVER_SOCKET, IORING_OP_ACCEPT, handlerIndex);
                submissionQueue.prepareAccept(
                        linuxSocket.fd(), acceptMemory.addr, acceptMemory.lenAddr, userdata);
                pending++;
            } catch (Exception e) {
                socket.close("Failed to prepare accept.", e);
            }
        }

        private void completeAccept(int res) {
            if (res >= 0) {
                int fd = res;
                metrics.incAccepted();

                prepareAccept();

                SocketAddress address = LinuxSocket.toInetSocketAddress(
                        acceptMemory.addr,
                        acceptMemory.lenAddr);

                if (logger.isInfoEnabled()) {
                    logger.info(socket + " new connected accepted: " + address);
                }

                // todo: ugly that AF_INET is hard configured.
                // We should use the address to determine the type
                LinuxSocket linuxSocket = new LinuxSocket(fd, AF_INET);
                linuxSocket.setBlocking(false);

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
            } else if (res == -EINVAL) {
                // when the socket is closing, a shutdown is send first to cancel the
                // accept operation and and we get an EINVAL. Otherwise we need to
                // propagate the error because there must be something wrong with the
                // accept request.

                if (!closing) {
                    throw newCQEFailedException(
                            "Failed to accept a socket.", "accept(2)", IORING_OP_ACCEPT, -res);
                }
            } else {
                throw newCQEFailedException(
                        "Failed to accept a socket.", "accept(2)", IORING_OP_ACCEPT, -res);
            }
        }

        private void prepareShutdown() {
            try {
                if (closing) {
                    return;
                }
                closing = true;

                long userdata = encodeUserdata(TYPE_SERVER_SOCKET, IORING_OP_SHUTDOWN, handlerIndex);
                submissionQueue.prepareShutdown(linuxSocket.fd(), userdata);
                pending++;
            } catch (Exception e) {
                socket.close("Failed to prepare shutdown", e);
            }
        }

        private void completeShutdown(int res) {
            if (res != 0) {
                System.out.println("Shutdown of socket failed with " + strerror(-res));
                //todo: EAGAIN and all that
            }

            // the shutdown completed, now we go in close the socket.
            prepareClose();
        }

        private void prepareClose() {
            try {
                long userdata = encodeUserdata(TYPE_SERVER_SOCKET, IORING_OP_CLOSE, handlerIndex);
                submissionQueue.prepareClose(linuxSocket.fd(), userdata);
                pending++;
            } catch (Exception e) {
                socket.close("Failed to prepare close", e);
            }
        }

        private void completeClose(int res) {
            if (res != 0) {
                System.out.println("Closing of socket failed with " + strerror(-res));
                //todo: EAGAIN and all that
            }
            // we don't need to do anything here. The last pending
            // request that returns will trigger the actual close
        }

        public void complete(byte opcode, int res) {
            try {
                //System.out.println(socket + " complete :" + opcodeToString(opcode) + " res:" + res);
                switch (opcode) {
                    case IORING_OP_ACCEPT:
                        completeAccept(res);
                        break;
                    case IORING_OP_SHUTDOWN:
                        completeShutdown(res);
                        break;
                    case IORING_OP_CLOSE:
                        completeClose(res);
                        break;
                    default:
                        throw new IllegalArgumentException("Unhandled opcode:" + opcodeToString(opcode));
                }

                pending--;

                if (closing && pending == 0) {
                    // if the socket is closing and this is the last request to
                    // complete, we can do deregister the resources.
                    socket.networkScheduler.unregister(this);
                    socket.reactor.serverSockets().remove(socket);
                }
            } catch (Exception e) {
                socket.close(null, e);
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
        public UringNetworkScheduler networkScheduler;

        private InetSocketAddress localAddress;
        private int localPort;

        public Builder() {
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
            checkNotNull(networkScheduler, "networkScheduler");

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
                                        "Failed to find an address to bind to after " + attempts + " attempts."));
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
