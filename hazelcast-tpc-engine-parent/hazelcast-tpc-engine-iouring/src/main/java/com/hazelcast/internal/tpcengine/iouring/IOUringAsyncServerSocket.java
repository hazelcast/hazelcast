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


import com.hazelcast.internal.tpcengine.Option;
import com.hazelcast.internal.tpcengine.net.AsyncServerSocket;
import com.hazelcast.internal.tpcengine.net.AsyncSocket;
import com.hazelcast.internal.tpcengine.util.UnsafeLocator;
import sun.misc.Unsafe;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.SocketAddress;

import static com.hazelcast.internal.tpcengine.iouring.CompletionQueue.newCQEFailedException;
import static com.hazelcast.internal.tpcengine.iouring.IOUring.IORING_OP_ACCEPT;
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
import static com.hazelcast.internal.tpcengine.util.CloseUtil.closeQuietly;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;

/**
 * The io_uring implementation of the {@link AsyncServerSocket}.
 */
@SuppressWarnings({"checkstyle:MethodName", "checkstyle:TypeName", "checkstyle:MemberName"})
public final class IOUringAsyncServerSocket extends AsyncServerSocket {
    private static final Unsafe UNSAFE = UnsafeLocator.UNSAFE;

    private final LinuxSocket linuxSocket;
    private final AcceptMemory acceptMemory = new AcceptMemory();
    private final IOUringEventloop eventloop;
    private final SubmissionQueue sq;
    private long userdata_OP_ACCEPT;
    private boolean bind;

    private IOUringAsyncServerSocket(Builder builder) {
        super(builder);
        this.eventloop = (IOUringEventloop) reactor.eventloop();
        this.linuxSocket = builder.nativeSocket;
        this.sq = eventloop.sq;

        // todo: return value not checked.
        reactor.offer(() -> {
            // todo: on close we need to deregister
            this.userdata_OP_ACCEPT = eventloop.nextPermanentHandlerId();
            eventloop.handlers.put(userdata_OP_ACCEPT, new Handler_OP_ACCEPT());
        });
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
    protected SocketAddress getLocalAddress0() {
        if (!bind) {
            return null;
        } else {
            return linuxSocket.getLocalAddress();
        }
    }

    @Override
    protected void close0() throws IOException {
        super.close0();
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
    protected void start0() {
        sq_add_OP_ACCEPT();
    }

    private void sq_add_OP_ACCEPT() {
        int index = sq.nextIndex();
        if (index < 0) {
            throw new RuntimeException("No space in submission queue");
        }

        long sqeAddr = sq.sqesAddr + index * SIZEOF_SQE;
        UNSAFE.putByte(sqeAddr + OFFSET_SQE_opcode, IORING_OP_ACCEPT);
        UNSAFE.putByte(sqeAddr + OFFSET_SQE_flags, (byte) 0);
        UNSAFE.putShort(sqeAddr + OFFSET_SQE_ioprio, (short) 0);
        UNSAFE.putInt(sqeAddr + OFFSET_SQE_fd, linuxSocket.fd());
        UNSAFE.putLong(sqeAddr + OFFSET_SQE_off, acceptMemory.lenAddr);
        UNSAFE.putLong(sqeAddr + OFFSET_SQE_addr, acceptMemory.addr);
        UNSAFE.putInt(sqeAddr + OFFSET_SQE_len, 0);
        UNSAFE.putInt(sqeAddr + OFFSET_SQE_rw_flags, SOCK_NONBLOCK | SOCK_CLOEXEC);
        UNSAFE.putLong(sqeAddr + OFFSET_SQE_user_data, userdata_OP_ACCEPT);
    }

    private class Handler_OP_ACCEPT implements CompletionHandler {

        @Override
        public void handle(int res, int flags, long userdata) {
            try {
                if (res >= 0) {
                    int fd = res;
                    metrics.incAccepted();

                    // re-register for more accepts.
                    sq_add_OP_ACCEPT();

                    SocketAddress address = LinuxSocket.toInetSocketAddress(
                            acceptMemory.addr,
                            acceptMemory.lenAddr);

                    if (logger.isInfoEnabled()) {
                        logger.info(IOUringAsyncServerSocket.this + " new connected accepted: " + address);
                    }

                    // todo: ugly that AF_INET is hard configured.
                    // We should use the address to determine the type
                    LinuxSocket linuxSocket = new LinuxSocket(fd, AF_INET);
                    AcceptRequest acceptRequest = new IOUringAcceptRequest(linuxSocket);
                    try {
                        acceptFn.accept(acceptRequest);
                    } catch (Throwable t) {
                        logger.severe(t);

                        // If for whatever reason the socket isn't consumed, we need
                        // to properly close the socket. Otherwise the socket remains
                        // under a CLOSE_WAIT state and the port doesn't get released.
                        closeQuietly(acceptRequest);

                        if (logger.isWarningEnabled()) {
                            logger.warning("Closing socket " + address + "->" + getLocalAddress());
                        }
                    }
                } else {
                    throw newCQEFailedException("Failed to accept a socket.", "accept(2)", IORING_OP_ACCEPT, -res);
                }
            } catch (Exception e) {
                close(null, e);
            }
        }
    }

    @SuppressWarnings("checkstyle:SimplifyBooleanReturn")
    public static class IOUringOptions implements AsyncSocket.Options {

        private final LinuxSocket nativeSocket;

        IOUringOptions(LinuxSocket nativeSocket) {
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
     * An {@link IOUringAsyncServerSocket} builder.
     */
    @SuppressWarnings({"checkstyle:VisibilityModifier"})
    public static class Builder extends AsyncServerSocket.Builder {

        public final LinuxSocket nativeSocket;

        Builder() {
            // to conclude.
            this.nativeSocket = LinuxSocket.openTcpIpv4Socket();
            nativeSocket.setBlocking(true);
            this.options = new IOUringOptions(nativeSocket);
        }

        @Override
        protected void conclude() {
            super.conclude();

            checkNotNull(nativeSocket, "nativeSocket");
        }

        @Override
        public AsyncServerSocket construct() {
            return new IOUringAsyncServerSocket(this);
        }
    }
}
