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

package com.hazelcast.tpc.engine.iouring;

import com.hazelcast.tpc.engine.AsyncServerSocket;
import io.netty.incubator.channel.uring.IOUringSubmissionQueue;
import io.netty.incubator.channel.uring.LinuxSocket;
import io.netty.incubator.channel.uring.SockaddrIn;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.SocketAddress;
import java.util.function.Consumer;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

public final class IOUringAsyncServerSocket extends AsyncServerSocket {

    /**
     * Opens a new IOUringAsyncServerSocket.
     *
     * @param eventloop the IOUringEventloop this IOUringAsyncServerSocket belongs to.
     * @return the created IOUringAsyncServerSocket
     * @throws NullPointerException if eventloop is null.
     * @throws UncheckedIOException if there was a problem opening the socket.
     */
    public static IOUringAsyncServerSocket open(IOUringEventloop eventloop) {
        return new IOUringAsyncServerSocket(eventloop);
    }

    private final LinuxSocket serverSocket;
    private final IOUringEventloop eventloop;
    private final AcceptMemory acceptMemory = new AcceptMemory();
    private final byte[] inet4AddressArray = new byte[SockaddrIn.IPV4_ADDRESS_LENGTH];
    private IOUringSubmissionQueue sq;
    private Consumer<IOUringAsyncSocket> consumer;

    private IOUringAsyncServerSocket(IOUringEventloop eventloop) {
        try {
            this.serverSocket = LinuxSocket.newSocketStream(false);
            serverSocket.setBlocking();
            this.eventloop = checkNotNull(eventloop);

            if (!eventloop.registerResource(this)) {
                close();
                throw new IllegalStateException("EventLoop is not running");
            }

            eventloop.execute(() -> {
                eventloop.completionListeners.put(serverSocket.intValue(), new EventloopHandler());
                sq = eventloop.sq;
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Returns the underlying {@link LinuxSocket}.
     *
     * @return the {@link LinuxSocket}.
     */
    public LinuxSocket serverSocket() {
        return serverSocket;
    }

    @Override
    public int getLocalPort() {
        return serverSocket.localAddress().getPort();
    }

    @Override
    public IOUringEventloop eventloop() {
        return eventloop;
    }

    @Override
    protected SocketAddress getLocalAddress0() {
        return serverSocket.localAddress();
    }

    @Override
    public void listen(int backlog) {
        try {
            serverSocket.listen(10);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public boolean isReusePort() {
        try {
            return serverSocket.isReusePort();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void setReusePort(boolean reusePort) {
        try {
            serverSocket.setReusePort(reusePort);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public boolean isReuseAddress() {
        try {
            return serverSocket.isReuseAddress();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void reuseAddress(boolean reuseAddress) {
        try {
            serverSocket.setReuseAddress(reuseAddress);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void receiveBufferSize(int size) {
        try {
            serverSocket.setReceiveBufferSize(size);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public int receiveBufferSize() {
        try {
            return serverSocket.getReceiveBufferSize();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            if(logger.isInfoEnabled()) {
                logger.info("Closing  " + this);
            }
            eventloop.deregisterResource(this);
            try {
                serverSocket.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    @Override
    public void bind(SocketAddress local) {
        try {
            serverSocket.bind(local);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void sq_addAccept() {
        sq.addAccept(serverSocket.intValue(),
                acceptMemory.memoryAddress,
                acceptMemory.lengthMemoryAddress, (short) 0);
    }

    public void accept(Consumer<IOUringAsyncSocket> consumer) {
        eventloop.execute(() -> {
            this.consumer = consumer;
            sq_addAccept();
            if(logger.isInfoEnabled()) {
                logger.info("ServerSocket listening at " + localAddress());
            }
        });
    }

    private class EventloopHandler implements CompletionListener {

        @Override
        public void handle(int fd, int res, int flags, byte op, short data) {
            sq_addAccept();

            if (res < 0) {
                logger.warning("Problem: IORING_OP_ACCEPT res: " + res);
            } else {
                // System.out.println(getName() + " handle IORING_OP_ACCEPT fd:" + fd + " serverFd:" + serverSocket.intValue() + "res:" + res);

                SocketAddress address = SockaddrIn.readIPv4(acceptMemory.memoryAddress, inet4AddressArray);

                if(logger.isInfoEnabled()) {
                    logger.info(this + " new connected accepted: " + address);
                }
                LinuxSocket socket = new LinuxSocket(res);
                IOUringAsyncSocket asyncSocket = new IOUringAsyncSocket(socket);
                consumer.accept(asyncSocket);
            }
        }
    }
}
