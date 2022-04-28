package com.hazelcast.spi.impl.engine.iouring;

import com.hazelcast.spi.impl.engine.AsyncServerSocket;
import io.netty.incubator.channel.uring.IOUringSubmissionQueue;
import io.netty.incubator.channel.uring.LinuxSocket;
import io.netty.incubator.channel.uring.SockaddrIn;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.SocketAddress;
import java.util.function.Consumer;

public class IOUringAsyncServerSocket extends AsyncServerSocket implements CompletionListener {

    public static IOUringAsyncServerSocket open(IOUringEventloop eventloop) {
        return new IOUringAsyncServerSocket(eventloop);
    }

    public IOUringSubmissionQueue sq;
    public LinuxSocket serverSocket;
    public IOUringEventloop eventloop;
    private final AcceptMemory acceptMemory = new AcceptMemory();
    private final byte[] inet4AddressArray = new byte[SockaddrIn.IPV4_ADDRESS_LENGTH];
    private Consumer<IOUringAsyncSocket> consumer;

    private IOUringAsyncServerSocket(IOUringEventloop eventloop) {
        try {
            this.eventloop = eventloop;
            this.serverSocket = LinuxSocket.newSocketStream(false);
            serverSocket.setBlocking();

            eventloop.execute(() -> {
                eventloop.completionListeners.put(serverSocket.intValue(), IOUringAsyncServerSocket.this);
                sq = eventloop.sq;
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public LinuxSocket serverSocket() {
        return serverSocket;
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
    public void setReuseAddress(boolean reuseAddress) {
        try {
            serverSocket.setReuseAddress(reuseAddress);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void setReceiveBufferSize(int size) {
        try {
            serverSocket.setReceiveBufferSize(size);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public int getReceiveBufferSize() {
        try {
            return serverSocket.getReceiveBufferSize();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
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

    @Override
    public void handle(int fd, int res, int flags, byte op, short data) {
        sq_addAccept();

        if (res < 0) {
            System.out.println("Problem: IORING_OP_ACCEPT res: " + res);
        } else {

//        System.out.println(getName() + " handle IORING_OP_ACCEPT fd:" + fd + " serverFd:" + serverSocket.intValue() + "res:" + res);

            SocketAddress address = SockaddrIn.readIPv4(acceptMemory.memoryAddress, inet4AddressArray);

            System.out.println(this + " new connected accepted: " + address);

            LinuxSocket socket = new LinuxSocket(res);
            IOUringAsyncSocket asyncSocket = new IOUringAsyncSocket(socket);
            consumer.accept(asyncSocket);
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
            System.out.println(" ServerSocket listening at " + getLocalAddress());
        });
    }
}
