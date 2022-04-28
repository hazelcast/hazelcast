package com.hazelcast.spi.impl.engine.epoll;

import com.hazelcast.spi.impl.engine.AsyncServerSocket;
import io.netty.channel.epoll.LinuxSocket;
import io.netty.channel.epoll.Native;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.function.Supplier;

// https://stackoverflow.com/questions/51777259/how-to-code-an-epoll-based-sockets-client-in-c
public class EpollAsyncServerSocket extends AsyncServerSocket {
    public LinuxSocket serverSocket;
    public int flags = Native.EPOLLIN;
    public Supplier<EpollAsyncSocket> channelSupplier;
    public InetSocketAddress address;
    private final byte[] acceptedAddress = new byte[26];
    public EpollEventloop eventloop;

    public LinuxSocket socket() {
        return serverSocket;
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
    public void bind(SocketAddress socketAddress) {

    }

    @Override
    public void close() {

    }

    public void handleAccept() {
        try {
            System.out.println("Handle accept");

            int fd = serverSocket.accept(acceptedAddress);

            LinuxSocket socket = new LinuxSocket(fd);

            EpollAsyncSocket channel = channelSupplier.get();
            eventloop.registeredAsyncSockets.add(channel);

            // channel.configure(eventloop, socket, socketConfig);
            channel.onConnectionEstablished();

            System.out.println("accepted fd:" + fd);

//        sq_addAccept();
//
////        System.out.println(getName() + " handle IORING_OP_ACCEPT fd:" + fd + " serverFd:" + serverSocket.intValue() + "res:" + res);
//
//        SocketAddress address = SockaddrIn.readIPv4(acceptMemory.memoryAddress, inet4AddressArray);
//
//        System.out.println(this + " new connected accepted: " + address);
//
//        IOUringChannel channel = channelSupplier.get();
//        io.netty.incubator.channel.uring.LinuxSocket socket = new io.netty.incubator.channel.uring.LinuxSocket(res);
//        reactor.channelMap.put(socket.intValue(), channel);
//        try {
//            channel.configure(reactor, socketConfig, socket);
//            channel.onConnectionEstablished();
//        } catch (IOException e) {
//            throw new RuntimeException();
//        }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
