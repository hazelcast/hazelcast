package com.hazelcast.tpc.engine.epoll;

import com.hazelcast.tpc.engine.AsyncServerSocket;
import com.hazelcast.tpc.engine.Eventloop;
import com.hazelcast.tpc.engine.nio.NioAsyncServerSocket;
import com.hazelcast.tpc.engine.nio.NioAsyncSocket;
import io.netty.channel.epoll.LinuxSocket;
import io.netty.channel.epoll.Native;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.nio.channels.SelectionKey.OP_ACCEPT;

// https://stackoverflow.com/questions/51777259/how-to-code-an-epoll-based-sockets-client-in-c
public final class EpollAsyncServerSocket extends AsyncServerSocket {


    public static EpollAsyncServerSocket open(EpollEventloop eventloop) {
        return new EpollAsyncServerSocket(eventloop);
    }


    public LinuxSocket serverSocket;
    public int flags = Native.EPOLLIN;
    public InetSocketAddress address;
    private final byte[] acceptedAddress = new byte[26];
    private EpollEventloop eventloop;

    private EpollAsyncServerSocket(EpollEventloop eventloop) {
//        try {
        this.eventloop = eventloop;
        this.eventloop.registerServerSocket(this);
        this.serverSocket = LinuxSocket.newSocketStream();
        // this.serverSocket.
//            this.selector = eventloop.selector;
//            this.serverSocketChannel = ServerSocketChannel.open();
//            serverSocketChannel.configureBlocking(false);
//        } catch (IOException e) {
//            throw new UncheckedIOException(e);
//        }
    }

    @Override
    protected SocketAddress localAddress() throws Exception {
        return serverSocket.localAddress();
    }

    public LinuxSocket socket() {
        return serverSocket;
    }

    @Override
    public Eventloop getEventloop() {
        return eventloop;
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
        try {
            serverSocket.bind(socketAddress);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() {
        if(closed.compareAndSet(false,true)) {
            System.out.println("Closing  "+ this);
            eventloop.deregisterSocket(this);
            try {
                serverSocket.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void listen(int backlog) {
        try {
            serverSocket.listen(backlog);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    //
//    public void accept(EpollAsyncServerSocket serverChannel) throws IOException {
//        LinuxSocket serverSocket = LinuxSocket.newSocketStream(false);
//
//        // should come from properties.
//        serverSocket.setReuseAddress(true);
//        System.out.println(getName() + " serverSocket.fd:" + serverSocket.intValue());
//
//        serverSocket.bind(serverChannel.address);
//        System.out.println(getName() + " Bind success " + serverChannel.address);
//        serverSocket.listen(10);
//        System.out.println(getName() + " Listening on " + serverChannel.address);
//
//        execute(() -> {
//            serverChannel.eventloop = EpollEventloop.this;
//            serverChannel.serverSocket = serverSocket;
//            channels.put(serverSocket.intValue(), serverChannel);
//            serverChannels.put(serverSocket.intValue(), serverChannel);
//            //serverSocket.listen(serverChannel.socketConfig.backlog);
//            epollCtlAdd(epollFd.intValue(), serverSocket.intValue(), serverChannel.flags);
//        });
//    }


    public void accept(Consumer<NioAsyncSocket> consumer) {
        eventloop.execute(() -> {
            //serverSocketChannel.register(selector, OP_ACCEPT, new NioAsyncServerSocket.AcceptHandler(consumer));
            //System.out.println(eventloop.getName() + " ServerSocket listening at " + serverSocketChannel.getLocalAddress());
        });
    }

    public void handleAccept() {

    }
}
