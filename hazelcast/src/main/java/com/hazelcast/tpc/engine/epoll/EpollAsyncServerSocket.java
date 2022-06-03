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

package com.hazelcast.tpc.engine.epoll;

import com.hazelcast.tpc.engine.AsyncServerSocket;
import com.hazelcast.tpc.engine.Eventloop;
import com.hazelcast.tpc.engine.nio.NioAsyncSocket;
import io.netty.channel.epoll.LinuxSocket;
import io.netty.channel.epoll.Native;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.function.Consumer;

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
        this.serverSocket = LinuxSocket.newSocketStream();
        this.eventloop = eventloop;
        if (!eventloop.registerResource(this)) {
            close();
            throw new IllegalStateException("EventLoop is not running");
        }

        // this.serverSocket.
//            this.selector = eventloop.selector;
//            this.serverSocketChannel = ServerSocketChannel.open();
//            serverSocketChannel.configureBlocking(false);
//        } catch (IOException e) {
//            throw new UncheckedIOException(e);
//        }
    }

    @Override
    protected SocketAddress getLocalAddress0() throws Exception {
        return serverSocket.localAddress();
    }

    public LinuxSocket socket() {
        return serverSocket;
    }

    @Override
    public Eventloop eventloop() {
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
    public void bind(SocketAddress socketAddress) {
        try {
            serverSocket.bind(socketAddress);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            System.out.println("Closing  " + this);
            eventloop.deregisterResource(this);
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
