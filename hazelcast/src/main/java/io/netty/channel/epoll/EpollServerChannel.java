package io.netty.channel.epoll;

import com.hazelcast.spi.impl.reactor.SocketConfig;

import java.nio.channels.ServerSocketChannel;

// https://stackoverflow.com/questions/51777259/how-to-code-an-epoll-based-sockets-client-in-c
public class EpollServerChannel {
    public SocketConfig socketConfig;
    public LinuxSocket serverSocket;
    public int flags = Native.EPOLLIN;
}
