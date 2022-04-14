package io.netty.channel.epoll;

import com.hazelcast.spi.impl.reactor.SocketConfig;

import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.util.function.Supplier;

// https://stackoverflow.com/questions/51777259/how-to-code-an-epoll-based-sockets-client-in-c
public class EpollServerChannel {
    public SocketConfig socketConfig;
    public LinuxSocket serverSocket;
    public int flags = Native.EPOLLIN;
    public Supplier<EpollChannel> channelSupplier;
    public InetSocketAddress address;
}
