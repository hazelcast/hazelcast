package io.netty.channel.epoll;

import com.hazelcast.spi.impl.reactor.SocketConfig;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.function.Supplier;

// https://stackoverflow.com/questions/51777259/how-to-code-an-epoll-based-sockets-client-in-c
public class EpollServerChannel {
    public SocketConfig socketConfig;
    public LinuxSocket serverSocket;
    public int flags = Native.EPOLLIN;
    public Supplier<EpollChannel> channelSupplier;
    public InetSocketAddress address;
    private final byte[] acceptedAddress = new byte[26];
    public EpollReactor reactor;

    public void handleAccept() {
        try {
            System.out.println("Handle accept");

            int fd = serverSocket.accept(acceptedAddress);

            LinuxSocket socket = new LinuxSocket(fd);

            EpollChannel channel = channelSupplier.get();
            reactor.registeredChannels.add(channel);

            channel.configure(reactor, socket, socketConfig);
            channel.onConnectionEstablished();

            System.out.println("accepted fd:"+fd);

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
