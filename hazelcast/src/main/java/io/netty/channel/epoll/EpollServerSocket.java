package io.netty.channel.epoll;

import com.hazelcast.spi.impl.engine.SocketConfig;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.function.Supplier;

// https://stackoverflow.com/questions/51777259/how-to-code-an-epoll-based-sockets-client-in-c
public class EpollServerSocket {
    public SocketConfig socketConfig;
    public LinuxSocket serverSocket;
    public int flags = Native.EPOLLIN;
    public Supplier<EpollAsyncSocket> channelSupplier;
    public InetSocketAddress address;
    private final byte[] acceptedAddress = new byte[26];
    public EpollEventloop eventloop;

    public void handleAccept() {
        try {
            System.out.println("Handle accept");

            int fd = serverSocket.accept(acceptedAddress);

            LinuxSocket socket = new LinuxSocket(fd);

            EpollAsyncSocket channel = channelSupplier.get();
            eventloop.registeredsockets.add(channel);

            channel.configure(eventloop, socket, socketConfig);
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
