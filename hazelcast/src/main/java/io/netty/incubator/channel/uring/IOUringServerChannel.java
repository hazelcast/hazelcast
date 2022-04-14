package io.netty.incubator.channel.uring;

import com.hazelcast.spi.impl.reactor.SocketConfig;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.function.Supplier;

public class IOUringServerChannel {

    public SocketConfig socketConfig;
    public IOUringSubmissionQueue sq;
    public LinuxSocket serverSocket;
    private final AcceptMemory acceptMemory = new AcceptMemory();
    private final byte[] inet4AddressArray = new byte[SockaddrIn.IPV4_ADDRESS_LENGTH];
    public InetSocketAddress address;
    public Supplier<IOUringChannel> channelSupplier;
    public IOUringReactor reactor;

    public void sq_addAccept() {
        sq.addAccept(serverSocket.intValue(),
                acceptMemory.memoryAddress,
                acceptMemory.lengthMemoryAddress, (short) 0);
    }

    public void handle_IORING_OP_ACCEPT(int res, int flags, short data) {
        sq_addAccept();

//        System.out.println(getName() + " handle IORING_OP_ACCEPT fd:" + fd + " serverFd:" + serverSocket.intValue() + "res:" + res);

        SocketAddress address = SockaddrIn.readIPv4(acceptMemory.memoryAddress, inet4AddressArray);

        System.out.println(this + " new connected accepted: " + address);

        IOUringChannel channel = channelSupplier.get();
        LinuxSocket socket = new LinuxSocket(res);
        reactor.channelMap.put(socket.intValue(), channel);
        try {
            channel.configure(reactor, socketConfig, socket);
            channel.onConnectionEstablished();
        } catch (IOException e) {
            throw new RuntimeException();
        }
    }
}
