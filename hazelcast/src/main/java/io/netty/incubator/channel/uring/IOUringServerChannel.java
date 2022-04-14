package io.netty.incubator.channel.uring;

import com.hazelcast.internal.networking.nio.NioChannel;
import com.hazelcast.spi.impl.reactor.SocketConfig;
import io.netty.buffer.ByteBuf;
import io.netty.channel.unix.IovArray;


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
        LinuxSocket socket = new LinuxSocket(res);
        try {
            reactor.configure(socket, socketConfig);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        IOUringChannel channel = channelSupplier.get();
        channel.reactor = reactor;
        channel.receiveBuff = reactor.allocator.directBuffer(socketConfig.receiveBufferSize);
        channel.socket = socket;
        channel.remoteAddress = socket.remoteAddress();
        channel.localAddress = socket.localAddress();
        ByteBuf iovArrayBuffer = reactor.iovArrayBufferAllocator.directBuffer(1024 * IovArray.IOV_SIZE);
        channel.iovArray = new IovArray(iovArrayBuffer);
        channel.sq = sq;
        reactor.channelMap.put(socket.intValue(), channel);
        channel.sq_addRead();
        channel.sq_addRead();
    }
}
