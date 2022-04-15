package io.netty.incubator.channel.uring;

import com.hazelcast.spi.impl.engine.SocketConfig;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.function.Supplier;

public class IOUringServerChannel implements CompletionListener {

    public SocketConfig socketConfig;
    public IOUringSubmissionQueue sq;
    public LinuxSocket serverSocket;
    public InetSocketAddress address;
    public Supplier<IOUringChannel> channelSupplier;
    public IOUringReactor reactor;
    private final AcceptMemory acceptMemory = new AcceptMemory();
    private final byte[] inet4AddressArray = new byte[SockaddrIn.IPV4_ADDRESS_LENGTH];

    public void sq_addAccept() {
        sq.addAccept(serverSocket.intValue(),
                acceptMemory.memoryAddress,
                acceptMemory.lengthMemoryAddress, (short) 0);
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

            IOUringChannel channel = channelSupplier.get();
            LinuxSocket socket = new LinuxSocket(res);
            reactor.completionListeners.put(socket.intValue(), channel);
            try {
                channel.configure(reactor, socketConfig, socket);
                channel.onConnectionEstablished();
            } catch (IOException e) {
                throw new RuntimeException();
            }
        }
    }
}
