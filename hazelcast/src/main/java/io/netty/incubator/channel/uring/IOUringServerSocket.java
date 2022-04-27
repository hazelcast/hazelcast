package io.netty.incubator.channel.uring;

import com.hazelcast.spi.impl.engine.SocketConfig;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.function.Supplier;

public class IOUringServerSocket implements CompletionListener {

    public SocketConfig socketConfig;
    public IOUringSubmissionQueue sq;
    public LinuxSocket serverSocket;
    public InetSocketAddress address;
    public Supplier<IOUringAsyncSocket> channelSupplier;
    public IOUringEventloop eventloop;
    private final AcceptMemory acceptMemory = new AcceptMemory();
    private final byte[] inet4AddressArray = new byte[SockaddrIn.IPV4_ADDRESS_LENGTH];

    public void sq_addAccept() {
        sq.addAccept(serverSocket.intValue(),
                acceptMemory.memoryAddress,
                acceptMemory.lengthMemoryAddress, (short) 0);
    }

    public void configure(IOUringEventloop eventloop) throws IOException {
        this.eventloop = eventloop;
        this.sq = eventloop.sq;

        serverSocket = LinuxSocket.newSocketStream(false);
        serverSocket.setBlocking();
        serverSocket.setReuseAddress(true);
        serverSocket.setReceiveBufferSize(socketConfig.receiveBufferSize);
        System.out.println(eventloop + " serverSocket.fd:" + serverSocket.intValue());

        serverSocket.bind(address);
        System.out.println(eventloop.getName() + " Bind success " + address);
        serverSocket.listen(10);
        System.out.println(eventloop.getName() + " Listening on " + address);
        eventloop.completionListeners.put(serverSocket.intValue(), this);
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

            IOUringAsyncSocket channel = channelSupplier.get();
            LinuxSocket socket = new LinuxSocket(res);
            eventloop.completionListeners.put(socket.intValue(), channel);
            try {
                channel.configure(eventloop, socketConfig, socket);
                channel.onConnectionEstablished();
            } catch (IOException e) {
                throw new RuntimeException();
            }
        }
    }

    public void accept() {
        sq_addAccept();
    }
}
