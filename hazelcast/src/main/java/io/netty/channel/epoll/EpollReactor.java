package io.netty.channel.epoll;

import com.hazelcast.internal.nio.Connection;
import com.hazelcast.spi.impl.reactor.Channel;
import com.hazelcast.spi.impl.reactor.Reactor;
import com.hazelcast.spi.impl.reactor.SocketConfig;
import io.netty.channel.unix.FileDescriptor;
import io.netty.util.collection.IntObjectHashMap;
import io.netty.util.collection.IntObjectMap;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.netty.channel.epoll.Native.EPOLLIN;
import static io.netty.channel.epoll.Native.epollCtlAdd;

public final class EpollReactor extends Reactor {
    private final boolean spin;
    private final boolean writeThrough;
    private final AtomicBoolean wakeupNeeded = new AtomicBoolean(true);
    private final IntObjectMap<EpollChannel> channels = new IntObjectHashMap<>(4096);
    private final IntObjectMap<EpollServerChannel> serverChannels = new IntObjectHashMap<>(4096);
    public final FileDescriptor epollFd;
    private final FileDescriptor eventFd;
    private final FileDescriptor timerFd;
    private final EpollEventArray events;

    public EpollReactor(EpollReactorConfig config) {
        super(config);
        this.spin = config.spin;
        this.writeThrough = config.writeThrough;
        this.events = new EpollEventArray(4096);
        this.epollFd = Native.newEpollCreate();
        this.eventFd = Native.newEventFd();
        try {
            // It is important to use EPOLLET here as we only want to get the notification once per
            // wakeup and don't call eventfd_read(...).
            epollCtlAdd(epollFd.intValue(), eventFd.intValue(), EPOLLIN | Native.EPOLLET);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to add eventFd filedescriptor to epoll", e);
        }
        this.timerFd = Native.newTimerFd();
        try {
            // It is important to use EPOLLET here as we only want to get the notification once per
            // wakeup and don't call read(...).
            epollCtlAdd(epollFd.intValue(), timerFd.intValue(), EPOLLIN | Native.EPOLLET);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to add timerFd filedescriptor to epoll", e);
        }
    }

    @Override
    public Future<Channel> connect(Channel channel, SocketAddress address)  {
        return null;
    }

    @Override
    public void wakeup() {
        if (spin || Thread.currentThread() == this) {
            return;
        }

        if (wakeupNeeded.get() && wakeupNeeded.compareAndSet(true, false)) {
            Native.eventFdWrite(eventFd.intValue(), 1L);
        }
    }

    @Override
    protected void eventLoop() throws Exception {
        int k = 0;
        while (running) {
            runTasks();
            k++;

            Thread.sleep(500);
            System.out.println(getName() + " eventLoop run " + k);

            boolean moreWork = scheduler.tick();

            flushDirtyChannels();

            int ready;
            if (spin || moreWork) {
                ready = epollBusyWait();
            } else {
                wakeupNeeded.set(true);
                if (publicRunQueue.isEmpty()) {
                    ready = epollWait();
                } else {
                    ready = epollBusyWait();
                }
                wakeupNeeded.set(false);
            }

            if (ready > 0) {
                processReady(ready);
            }
        }
    }

    private int epollWait() throws IOException {
        return Native.epollWait(epollFd, events, true);
    }

    private int epollBusyWait() throws IOException {
        return Native.epollBusyWait(epollFd, events);
    }

    private void processReady(int ready) {
        System.out.println("handleReadyEvents: " + ready + " ready");

        for (int i = 0; i < ready; i++) {
            final int fd = events.fd(i);
            if (fd == eventFd.intValue()) {
                System.out.println("eventFd");
                //pendingWakeup = false;
            } else if (fd == timerFd.intValue()) {
                System.out.println("timerFd");
                //timerFired = true;
            } else {
                System.out.println("Something else");
                final long ev = events.events(i);

                EpollChannel channel = channels.get(fd);
                if (channel != null) {
                    if ((ev & (Native.EPOLLERR | Native.EPOLLOUT)) != 0) {
                        channel.handleWrite();
                    }

                    if ((ev & (Native.EPOLLERR | EPOLLIN)) != 0) {
                        channel.handleRead();
                    }
                } else {
                    // no channel found
                    // We received an event for an fd which we not use anymore. Remove it from the epoll_event set.
                    try {
                        Native.epollCtlDel(epollFd.intValue(), fd);
                    } catch (IOException ignore) {
                    }
                }
            }
        }
    }


    private void handleAccept(SelectionKey key) {
//        EpollServerChannel serverChannel = (EpollServerChannel) key.attachment();
//        try {
//            SocketChannel socketChannel = serverChannel.serverSocketChannel.accept();
//            socketChannel.configureBlocking(false);
//            configure(socketChannel.socket(), serverChannel.socketConfig);
//
//            SelectionKey channelSelectionKey = socketChannel.register(selector, OP_READ);
//            channelSelectionKey.attach(newChannel(socketChannel, null, key, serverChannel.socketConfig));
//
//            logger.info("Connection Accepted: " + socketChannel.getLocalAddress());
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }

    public void register(EpollServerChannel serverChannel) throws IOException {
        LinuxSocket serverSocket = LinuxSocket.newSocketStream(false);

        // should come from properties.
        serverSocket.setReuseAddress(true);
        System.out.println(getName() + " serverSocket.fd:" + serverSocket.intValue());

        serverSocket.bind(serverChannel.address);
        System.out.println(getName() + " Bind success " + serverChannel.address);
        serverSocket.listen(10);
        System.out.println(getName() + " Listening on " + serverChannel.address);

        schedule(() -> {
            serverChannel.serverSocket = serverSocket;
            serverChannels.put(serverSocket.intValue(), serverChannel);
            serverSocket.listen(serverChannel.socketConfig.backlog);
            epollCtlAdd(epollFd.intValue(), serverSocket.intValue(), serverChannel.flags);
        });
    }
//
//    @Override
//    protected void handleConnect(ConnectRequest request) {
//        try {
//            SocketAddress address = request.address;
//            System.out.println("ConnectRequest address:" + address);
//
//            LinuxSocket socket = LinuxSocket.newSocketStream();
//            configure(socket, request.socketConfig);
//            if (!socket.connect(address)) {
//                throw new RuntimeException("Failed to connect to " + request.address);
//            }
//
////            socketChannel.configureBlocking(false);
//
////            SelectionKey key = socketChannel.register(selector, OP_READ);
////
//            EpollChannel channel = newChannel(socket, request.connection, request.socketConfig);
//            channel.setFlag(EPOLLIN);
//            //todo: register for read.
//
//            logger.info("Socket listening at " + address);
//            request.future.complete(channel);
//        } catch (Exception e) {
//            e.printStackTrace();
//            request.future.completeExceptionally(e);
//        }
//    }

//    private EpollChannel newChannel(LinuxSocket socket, Connection connection, SocketConfig socketConfig) throws IOException {
//        System.out.println(this + " newChannel: " + socket);
//
//        EpollChannel channel = new EpollChannel();
//        channel.reactor = this;
//        channel.writeThrough = writeThrough;
//        channel.receiveBuffer = ByteBuffer.allocateDirect(socketConfig.receiveBufferSize);
//        channel.socket = socket;
//        channel.connection = connection;
//        channel.remoteAddress = socket.remoteAddress();
//        channel.localAddress = socket.localAddress();
//        registeredChannels.add(channel);
//
//        return channel;
//    }

    private void configure(LinuxSocket socket, SocketConfig socketConfig) throws IOException {
        socket.setTcpNoDelay(socketConfig.tcpNoDelay);
        socket.setSendBufferSize(socketConfig.sendBufferSize);
        socket.setReceiveBufferSize(socketConfig.receiveBufferSize);
        //socket.setTcpQuickAck(socketConfig.tcpQuickAck);

        String id = socket.localAddress() + "->" + socket.remoteAddress();
        System.out.println(getName() + " " + id + " tcpNoDelay: " + socket.isTcpNoDelay());
        System.out.println(getName() + " " + id + " tcpQuickAck: " + socket.isTcpQuickAck());
        System.out.println(getName() + " " + id + " receiveBufferSize: " + socket.getReceiveBufferSize());
        System.out.println(getName() + " " + id + " sendBufferSize: " + socket.getSendBufferSize());
    }

}
