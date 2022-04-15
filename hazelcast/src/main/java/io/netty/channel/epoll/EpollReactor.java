package io.netty.channel.epoll;

import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.engine.Channel;
import com.hazelcast.spi.impl.engine.Reactor;
import com.hazelcast.spi.impl.engine.Scheduler;
import io.netty.channel.unix.FileDescriptor;
import io.netty.util.collection.IntObjectHashMap;
import io.netty.util.collection.IntObjectMap;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.netty.channel.epoll.Native.EPOLLIN;
import static io.netty.channel.epoll.Native.epollCtlAdd;

public final class EpollReactor extends Reactor {
    private final boolean spin;
    private final AtomicBoolean wakeupNeeded = new AtomicBoolean(true);
    private final IntObjectMap channels = new IntObjectHashMap<>(4096);
    private final IntObjectMap<EpollServerChannel> serverChannels = new IntObjectHashMap<>(4096);
    public final FileDescriptor epollFd;
    private final FileDescriptor eventFd;
    private final FileDescriptor timerFd;
    private final EpollEventArray events;

    public EpollReactor(int idx, String name, ILogger logger, Scheduler scheduler, boolean spin) {
        super(idx, name, logger, scheduler, spin);
        this.spin = false;//config.spin;
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

             System.out.println(getName() + " eventLoop run " + k);

            boolean moreWork = scheduler.tick();

            flushDirtyChannels();

            int ready;
            if (spin || moreWork) {
                System.out.println("epollBusyWait");
                ready = epollBusyWait();
            } else {
                wakeupNeeded.set(true);
                if (publicRunQueue.isEmpty()) {
                    System.out.println("epollWait");
                    ready = epollWait();
                } else {
                    System.out.println("epollBusyWait");
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
        return Native.epollWait(epollFd, events, false);
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

                Object channel = channels.get(fd);
                if (channel != null) {
                    System.out.println("channel found");
                    if ((ev & (Native.EPOLLERR | Native.EPOLLOUT)) != 0) {
                        System.out.println("epollout");
                        ((EpollChannel)channel).handleWrite();
                    }

                    if ((ev & (Native.EPOLLERR | EPOLLIN)) != 0) {
                        System.out.println("epoll in");
                        if(channel instanceof EpollServerChannel){
                            System.out.println("EpollServerChannel.handleAccept");
                            ((EpollServerChannel)channel).handleAccept();
                        }else{
                            System.out.println("EpollChannel.handleRead");
                            ((EpollChannel)channel).handleRead();
                        }
                    }
                } else {
                    System.out.println("no channel found");
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

    public void accept(EpollServerChannel serverChannel) throws IOException {
        LinuxSocket serverSocket = LinuxSocket.newSocketStream(false);

        // should come from properties.
        serverSocket.setReuseAddress(true);
        System.out.println(getName() + " serverSocket.fd:" + serverSocket.intValue());

        serverSocket.bind(serverChannel.address);
        System.out.println(getName() + " Bind success " + serverChannel.address);
        serverSocket.listen(10);
        System.out.println(getName() + " Listening on " + serverChannel.address);

        schedule(() -> {
            serverChannel.reactor = EpollReactor.this;
            serverChannel.serverSocket = serverSocket;
            channels.put(serverSocket.intValue(), serverChannel);
            serverChannels.put(serverSocket.intValue(), serverChannel);
            serverSocket.listen(serverChannel.socketConfig.backlog);
            epollCtlAdd(epollFd.intValue(), serverSocket.intValue(), serverChannel.flags);
        });
    }

    @Override
    public Future<Channel> connect(Channel c, SocketAddress address) {
        EpollChannel channel = (EpollChannel) c;

        CompletableFuture<Channel> future = new CompletableFuture();
        try {
            System.out.println("ConnectRequest address:" + address);

            LinuxSocket socket = LinuxSocket.newSocketStream();
            channel.configure(this, socket, c.socketConfig);

            if (!socket.connect(address)) {
                future.completeExceptionally(new RuntimeException("Failed to connect to " + address));
            } else {
                schedule(() -> {
                    try {
                        channel.onConnectionEstablished();
                        registeredChannels.add(channel);
                        logger.info("Socket listening at " + address);
                        future.complete(channel);
                    } catch (Exception e) {
                        future.completeExceptionally(e);
                    }
                });
            }
        } catch (Exception e) {
            future.completeExceptionally(e);
        }
        return future;
    }


}
