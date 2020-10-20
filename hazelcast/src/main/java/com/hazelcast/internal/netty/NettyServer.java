package com.hazelcast.internal.netty;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.server.ServerConnectionManager;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.channel.uring.IOUringEventLoopGroup;
import io.netty.channel.uring.IOUringServerSocketChannel;
import io.netty.channel.uring.IOUringSocketChannel;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

public class NettyServer {

    private final Address thisAddress;
    private final Consumer<Packet> packetDispatcher;
    private MultithreadEventLoopGroup bossGroup;
    private MultithreadEventLoopGroup workerGroup;
    private ServerBootstrap serverBootstrap;
    private Bootstrap clientBootstrap;
    //   private NioEventLoopGroup clientEventLoopGroup;
    private ServerConnectionManager serverConnectionManager;
    private int threadCount = 8;
    private Mode mode = Mode.IO_URING;

    enum Mode{NIO,EPOLL,IO_URING}

    public NettyServer(Address thisAddress, Consumer<Packet> packetDispatcher) {
        this.thisAddress = thisAddress;
        this.packetDispatcher = packetDispatcher;
        System.out.println("Mode:"+mode);
    }

    public void setServerConnectionManager(ServerConnectionManager serverConnectionManager) {
        this.serverConnectionManager = serverConnectionManager;
    }

    public void start() {
        System.out.println("Started ");
        int inetPort = thisAddress.getPort() + 10000;
        System.out.println("Started netty server on " + (thisAddress.getHost() + " " + inetPort));

        bossGroup = newBossGroup();
        workerGroup = newWorkerGroup();

        serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(bossGroup, workerGroup)
                .channel(getServerSocketChannelClass())
                .childHandler(new ChannelInitializer<Channel>() {
                    @Override
                    public void initChannel(Channel ch) {
                        ch.pipeline().addLast(
                                new LinkDecoder(thisAddress, serverConnectionManager),
                                new PacketEncoder(),
                                new PacketDecoder(thisAddress),
                                new OperationHandler(packetDispatcher));
                    }
                })
                .childOption(ChannelOption.SO_RCVBUF, 128 * 1024)
                .childOption(ChannelOption.SO_SNDBUF, 128 * 1024)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true);
        serverBootstrap.bind(inetPort);


        // clientEventLoopGroup = new NioEventLoopGroup();
        clientBootstrap = new Bootstrap();

        clientBootstrap.group(workerGroup);
        clientBootstrap.channel(getChannelClass());
        clientBootstrap.handler(new ChannelInitializer<Channel>() {
            protected void initChannel(Channel ch) {
                ch.pipeline().addLast(
                        new LinkEncoder(thisAddress),
                        new PacketEncoder(),
                        new PacketDecoder(thisAddress),
                        new OperationHandler(packetDispatcher));
            }
        })      .option(ChannelOption.SO_RCVBUF, 128 * 1024)
                .option(ChannelOption.SO_SNDBUF, 128 * 1024)
                .option(ChannelOption.TCP_NODELAY, true);
    }

    @NotNull
    public Class<? extends ServerSocketChannel> getServerSocketChannelClass() {
        switch (mode){
            case NIO:
                return NioServerSocketChannel.class;
            case EPOLL:
                return EpollServerSocketChannel.class;
            case IO_URING:
                return IOUringServerSocketChannel.class;
            default:
                throw new RuntimeException();
        }
    }

    @NotNull
    public Class<? extends SocketChannel> getChannelClass() {
        switch (mode){
            case NIO:
                return NioSocketChannel.class;
            case EPOLL:
                return EpollSocketChannel.class;
            case IO_URING:
                return IOUringSocketChannel.class;
            default:
                throw new RuntimeException();
        }

    }

    @NotNull
    public MultithreadEventLoopGroup newWorkerGroup() {
        switch (mode){
            case NIO:
                return new NioEventLoopGroup(threadCount);
            case EPOLL:
                return  new EpollEventLoopGroup(threadCount);
            case IO_URING:
                return  new IOUringEventLoopGroup(threadCount);
            default:
                throw new RuntimeException();
        }
    }

    @NotNull
    public MultithreadEventLoopGroup newBossGroup() {
        switch (mode){
            case NIO:
                return new NioEventLoopGroup(threadCount);
            case EPOLL:
                return  new EpollEventLoopGroup(threadCount);
            case IO_URING:
                return  new IOUringEventLoopGroup(threadCount);
            default:
                throw new RuntimeException();
        }
    }

    public Channel connect(Address address) {
        if (address == null) {
            throw new RuntimeException("Address can't be null");
        }
        ChannelFuture future = clientBootstrap.connect(address.getHost(), address.getPort() + 10000);
        try {
            future.get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
        return future.channel();
    }

    public void shutdown() {
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }

        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
    }

}
