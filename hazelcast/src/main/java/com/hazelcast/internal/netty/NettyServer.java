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
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

public class NettyServer {

    private final Address thisAddress;
    private final Consumer<Packet> packetDispatcher;
    private EpollEventLoopGroup bossGroup;
    private EpollEventLoopGroup workerGroup;
    private ServerBootstrap serverBootstrap;
    private Bootstrap clientBootstrap;
    //   private NioEventLoopGroup clientEventLoopGroup;
    private ServerConnectionManager serverConnectionManager;
    private int threadCount = 8;

    public NettyServer(Address thisAddress, Consumer<Packet> packetDispatcher) {
        this.thisAddress = thisAddress;
        this.packetDispatcher = packetDispatcher;
    }

    public void setServerConnectionManager(ServerConnectionManager serverConnectionManager) {
        this.serverConnectionManager = serverConnectionManager;
    }

    public void start() {
        System.out.println("Started ");
        int inetPort = thisAddress.getPort() + 10000;
        System.out.println("Started netty server on " + (thisAddress.getHost() + " " + inetPort));

        bossGroup = new EpollEventLoopGroup();
        workerGroup = new EpollEventLoopGroup(threadCount);

        serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(bossGroup, workerGroup)
                .channel(EpollServerSocketChannel.class)
                .childHandler(new ChannelInitializer<Channel>() {
                    @Override
                    public void initChannel(Channel ch) {
                        ch.pipeline().addLast(
                                new AddressDecoder(thisAddress, serverConnectionManager),
                                new PacketEncoder(),
                                new PacketDecoder(thisAddress),
                                new OperationHandler(packetDispatcher));
                    }
                })
                .childOption(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.SO_RCVBUF, 128 * 1024)
                .childOption(ChannelOption.SO_SNDBUF, 128 * 1024)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true);
        serverBootstrap.bind(inetPort);


        // clientEventLoopGroup = new NioEventLoopGroup();
        clientBootstrap = new Bootstrap();

        clientBootstrap.group(workerGroup);
        clientBootstrap.channel(EpollSocketChannel.class);
        clientBootstrap.handler(new ChannelInitializer<Channel>() {
            protected void initChannel(Channel ch) {
                ch.pipeline().addLast(
                        new AddressEncoder(thisAddress),
                        new PacketEncoder(),
                        new PacketDecoder(thisAddress),
                        new OperationHandler(packetDispatcher));
            }
        }).option(ChannelOption.SO_BACKLOG, 128)
                .option(ChannelOption.SO_RCVBUF, 128 * 1024)
                .option(ChannelOption.SO_SNDBUF, 128 * 1024)
                .option(ChannelOption.TCP_NODELAY, true);

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

//        if (clientBootstrap != null) {
//            clientEventLoopGroup.shutdownGracefully();
//        }
    }

}
