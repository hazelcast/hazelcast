package com.hazelcast.echo;

import com.hazelcast.internal.util.ThreadAffinity;
import com.hazelcast.internal.util.ThreadAffinityHelper;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.incubator.channel.uring.IOUringEventLoopGroup;
import io.netty.incubator.channel.uring.IOUringServerSocketChannel;
import io.netty.incubator.channel.uring.IOUringSocketChannel;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;


public class EchoBenchmark_Netty {

    public static final int port = 5000;
    public static final int concurrency = 1;
    public static final long iterations = 4_000_000;
    public static final Type type = Type.EPOLL;
    public static final String cpuAffinityClient = "1";
    public static final String cpuAffinityServer = "4";

    public enum Type {
        NIO,
        EPOLL,
        IO_URING
    }

    public static CountDownLatch countDownLatch = new CountDownLatch(concurrency);

    public static void main(String[] args) throws InterruptedException {
        EventLoopGroup bossGroup = newEventloopGroup(type, null);
        EventLoopGroup serverWorkerGroup = newEventloopGroup(type, cpuAffinityServer);

        ServerBootstrap serverBootstrap = new ServerBootstrap();
        ChannelFuture sf = serverBootstrap.group(bossGroup, serverWorkerGroup)
                .channel(newServerChannel(type))
                .childHandler(new EchoChannelInitializer(new EchoServerHandler()))
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .bind(port).sync();
        sf.await();
        sf.channel();

        EventLoopGroup clientWorkerGroup = newEventloopGroup(type, cpuAffinityClient);
        Bootstrap clientBootstrap = new Bootstrap();
        ChannelFuture cf = clientBootstrap.group(clientWorkerGroup)
                .channel(newSocketChannel(type))
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(new EchoChannelInitializer(new EchoClientHandler()))
                .connect("127.0.0.1", port).sync();
        cf.await();

        long start = System.currentTimeMillis();
        Channel channel = cf.channel();
        for (int k = 0; k < concurrency; k++) {
            ByteBuffer byteBuffer = ByteBuffer.allocate(8);
            byteBuffer.putLong(iterations / concurrency);
            byteBuffer.flip();
            ByteBuf buf = Unpooled.wrappedBuffer(byteBuffer);
            channel.write(buf);
        }
        System.out.println("Starting with " + type);
        channel.flush();
        countDownLatch.await();
        long duration = System.currentTimeMillis() - start;
        System.out.println("Duration " + duration + " ms");
        System.out.println("Throughput:" + (iterations * 1000 / duration) + " ops");
        System.exit(0);
    }

    @NotNull
    private static Class<? extends Channel> newSocketChannel(Type type) {
        switch (type) {
            case NIO:
                return NioSocketChannel.class;
            case IO_URING:
                return IOUringSocketChannel.class;
            case EPOLL:
                return EpollSocketChannel.class;
            default:
                throw new RuntimeException();
        }
    }

    @NotNull
    private static Class<? extends ServerChannel> newServerChannel(Type type) {
        switch (type) {
            case NIO:
                return NioServerSocketChannel.class;
            case IO_URING:
                return IOUringServerSocketChannel.class;
            case EPOLL:
                return EpollServerSocketChannel.class;
            default:
                throw new RuntimeException();
        }
    }

    @NotNull
    private static EventLoopGroup newEventloopGroup(Type type, String affinity) {
        switch (type) {
            case NIO:
                return new NioEventLoopGroup(1, new NettyThreadFactory(affinity));
            case IO_URING:
                return new IOUringEventLoopGroup(1, new NettyThreadFactory(affinity));
            case EPOLL:
                return new EpollEventLoopGroup(1, new NettyThreadFactory(affinity));
            default:
                throw new RuntimeException();
        }
    }

    private static class NettyThreadFactory implements ThreadFactory {
        private final String affinity;

        public NettyThreadFactory(String affinity) {
            this.affinity = affinity;
        }

        @Override
        public Thread newThread(@NotNull Runnable r) {
            Runnable task = () -> {
                ThreadAffinity threadAffinity = affinity == null ? null : new ThreadAffinity(affinity);
                if (threadAffinity != null) {
                    System.out.println("Setting affinity " + affinity);
                    ThreadAffinityHelper.setAffinity(threadAffinity.nextAllowedCpus());
                }
                r.run();
            };
            return new Thread(task);
        }
    }

    @ChannelHandler.Sharable
    static class EchoServerHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
//            ByteBuf receiveBuf = (ByteBuf) m;
//            long round = receiveBuf.readLong();
//            receiveBuf.release();
//            ByteBuf sendBuffer = ctx.alloc().buffer(8);
//            sendBuffer.writeLong(round);
//            ctx.write(sendBuffer);
            ctx.write(msg, ctx.voidPromise());
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) {
            ctx.flush();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            ctx.close();
        }
    }

    static class EchoClientHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object m) {
            ByteBuf receiveBuf = (ByteBuf) m;
            long round = receiveBuf.readLong();
            receiveBuf.release();
            if (round == 0) {
                countDownLatch.countDown();
            } else {
                ByteBuf sendBuffer = ctx.alloc().buffer(8);
                sendBuffer.writeLong(round - 1);
                ctx.write(sendBuffer, ctx.voidPromise());
            }
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) {
            ctx.flush();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            ctx.close();
        }
    }

    static class EchoChannelInitializer extends ChannelInitializer<SocketChannel> {

        private ChannelHandler handler;

        public EchoChannelInitializer(ChannelHandler handler) {
            this.handler = handler;
        }

        @Override
        public void initChannel(SocketChannel ch) {
            ch.pipeline().addLast(handler);
        }
    }
}
