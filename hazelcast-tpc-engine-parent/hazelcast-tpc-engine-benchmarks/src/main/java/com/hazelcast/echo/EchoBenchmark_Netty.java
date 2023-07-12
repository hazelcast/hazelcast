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
import org.jctools.util.PaddedAtomicLong;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Make sure you add the following the JVM options, otherwise the selector will create
 * garbage:
 * <p>
 * --add-opens java.base/sun.nio.ch=ALL-UNNAMED
 */
public class EchoBenchmark_Netty {
    public static final int durationSeconds = 600;
    public static final int port = 5000;
    public static final int concurrency = 1;
    public static final Type type = Type.EPOLL;
    public static final String cpuAffinityClient = "1";
    public static final String cpuAffinityServer = "4";
    public static final int connections = 100;
    private static volatile boolean stop;

    public enum Type {
        NIO,
        EPOLL,
        IO_URING
    }

    private static CountDownLatch countDownLatch = new CountDownLatch(connections);

    public static void main(String[] args) throws InterruptedException {
        // needed so that Netty doesn't run into bad performance with a high number of connections
        System.setProperty("io.netty.iouring.iosqeAsyncThreshold", "16384");

        PaddedAtomicLong[] completedArray = new PaddedAtomicLong[connections];
        for (int k = 0; k < completedArray.length; k++) {
            completedArray[k] = new PaddedAtomicLong();
        }

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
        Channel[] channels = new Channel[connections];
        for (int k = 0; k < channels.length; k++) {
            Bootstrap clientBootstrap = new Bootstrap();
            ChannelFuture cf = clientBootstrap.group(clientWorkerGroup)
                    .channel(newSocketChannel(type))
                    .option(ChannelOption.TCP_NODELAY, true)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .handler(new EchoChannelInitializer(new EchoClientHandler(completedArray[k])))
                    .connect("127.0.0.1", port).sync();
            cf.await();
            channels[k] = cf.channel();
        }

        long start = System.currentTimeMillis();
        for (Channel channel : channels) {
            for (int k = 0; k < concurrency; k++) {
                ByteBuffer byteBuffer = ByteBuffer.allocate(8);
                byteBuffer.putLong(Long.MAX_VALUE);
                byteBuffer.flip();
                ByteBuf buf = Unpooled.wrappedBuffer(byteBuffer);
                channel.write(buf);
            }
        }
        for (Channel channel : channels) {
            channel.flush();
        }

        System.out.println("Starting with " + type);

        Monitor monitor = new Monitor(completedArray);
        monitor.start();

        countDownLatch.await();
        long count = sum(completedArray);
        long duration = System.currentTimeMillis() - start;
        System.out.println("Duration " + duration + " ms");
        System.out.println("Throughput:" + (count * 1000 / duration) + " ops");
        System.exit(0);
    }

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
        public Thread newThread(Runnable r) {
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

        public EchoClientHandler(PaddedAtomicLong completed) {
            this.completed = completed;
        }

        private final PaddedAtomicLong completed;

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object m) {
            if (stop) {
                countDownLatch.countDown();
                return;
            }

            ByteBuf receiveBuf = (ByteBuf) m;
            long round = receiveBuf.readLong();
            receiveBuf.release();

            completed.lazySet(completed.get() + 1);

            ByteBuf sendBuffer = ctx.alloc().buffer(8);
            sendBuffer.writeLong(round - 1);
            ctx.write(sendBuffer, ctx.voidPromise());
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

    private static long sum(PaddedAtomicLong[] array) {
        long sum = 0;
        for (PaddedAtomicLong a : array) {
            sum += a.get();
        }
        return sum;
    }

    private static class Monitor extends Thread {
        private final PaddedAtomicLong[] completedArray;
        private long last = 0;

        public Monitor(PaddedAtomicLong[] completedArray) {
            this.completedArray = completedArray;
        }

        @Override
        public void run() {
            long end = System.currentTimeMillis() + SECONDS.toMillis(durationSeconds);
            while (System.currentTimeMillis() < end) {
                try {
                    Thread.sleep(SECONDS.toMillis(1));
                } catch (InterruptedException e) {
                }

                long total = sum(completedArray);
                long diff = total - last;
                last = total;
                System.out.println("  thp " + diff + " echo/sec");
            }

            stop = true;
        }
    }
}
