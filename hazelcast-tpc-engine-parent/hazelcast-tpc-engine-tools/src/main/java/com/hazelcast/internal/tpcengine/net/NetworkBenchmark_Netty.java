/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.tpcengine.net;

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

import static com.hazelcast.internal.tpcengine.FormatUtil.humanReadableCountSI;
import static com.hazelcast.internal.tpcengine.util.BitUtil.SIZEOF_INT;
import static java.lang.Math.min;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Make sure you add the following the JVM options, otherwise the selector will create
 * garbage:
 * <p>
 * --add-opens java.base/sun.nio.ch=ALL-UNNAMED
 */
public class NetworkBenchmark_Netty {
    public enum Type {
        NIO,
        EPOLL,
        IO_URING
    }

    public int runtimeSeconds = 30;
    public int payloadSize = 0;
    // the number of inflight packets per connection
    public int concurrency = 1;
    public int connections = 1;
    public Type type = Type.NIO;
    public String cpuAffinityClient = "1,2";
    public String cpuAffinityServer = "5,6";
    public int socketBufferSize = 256 * 1024;
    public int clientThreadCount = 2;
    public int serverThreadCount = 2;
    public boolean tcpNoDelay = true;
    public int port = 5000;

    private volatile boolean stop;
    private final CountDownLatch countDownLatch = new CountDownLatch(connections);

    public static void main(String[] args) throws InterruptedException {
        NetworkBenchmark_Netty benchmark = new NetworkBenchmark_Netty();
        benchmark.run();
    }

    private void run() throws InterruptedException {
        // needed so that Netty doesn't run into bad performance with a high number of connections
        System.setProperty("io.netty.iouring.iosqeAsyncThreshold", "16384");

        printConfig();

        PaddedAtomicLong[] completedArray = new PaddedAtomicLong[connections];
        for (int k = 0; k < completedArray.length; k++) {
            completedArray[k] = new PaddedAtomicLong();
        }

        EventLoopGroup bossGroup = newEventloopGroup(type, null, serverThreadCount);
        EventLoopGroup serverWorkerGroup = newEventloopGroup(type, cpuAffinityServer, serverThreadCount);

        ServerBootstrap serverBootstrap = new ServerBootstrap();
        ChannelFuture sf = serverBootstrap.group(bossGroup, serverWorkerGroup)
                .channel(newServerChannel(type))
                .childHandler(new EchoChannelInitializer(new EchoServerHandler()))
                .option(ChannelOption.SO_BACKLOG, 128)
                .option(ChannelOption.SO_RCVBUF, socketBufferSize)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.TCP_NODELAY, tcpNoDelay)
                .childOption(ChannelOption.SO_RCVBUF, socketBufferSize)
                .childOption(ChannelOption.SO_SNDBUF, socketBufferSize)
                .bind(port)
                .sync();
        sf.await();
        sf.channel();

        EventLoopGroup clientWorkerGroup = newEventloopGroup(type, cpuAffinityClient, clientThreadCount);
        Channel[] channels = new Channel[connections];
        for (int channelIdx = 0; channelIdx < channels.length; channelIdx++) {
            Bootstrap clientBootstrap = new Bootstrap();
            ChannelFuture cf = clientBootstrap.group(clientWorkerGroup)
                    .channel(newSocketChannel(type))
                    .option(ChannelOption.TCP_NODELAY, tcpNoDelay)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .handler(new EchoChannelInitializer(new EchoClientHandler(completedArray[channelIdx])))
                    .connect("127.0.0.1", port).sync();
            cf.await();
            channels[channelIdx] = cf.channel();
        }

        byte[] bogusPayload = new byte[payloadSize];
        long start = currentTimeMillis();
        for (Channel channel : channels) {
            for (int k = 0; k < concurrency; k++) {
                ByteBuffer byteBuffer = ByteBuffer.allocate(SIZEOF_INT + payloadSize);
                byteBuffer.putInt(payloadSize);
                byteBuffer.put(bogusPayload);
                byteBuffer.flip();
                ByteBuf buf = Unpooled.wrappedBuffer(byteBuffer);
                channel.write(buf);
            }

            channel.flush();
        }

        MonitorThread monitor = new MonitorThread(completedArray);
        monitor.start();

        countDownLatch.await();
        printResults(completedArray, start);

        for(Channel channel:channels){
            channel.close();
        }

        System.out.println("Sleeping");
        Thread.sleep(10000000);
    }

    private void printConfig() {
        System.out.println("runtimeSeconds:" + runtimeSeconds);
        System.out.println("payloadSize:" + payloadSize);
        System.out.println("concurrency:" + concurrency);
        System.out.println("connections:" + connections);
        System.out.println("type:" + type);
        System.out.println("cpuAffinityClient:" + cpuAffinityClient);
        System.out.println("cpuAffinityServer:" + cpuAffinityServer);
        System.out.println("clientThreadCount:" + clientThreadCount);
        System.out.println("serverThreadCount:" + serverThreadCount);
        System.out.println("socketBufferSize:" + socketBufferSize);
        System.out.println("tcpNoDelay:" + tcpNoDelay);
        System.out.println("port:" + port);
    }

    private static void printResults(PaddedAtomicLong[] completedArray, long start) {
        long count = sum(completedArray);
        long duration = currentTimeMillis() - start;
        System.out.println("Duration " + duration + " ms");
        System.out.println("Throughput:" + (count * 1000 / duration) + " ops");
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

    private static EventLoopGroup newEventloopGroup(Type type, String affinity, int threadCount) {
        switch (type) {
            case NIO:
                return new NioEventLoopGroup(threadCount, new NettyThreadFactory(affinity));
            case IO_URING:
                return new IOUringEventLoopGroup(threadCount, new NettyThreadFactory(affinity));
            case EPOLL:
                return new EpollEventLoopGroup(threadCount, new NettyThreadFactory(affinity));
            default:
                throw new RuntimeException();
        }
    }

    private static class NettyThreadFactory implements ThreadFactory {
        private final String affinity;
        private final ThreadAffinity threadAffinity;

        public NettyThreadFactory(String affinity) {
            this.affinity = affinity;
            this.threadAffinity = affinity == null ? null : new ThreadAffinity(affinity);
        }

        @Override
        public Thread newThread(Runnable r) {
            Runnable task = () -> {
                if (threadAffinity != null) {
                    //todo: affinity needs work
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

    class EchoClientHandler extends ChannelInboundHandlerAdapter {

        private int payloadSize = -1;
        private ByteBuf response;

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
            for (; ; ) {
                if (payloadSize == -1) {
                    if (receiveBuf.readableBytes() < SIZEOF_INT) {
                        break;
                    }

                    payloadSize = receiveBuf.readInt();
                    int responseBufSize = SIZEOF_INT + payloadSize;
                    response = ctx.alloc().buffer(responseBufSize, responseBufSize);
                    response.writeInt(payloadSize);
                }

                response.writeBytes(receiveBuf, min(response.writableBytes(), receiveBuf.readableBytes()));

                if (response.writableBytes() > 0) {
                    // not all bytes have been received.
                    break;
                }

                completed.lazySet(completed.get() + 1);
                ctx.write(response, ctx.voidPromise());
                payloadSize = -1;
                response = null;
            }
            receiveBuf.release();
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

    private class MonitorThread extends Thread {
        private final PaddedAtomicLong[] completedArray;

        public MonitorThread(PaddedAtomicLong[] completedArray) {
            super("MonitorThread");
            this.completedArray = completedArray;
        }

        @Override
        public void run() {
            try {
                run0();
            } catch (Throwable e) {
                e.printStackTrace();
            }

            stop = true;
        }

        private void run0() throws InterruptedException {
            long runtimeMs = SECONDS.toMillis(runtimeSeconds);
            long startMs = currentTimeMillis();
            long endMs = startMs + runtimeMs;
            long lastMs = startMs;
            StringBuffer sb = new StringBuffer();
            long last = 0;
            while (currentTimeMillis() < endMs) {
                Thread.sleep(SECONDS.toMillis(1));
                long nowMs = System.currentTimeMillis();

                long completedSeconds = MILLISECONDS.toSeconds(nowMs - startMs);
                double completed = (100f * completedSeconds) / runtimeSeconds;
                sb.append("[etd ");
                sb.append(completedSeconds / 60);
                sb.append("m:");
                sb.append(completedSeconds % 60);
                sb.append("s ");
                sb.append(String.format("%,.3f", completed));
                sb.append("%]");

                long eta = MILLISECONDS.toSeconds(endMs - nowMs);
                sb.append("[eta ");
                sb.append(eta / 60);
                sb.append("m:");
                sb.append(eta % 60);
                sb.append("s]");

                long total = sum(completedArray);
                long diff = total - last;
                last = total;
                sb.append("[thp=");
                sb.append(humanReadableCountSI(diff));
                sb.append("/s]");

                System.out.println(sb);
                sb.setLength(0);
            }
        }
    }
}
