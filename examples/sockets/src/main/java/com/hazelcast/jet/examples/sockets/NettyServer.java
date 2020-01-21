/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.examples.sockets;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;

import java.util.function.Consumer;

/**
 * A server instance which accepts a port number to connect,
 * a consumer to be used for each channel activated and
 * a consumer to be used for each message read.
 */
class NettyServer {

    private final int port;
    private final Consumer<Channel> channelActiveHandler;
    private final Consumer<Object> messageConsumer;

    private EventLoopGroup parentGroup;
    private EventLoopGroup childGroup;
    private Channel channel;

    NettyServer(int port, Consumer<Channel> channelActiveHandler, Consumer<Object> messageConsumer) {
        this.port = port;
        this.channelActiveHandler = channelActiveHandler;
        this.messageConsumer = messageConsumer;
    }

    void start() throws Exception {
        parentGroup = new NioEventLoopGroup(1);
        childGroup = new NioEventLoopGroup();

        ServerBootstrap bootstrap = new ServerBootstrap()
                .group(parentGroup, childGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override protected void initChannel(SocketChannel channel) throws Exception {
                        ChannelPipeline pipeline = channel.pipeline();

                        pipeline.addLast("lineFrame", new LineBasedFrameDecoder(256));
                        pipeline.addLast("decoder", new StringDecoder());
                        pipeline.addLast("encoder", new StringEncoder());

                        pipeline.addLast("handler", new ChannelInboundHandlerAdapter() {
                            @Override
                            public void channelActive(ChannelHandlerContext ctx) throws Exception {
                                Channel channel = ctx.channel();
                                channelActiveHandler.accept(channel);
                            }

                            @Override
                            public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                                messageConsumer.accept(msg);
                            }
                        });
                    }
                });
        channel = bootstrap.bind(port).channel();
        System.out.println("NettyServer started");
    }

    void stop() throws Exception {
        channel.close().get();
        parentGroup.shutdownGracefully().get();
        childGroup.shutdownGracefully().get();
        System.out.println("NettyServer closed");
    }

}

