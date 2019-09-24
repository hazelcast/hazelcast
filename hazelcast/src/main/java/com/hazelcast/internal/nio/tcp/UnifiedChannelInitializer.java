/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.nio.tcp;

import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.ChannelInitializer;
import com.hazelcast.internal.networking.ChannelOptions;
import com.hazelcast.internal.nio.IOService;
import com.hazelcast.spi.properties.HazelcastProperties;

import static com.hazelcast.internal.networking.ChannelOption.DIRECT_BUF;
import static com.hazelcast.internal.networking.ChannelOption.SO_KEEPALIVE;
import static com.hazelcast.internal.networking.ChannelOption.SO_LINGER;
import static com.hazelcast.internal.networking.ChannelOption.SO_RCVBUF;
import static com.hazelcast.internal.networking.ChannelOption.SO_SNDBUF;
import static com.hazelcast.internal.networking.ChannelOption.TCP_NODELAY;
import static com.hazelcast.internal.nio.IOService.KILO_BYTE;
import static com.hazelcast.spi.properties.GroupProperty.SOCKET_BUFFER_DIRECT;
import static com.hazelcast.spi.properties.GroupProperty.SOCKET_KEEP_ALIVE;
import static com.hazelcast.spi.properties.GroupProperty.SOCKET_LINGER_SECONDS;
import static com.hazelcast.spi.properties.GroupProperty.SOCKET_NO_DELAY;
import static com.hazelcast.spi.properties.GroupProperty.SOCKET_RECEIVE_BUFFER_SIZE;
import static com.hazelcast.spi.properties.GroupProperty.SOCKET_SEND_BUFFER_SIZE;

/**
 * A {@link ChannelInitializer} that runs on a member and used for unencrypted
 * channels. It will deal with the exchange of protocols and based on that it
 * will set up the appropriate handlers in the pipeline.
 */
public class UnifiedChannelInitializer
        implements ChannelInitializer {

    private final IOService ioService;
    private final HazelcastProperties props;

    public UnifiedChannelInitializer(IOService ioService) {
        this.props = ioService.properties();
        this.ioService = ioService;
    }

    @Override
    public void initChannel(Channel channel) {
        ChannelOptions config = channel.options();
        config.setOption(DIRECT_BUF, props.getBoolean(SOCKET_BUFFER_DIRECT))
                .setOption(TCP_NODELAY, props.getBoolean(SOCKET_NO_DELAY))
                .setOption(SO_KEEPALIVE, props.getBoolean(SOCKET_KEEP_ALIVE))
                .setOption(SO_SNDBUF, props.getInteger(SOCKET_SEND_BUFFER_SIZE) * KILO_BYTE)
                .setOption(SO_RCVBUF, props.getInteger(SOCKET_RECEIVE_BUFFER_SIZE) * KILO_BYTE)
                .setOption(SO_LINGER, props.getSeconds(SOCKET_LINGER_SECONDS));

        UnifiedProtocolEncoder encoder = new UnifiedProtocolEncoder(ioService);
        UnifiedProtocolDecoder decoder = new UnifiedProtocolDecoder(ioService, encoder);

        channel.outboundPipeline().addLast(encoder);
        channel.inboundPipeline().addLast(decoder);
    }
}
