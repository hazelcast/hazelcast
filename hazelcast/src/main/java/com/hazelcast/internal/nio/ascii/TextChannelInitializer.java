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

package com.hazelcast.internal.nio.ascii;

import com.hazelcast.config.EndpointConfig;
import com.hazelcast.instance.ProtocolType;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.InboundHandler;
import com.hazelcast.internal.nio.IOService;
import com.hazelcast.internal.nio.tcp.AbstractChannelInitializer;
import com.hazelcast.internal.nio.tcp.TcpIpConnection;
import com.hazelcast.internal.nio.tcp.TextHandshakeDecoder;

public class TextChannelInitializer
        extends AbstractChannelInitializer {

    private final boolean rest;

    public TextChannelInitializer(IOService ioService, EndpointConfig config, boolean rest) {
        super(ioService, config);
        this.rest = rest;
    }

    @Override
    public void initChannel(Channel channel) {
        TcpIpConnection connection = (TcpIpConnection) channel.attributeMap().get(TcpIpConnection.class);
        TextEncoder encoder = new TextEncoder(connection);

        InboundHandler decoder = rest
                ? new RestApiTextDecoder(connection, encoder, true)
                : new MemcacheTextDecoder(connection, encoder, true);

        TextHandshakeDecoder handshaker = new TextHandshakeDecoder(rest ? ProtocolType.REST : ProtocolType.MEMCACHE, decoder);
        channel.outboundPipeline().addLast(encoder);
        channel.inboundPipeline().addLast(handshaker);
    }
}
