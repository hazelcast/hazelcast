/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.server.ServerContext;
import com.hazelcast.internal.server.ServerConnection;
import com.hazelcast.internal.server.tcp.AbstractChannelInitializer;
import com.hazelcast.internal.server.tcp.SingleProtocolEncoder;
import com.hazelcast.internal.server.tcp.TcpServerConnection;
import com.hazelcast.internal.server.tcp.TextHandshakeDecoder;

public class TextChannelInitializer
        extends AbstractChannelInitializer {

    private final boolean rest;

    public TextChannelInitializer(ServerContext serverContext, EndpointConfig config, boolean rest) {
        super(serverContext, config);
        this.rest = rest;
    }

    @Override
    public void initChannel(Channel channel) {
        ServerConnection connection = (TcpServerConnection) channel.attributeMap().get(ServerConnection.class);
        SingleProtocolEncoder encoder = new SingleProtocolEncoder(new TextEncoder(connection));

        InboundHandler decoder = rest
                ? new RestApiTextDecoder(connection, (TextEncoder) encoder.getFirstOutboundHandler(), true)
                : new MemcacheTextDecoder(connection, (TextEncoder) encoder.getFirstOutboundHandler(), true);

        TextHandshakeDecoder handshaker = new TextHandshakeDecoder(rest ? ProtocolType.REST : ProtocolType.MEMCACHE,
                decoder, encoder);
        channel.outboundPipeline().addLast(encoder);
        channel.inboundPipeline().addLast(handshaker);
    }
}
