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

package com.hazelcast.internal.server.tcp;

import com.hazelcast.config.EndpointConfig;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.ProtocolType;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.InboundHandler;
import com.hazelcast.internal.networking.OutboundHandler;
import com.hazelcast.internal.server.ServerContext;
import com.hazelcast.internal.server.ServerConnection;

public class MemberChannelInitializer
        extends AbstractChannelInitializer {

    MemberChannelInitializer(ServerContext serverContext, EndpointConfig config) {
        super(serverContext, config);
    }

    @Override
    public void initChannel(Channel channel) {
        ServerConnection connection = (TcpServerConnection) channel.attributeMap().get(ServerConnection.class);
        OutboundHandler[] outboundHandlers = serverContext.createOutboundHandlers(EndpointQualifier.MEMBER, connection);
        InboundHandler[] inboundHandlers = serverContext.createInboundHandlers(EndpointQualifier.MEMBER, connection);

        OutboundHandler outboundHandler;
        SingleProtocolEncoder protocolEncoder;
        if (channel.isClientMode()) {
            protocolEncoder = new SingleProtocolEncoder(outboundHandlers);
            outboundHandler = new MemberProtocolEncoder(protocolEncoder);
        } else {
            protocolEncoder = new SingleProtocolEncoder(new MemberProtocolEncoder(outboundHandlers));
            outboundHandler = protocolEncoder;
        }
        SingleProtocolDecoder protocolDecoder = new SingleProtocolDecoder(ProtocolType.MEMBER,
                inboundHandlers, protocolEncoder);

        channel.outboundPipeline().addLast(outboundHandler);
        channel.inboundPipeline().addLast(protocolDecoder);
    }
}
