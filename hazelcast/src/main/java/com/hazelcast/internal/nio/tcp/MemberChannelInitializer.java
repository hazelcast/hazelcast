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

import com.hazelcast.config.EndpointConfig;
import com.hazelcast.instance.ProtocolType;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.InboundHandler;
import com.hazelcast.internal.networking.OutboundHandler;
import com.hazelcast.internal.nio.IOService;

public class MemberChannelInitializer
        extends AbstractChannelInitializer  {

    MemberChannelInitializer(IOService ioService, EndpointConfig config) {
        super(ioService, config);
    }

    @Override
    public void initChannel(Channel channel) {
        TcpIpConnection connection = (TcpIpConnection) channel.attributeMap().get(TcpIpConnection.class);
        OutboundHandler[] outboundHandlers = ioService.createOutboundHandlers(EndpointQualifier.MEMBER, connection);
        InboundHandler[] inboundHandlers = ioService.createInboundHandlers(EndpointQualifier.MEMBER, connection);

        MemberProtocolEncoder protocolEncoder = new MemberProtocolEncoder(outboundHandlers);
        SingleProtocolDecoder protocolDecoder = new SingleProtocolDecoder(ProtocolType.MEMBER, inboundHandlers, protocolEncoder);

        channel.outboundPipeline().addLast(protocolEncoder);
        channel.inboundPipeline().addLast(protocolDecoder);
    }
}
