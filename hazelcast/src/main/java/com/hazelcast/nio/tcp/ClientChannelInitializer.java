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

package com.hazelcast.nio.tcp;

import com.hazelcast.client.impl.protocol.util.ClientMessageDecoder;
import com.hazelcast.client.impl.protocol.util.ClientMessageEncoder;
import com.hazelcast.config.EndpointConfig;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.nio.IOService;

import static com.hazelcast.instance.ProtocolType.CLIENT;

public class ClientChannelInitializer
        extends AbstractChannelInitializer {

    ClientChannelInitializer(IOService ioService, EndpointConfig config) {
        super(ioService, config);
    }

    @Override
    public void initChannel(Channel channel) {
        TcpIpConnection connection = (TcpIpConnection) channel.attributeMap().get(TcpIpConnection.class);
        SingleProtocolDecoder protocolDecoder = new SingleProtocolDecoder(CLIENT,
                new ClientMessageDecoder(connection, ioService.getClientEngine()));

        channel.outboundPipeline().addLast(new ClientMessageEncoder());
        channel.inboundPipeline().addLast(protocolDecoder);
    }
}
