/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.ChannelInitializer;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.IOService;

public class TcpIpChannelInitializer implements ChannelInitializer {
    private final ILogger logger;
    private final IOService ioService;

    public TcpIpChannelInitializer(ILogger logger, IOService ioService) {
        this.logger = logger;
        this.ioService = ioService;
    }

    @Override
    public void initChannel(Channel channel) {
        channel.addLast(new ProtocolDecoder(logger, ioService));
        channel.addLast(new OffloadingProtocolEncoder(logger, ioService));
    }
}
