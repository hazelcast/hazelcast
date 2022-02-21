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
import com.hazelcast.internal.networking.ChannelInitializer;
import com.hazelcast.internal.server.ServerContext;

/**
 * A {@link ChannelInitializer} that runs on a member and used for unencrypted
 * channels. It will deal with the exchange of protocols and based on that it
 * will set up the appropriate handlers in the pipeline.
 */
public abstract class AbstractChannelInitializer implements ChannelInitializer {

    protected final ServerContext serverContext;
    private final EndpointConfig config;

    protected AbstractChannelInitializer(ServerContext serverContext, EndpointConfig config) {
        this.config = config;
        this.serverContext = serverContext;
    }
}
