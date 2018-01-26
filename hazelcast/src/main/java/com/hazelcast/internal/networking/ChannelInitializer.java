/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.networking;

import java.io.IOException;

/**
 * Initializes the Channel when the Channel is used for the first time.
 */
public interface ChannelInitializer {

    /**
     * Called when the Channel receives the first data.
     *
     * For the given channel, there will only be a single thread calling this method. But it could be that it is called
     * concurrently for the same channel with {@link #initOutbound(Channel)}.
     *
     * @param channel the {@link Channel} that requires initialization
     * @return the result of the initialization. Returned value could be null if not enough data is available to determine how
     * to init (e.g. protocol info lacking). This will probably change once the TLS integration is done through the pipeline.
     */
    InitResult<ChannelInboundHandler> initInbound(Channel channel) throws IOException;

    /***
     * Called when the Channel writes the first data.
     *
     * For the given channel, there will only be a single thread calling this method. But it could be that it is called
     * concurrently for the same channel with {@link #initInbound(Channel)}.
     *
     * @param channel the {@link Channel} that requires initialization
     * @return the result of the initialization.
     */
    InitResult<ChannelOutboundHandler> initOutbound(Channel channel);
}
