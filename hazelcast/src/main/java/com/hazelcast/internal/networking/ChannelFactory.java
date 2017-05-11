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

import java.nio.channels.SocketChannel;

/**
 * A factory for creating {@link Channel} instances.
 *
 * Currently this interface is suboptimal. It imposes a SocketChanel as a basis. Probably once the TLS is integrated
 * directly in the pipeline of the Channel, this whole factory can be dropped. TLS can then be added through a
 * ChannelInboundHandler/ChannelOutboundHandler.
 */
public interface ChannelFactory {

    /**
     * Creates the Channel.
     */
    Channel create(SocketChannel channel, boolean clientMode, boolean directBuffer) throws Exception;
}
