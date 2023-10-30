/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.codec.custom;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.Generated;
import com.hazelcast.client.impl.protocol.codec.builtin.*;

import static com.hazelcast.client.impl.protocol.codec.builtin.CodecUtil.fastForwardToEndFrame;
import static com.hazelcast.client.impl.protocol.ClientMessage.*;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.*;

@SuppressWarnings("unused")
@Generated("71353261fc182838cbaafeacd28c76d0")
public final class DiscoveryConfigCodec {

    private DiscoveryConfigCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.client.impl.protocol.codec.holder.DiscoveryConfigHolder discoveryConfig) {
        clientMessage.add(BEGIN_FRAME.copy());

        ListMultiFrameCodec.encode(clientMessage, discoveryConfig.getDiscoveryStrategyConfigs(), DiscoveryStrategyConfigCodec::encode);
        DataCodec.encode(clientMessage, discoveryConfig.getDiscoveryServiceProvider());
        DataCodec.encode(clientMessage, discoveryConfig.getNodeFilter());
        StringCodec.encode(clientMessage, discoveryConfig.getNodeFilterClass());

        clientMessage.add(END_FRAME.copy());
    }

    public static com.hazelcast.client.impl.protocol.codec.holder.DiscoveryConfigHolder decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        java.util.List<com.hazelcast.client.impl.protocol.codec.holder.DiscoveryStrategyConfigHolder> discoveryStrategyConfigs = ListMultiFrameCodec.decode(iterator, DiscoveryStrategyConfigCodec::decode);
        com.hazelcast.internal.serialization.Data discoveryServiceProvider = DataCodec.decode(iterator);
        com.hazelcast.internal.serialization.Data nodeFilter = DataCodec.decode(iterator);
        java.lang.String nodeFilterClass = StringCodec.decode(iterator);

        fastForwardToEndFrame(iterator);

        return new com.hazelcast.client.impl.protocol.codec.holder.DiscoveryConfigHolder(discoveryStrategyConfigs, discoveryServiceProvider, nodeFilter, nodeFilterClass);
    }
}
