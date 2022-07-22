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

package com.hazelcast.client.impl.protocol.codec.custom;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.Generated;
import com.hazelcast.client.impl.protocol.codec.builtin.*;

import static com.hazelcast.client.impl.protocol.codec.builtin.CodecUtil.fastForwardToEndFrame;
import static com.hazelcast.client.impl.protocol.ClientMessage.*;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.*;

@Generated("e821dbe123d55d5e1f205014b5e02135")
public final class BTreeIndexConfigCodec {

    private BTreeIndexConfigCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.config.BTreeIndexConfig bTreeIndexConfig) {
        clientMessage.add(BEGIN_FRAME.copy());

        CodecUtil.encodeNullable(clientMessage, bTreeIndexConfig.getPageSize(), CapacityCodec::encode);
        CodecUtil.encodeNullable(clientMessage, bTreeIndexConfig.getMemoryTierConfig(), MemoryTierConfigCodec::encode);

        clientMessage.add(END_FRAME.copy());
    }

    public static com.hazelcast.config.BTreeIndexConfig decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        com.hazelcast.memory.Capacity pageSize = CodecUtil.decodeNullable(iterator, CapacityCodec::decode);
        com.hazelcast.config.MemoryTierConfig memoryTierConfig = CodecUtil.decodeNullable(iterator, MemoryTierConfigCodec::decode);

        fastForwardToEndFrame(iterator);

        return CustomTypeFactory.createBTreeIndexConfig(pageSize, memoryTierConfig);
    }
}
