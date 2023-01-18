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

@Generated("206b8e347744c430c57e80999247b4fb")
public final class BTreeIndexConfigCodec {

    private BTreeIndexConfigCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.config.BTreeIndexConfig bTreeIndexConfig) {
        clientMessage.add(BEGIN_FRAME.copy());

        CapacityCodec.encode(clientMessage, bTreeIndexConfig.getPageSize());
        MemoryTierConfigCodec.encode(clientMessage, bTreeIndexConfig.getMemoryTierConfig());

        clientMessage.add(END_FRAME.copy());
    }

    public static com.hazelcast.config.BTreeIndexConfig decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        com.hazelcast.memory.Capacity pageSize = CapacityCodec.decode(iterator);
        com.hazelcast.config.MemoryTierConfig memoryTierConfig = MemoryTierConfigCodec.decode(iterator);

        fastForwardToEndFrame(iterator);

        return CustomTypeFactory.createBTreeIndexConfig(pageSize, memoryTierConfig);
    }
}
