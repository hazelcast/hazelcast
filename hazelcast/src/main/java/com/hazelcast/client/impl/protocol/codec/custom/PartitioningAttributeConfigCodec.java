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
@Generated("1c53ae43d72216b937dac021779c1ca7")
public final class PartitioningAttributeConfigCodec {

    private PartitioningAttributeConfigCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.config.PartitioningAttributeConfig partitioningAttributeConfig) {
        clientMessage.add(BEGIN_FRAME.copy());

        StringCodec.encode(clientMessage, partitioningAttributeConfig.getAttributeName());

        clientMessage.add(END_FRAME.copy());
    }

    public static com.hazelcast.config.PartitioningAttributeConfig decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        java.lang.String attributeName = StringCodec.decode(iterator);

        fastForwardToEndFrame(iterator);

        return new com.hazelcast.config.PartitioningAttributeConfig(attributeName);
    }
}
