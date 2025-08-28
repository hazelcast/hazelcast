/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
@Generated("d429ba09fd15342c7a939e34c9b5718e")
public final class WanConsumerConfigHolderCodec {
    private static final int PERSIST_WAN_REPLICATED_DATA_FIELD_OFFSET = 0;
    private static final int INITIAL_FRAME_SIZE = PERSIST_WAN_REPLICATED_DATA_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;

    private WanConsumerConfigHolderCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.client.impl.protocol.codec.holder.WanConsumerConfigHolder wanConsumerConfigHolder) {
        clientMessage.add(BEGIN_FRAME.copy());

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeBoolean(initialFrame.content, PERSIST_WAN_REPLICATED_DATA_FIELD_OFFSET, wanConsumerConfigHolder.isPersistWanReplicatedData());
        clientMessage.add(initialFrame);

        CodecUtil.encodeNullable(clientMessage, wanConsumerConfigHolder.getClassName(), StringCodec::encode);
        DataCodec.encodeNullable(clientMessage, wanConsumerConfigHolder.getImplementation());
        MapCodec.encode(clientMessage, wanConsumerConfigHolder.getProperties(), StringCodec::encode, DataCodec::encode);

        clientMessage.add(END_FRAME.copy());
    }

    public static com.hazelcast.client.impl.protocol.codec.holder.WanConsumerConfigHolder decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        boolean persistWanReplicatedData = decodeBoolean(initialFrame.content, PERSIST_WAN_REPLICATED_DATA_FIELD_OFFSET);

        java.lang.String className = CodecUtil.decodeNullable(iterator, StringCodec::decode);
        com.hazelcast.internal.serialization.Data implementation = DataCodec.decodeNullable(iterator);
        java.util.Map<java.lang.String, com.hazelcast.internal.serialization.Data> properties = MapCodec.decode(iterator, StringCodec::decode, DataCodec::decode);

        fastForwardToEndFrame(iterator);

        return new com.hazelcast.client.impl.protocol.codec.holder.WanConsumerConfigHolder(persistWanReplicatedData, className, implementation, properties);
    }
}
