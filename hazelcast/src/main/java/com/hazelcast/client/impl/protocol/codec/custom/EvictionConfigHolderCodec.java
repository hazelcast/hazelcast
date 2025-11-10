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
@Generated("96ad7b4ccedc7dc68d961c2216192109")
public final class EvictionConfigHolderCodec {
    private static final int SIZE_FIELD_OFFSET = 0;
    private static final int INITIAL_FRAME_SIZE = SIZE_FIELD_OFFSET + INT_SIZE_IN_BYTES;

    private EvictionConfigHolderCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.client.impl.protocol.task.dynamicconfig.EvictionConfigHolder evictionConfigHolder) {
        clientMessage.add(BEGIN_FRAME.copy());

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeInt(initialFrame.content, SIZE_FIELD_OFFSET, evictionConfigHolder.getSize());
        clientMessage.add(initialFrame);

        StringCodec.encode(clientMessage, evictionConfigHolder.getMaxSizePolicy());
        StringCodec.encode(clientMessage, evictionConfigHolder.getEvictionPolicy());
        CodecUtil.encodeNullable(clientMessage, evictionConfigHolder.getComparatorClassName(), StringCodec::encode);
        DataCodec.encodeNullable(clientMessage, evictionConfigHolder.getComparator());

        clientMessage.add(END_FRAME.copy());
    }

    public static com.hazelcast.client.impl.protocol.task.dynamicconfig.EvictionConfigHolder decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        int size = decodeInt(initialFrame.content, SIZE_FIELD_OFFSET);

        java.lang.String maxSizePolicy = StringCodec.decode(iterator);
        java.lang.String evictionPolicy = StringCodec.decode(iterator);
        java.lang.String comparatorClassName = CodecUtil.decodeNullable(iterator, StringCodec::decode);
        com.hazelcast.internal.serialization.Data comparator = DataCodec.decodeNullable(iterator);

        fastForwardToEndFrame(iterator);

        return new com.hazelcast.client.impl.protocol.task.dynamicconfig.EvictionConfigHolder(size, maxSizePolicy, evictionPolicy, comparatorClassName, comparator);
    }
}
