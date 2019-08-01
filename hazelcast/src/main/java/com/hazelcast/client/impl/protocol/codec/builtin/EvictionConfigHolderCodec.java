/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.codec.builtin;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.EvictionConfigHolder;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.serialization.Data;

import java.util.ListIterator;

import static com.hazelcast.client.impl.protocol.ClientMessage.BEGIN_FRAME;
import static com.hazelcast.client.impl.protocol.ClientMessage.END_FRAME;
import static com.hazelcast.client.impl.protocol.codec.builtin.CodecUtil.decodeNullable;
import static com.hazelcast.client.impl.protocol.codec.builtin.CodecUtil.encodeNullable;
import static com.hazelcast.client.impl.protocol.codec.builtin.CodecUtil.fastForwardToEndFrame;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.decodeInt;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.encodeInt;

public final class EvictionConfigHolderCodec {
    private static final int SIZE_OFFSET = 0;
    private static final int INITIAL_FRAME_SIZE = SIZE_OFFSET + Bits.INT_SIZE_IN_BYTES;

    private EvictionConfigHolderCodec() {
    }

    public static void encode(ClientMessage clientMessage, EvictionConfigHolder configHolder) {
        clientMessage.add(BEGIN_FRAME);

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeInt(initialFrame.content, SIZE_OFFSET, configHolder.getSize());
        clientMessage.add(initialFrame);

        StringCodec.encode(clientMessage, configHolder.getMaxSizePolicy());
        StringCodec.encode(clientMessage, configHolder.getEvictionPolicy());
        encodeNullable(clientMessage, configHolder.getComparatorClassName(), StringCodec::encode);
        encodeNullable(clientMessage, configHolder.getComparator(), DataCodec::encode);

        clientMessage.add(END_FRAME);
    }

    public static EvictionConfigHolder decode(ListIterator<ClientMessage.Frame> iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        int size = decodeInt(initialFrame.content, SIZE_OFFSET);

        String maxSizePolicy = StringCodec.decode(iterator);
        String evictionPolicy = StringCodec.decode(iterator);
        String comparatorClassName = decodeNullable(iterator, StringCodec::decode);
        Data comparator = decodeNullable(iterator, DataCodec::decode);

        fastForwardToEndFrame(iterator);

        return new EvictionConfigHolder(size, maxSizePolicy, evictionPolicy, comparatorClassName, comparator);
    }
}
