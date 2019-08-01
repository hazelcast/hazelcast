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

import com.hazelcast.cache.CacheEventType;
import com.hazelcast.cache.impl.CacheEventData;
import com.hazelcast.cache.impl.CacheEventDataImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.serialization.Data;

import java.util.ListIterator;

import static com.hazelcast.client.impl.protocol.ClientMessage.BEGIN_FRAME;
import static com.hazelcast.client.impl.protocol.ClientMessage.END_FRAME;
import static com.hazelcast.client.impl.protocol.codec.builtin.CodecUtil.decodeNullable;
import static com.hazelcast.client.impl.protocol.codec.builtin.CodecUtil.encodeNullable;
import static com.hazelcast.client.impl.protocol.codec.builtin.CodecUtil.fastForwardToEndFrame;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.decodeBoolean;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.decodeInt;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.encodeBoolean;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.encodeInt;

public final class CacheEventDataCodec {
    private static final int CACHE_EVENT_TYPE_OFFSET = 0;
    private static final int IS_OLD_VALUE_AVAILABLE_OFFSET = CACHE_EVENT_TYPE_OFFSET + Bits.INT_SIZE_IN_BYTES;
    private static final int INITIAL_FRAME_SIZE = IS_OLD_VALUE_AVAILABLE_OFFSET + Bits.BOOLEAN_SIZE_IN_BYTES;

    private CacheEventDataCodec() {
    }

    public static void encode(ClientMessage clientMessage, CacheEventData eventData) {
        clientMessage.add(BEGIN_FRAME);

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeInt(initialFrame.content, CACHE_EVENT_TYPE_OFFSET, eventData.getCacheEventType().getType());
        encodeBoolean(initialFrame.content, IS_OLD_VALUE_AVAILABLE_OFFSET, eventData.isOldValueAvailable());
        clientMessage.add(initialFrame);

        StringCodec.encode(clientMessage, eventData.getName());
        encodeNullable(clientMessage, eventData.getDataKey(), DataCodec::encode);
        encodeNullable(clientMessage, eventData.getDataValue(), DataCodec::encode);
        encodeNullable(clientMessage, eventData.getDataOldValue(), DataCodec::encode);

        clientMessage.add(END_FRAME);
    }

    public static CacheEventData decode(ListIterator<ClientMessage.Frame> iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        int cacheEventType = decodeInt(initialFrame.content, CACHE_EVENT_TYPE_OFFSET);
        boolean isOldValueAvailable = decodeBoolean(initialFrame.content, IS_OLD_VALUE_AVAILABLE_OFFSET);

        String name = StringCodec.decode(iterator);
        Data key = decodeNullable(iterator, DataCodec::decode);
        Data value = decodeNullable(iterator, DataCodec::decode);
        Data oldValue = decodeNullable(iterator, DataCodec::decode);

        fastForwardToEndFrame(iterator);

        return new CacheEventDataImpl(name, CacheEventType.getByType(cacheEventType), key, value, oldValue, isOldValueAvailable);
    }
}
