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

@Generated("5fc072d3a3c1bf2eea3c3361ed7ab313")
public final class CacheEventDataCodec {
    private static final int CACHE_EVENT_TYPE_FIELD_OFFSET = 0;
    private static final int OLD_VALUE_AVAILABLE_FIELD_OFFSET = CACHE_EVENT_TYPE_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int INITIAL_FRAME_SIZE = OLD_VALUE_AVAILABLE_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;

    private CacheEventDataCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.cache.impl.CacheEventData cacheEventData) {
        clientMessage.add(BEGIN_FRAME.copy());

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeInt(initialFrame.content, CACHE_EVENT_TYPE_FIELD_OFFSET, cacheEventData.getCacheEventType());
        encodeBoolean(initialFrame.content, OLD_VALUE_AVAILABLE_FIELD_OFFSET, cacheEventData.isOldValueAvailable());
        clientMessage.add(initialFrame);

        StringCodec.encode(clientMessage, cacheEventData.getName());
        CodecUtil.encodeNullable(clientMessage, cacheEventData.getDataKey(), DataCodec::encode);
        CodecUtil.encodeNullable(clientMessage, cacheEventData.getDataValue(), DataCodec::encode);
        CodecUtil.encodeNullable(clientMessage, cacheEventData.getDataOldValue(), DataCodec::encode);

        clientMessage.add(END_FRAME.copy());
    }

    public static com.hazelcast.cache.impl.CacheEventDataImpl decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        int cacheEventType = decodeInt(initialFrame.content, CACHE_EVENT_TYPE_FIELD_OFFSET);
        boolean oldValueAvailable = decodeBoolean(initialFrame.content, OLD_VALUE_AVAILABLE_FIELD_OFFSET);

        java.lang.String name = StringCodec.decode(iterator);
        com.hazelcast.internal.serialization.Data dataKey = CodecUtil.decodeNullable(iterator, DataCodec::decode);
        com.hazelcast.internal.serialization.Data dataValue = CodecUtil.decodeNullable(iterator, DataCodec::decode);
        com.hazelcast.internal.serialization.Data dataOldValue = CodecUtil.decodeNullable(iterator, DataCodec::decode);

        fastForwardToEndFrame(iterator);

        return CustomTypeFactory.createCacheEventData(name, cacheEventType, dataKey, dataValue, dataOldValue, oldValueAvailable);
    }
}
