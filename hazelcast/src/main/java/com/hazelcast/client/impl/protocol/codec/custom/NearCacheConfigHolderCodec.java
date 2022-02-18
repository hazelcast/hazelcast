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

@Generated("85cf70385b42ac1ba27cf96a7ec1e750")
public final class NearCacheConfigHolderCodec {
    private static final int SERIALIZE_KEYS_FIELD_OFFSET = 0;
    private static final int INVALIDATE_ON_CHANGE_FIELD_OFFSET = SERIALIZE_KEYS_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int TIME_TO_LIVE_SECONDS_FIELD_OFFSET = INVALIDATE_ON_CHANGE_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int MAX_IDLE_SECONDS_FIELD_OFFSET = TIME_TO_LIVE_SECONDS_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int CACHE_LOCAL_ENTRIES_FIELD_OFFSET = MAX_IDLE_SECONDS_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int INITIAL_FRAME_SIZE = CACHE_LOCAL_ENTRIES_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;

    private NearCacheConfigHolderCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.client.impl.protocol.task.dynamicconfig.NearCacheConfigHolder nearCacheConfigHolder) {
        clientMessage.add(BEGIN_FRAME.copy());

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeBoolean(initialFrame.content, SERIALIZE_KEYS_FIELD_OFFSET, nearCacheConfigHolder.isSerializeKeys());
        encodeBoolean(initialFrame.content, INVALIDATE_ON_CHANGE_FIELD_OFFSET, nearCacheConfigHolder.isInvalidateOnChange());
        encodeInt(initialFrame.content, TIME_TO_LIVE_SECONDS_FIELD_OFFSET, nearCacheConfigHolder.getTimeToLiveSeconds());
        encodeInt(initialFrame.content, MAX_IDLE_SECONDS_FIELD_OFFSET, nearCacheConfigHolder.getMaxIdleSeconds());
        encodeBoolean(initialFrame.content, CACHE_LOCAL_ENTRIES_FIELD_OFFSET, nearCacheConfigHolder.isCacheLocalEntries());
        clientMessage.add(initialFrame);

        StringCodec.encode(clientMessage, nearCacheConfigHolder.getName());
        StringCodec.encode(clientMessage, nearCacheConfigHolder.getInMemoryFormat());
        EvictionConfigHolderCodec.encode(clientMessage, nearCacheConfigHolder.getEvictionConfigHolder());
        StringCodec.encode(clientMessage, nearCacheConfigHolder.getLocalUpdatePolicy());
        CodecUtil.encodeNullable(clientMessage, nearCacheConfigHolder.getPreloaderConfig(), NearCachePreloaderConfigCodec::encode);

        clientMessage.add(END_FRAME.copy());
    }

    public static com.hazelcast.client.impl.protocol.task.dynamicconfig.NearCacheConfigHolder decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        boolean serializeKeys = decodeBoolean(initialFrame.content, SERIALIZE_KEYS_FIELD_OFFSET);
        boolean invalidateOnChange = decodeBoolean(initialFrame.content, INVALIDATE_ON_CHANGE_FIELD_OFFSET);
        int timeToLiveSeconds = decodeInt(initialFrame.content, TIME_TO_LIVE_SECONDS_FIELD_OFFSET);
        int maxIdleSeconds = decodeInt(initialFrame.content, MAX_IDLE_SECONDS_FIELD_OFFSET);
        boolean cacheLocalEntries = decodeBoolean(initialFrame.content, CACHE_LOCAL_ENTRIES_FIELD_OFFSET);

        java.lang.String name = StringCodec.decode(iterator);
        java.lang.String inMemoryFormat = StringCodec.decode(iterator);
        com.hazelcast.client.impl.protocol.task.dynamicconfig.EvictionConfigHolder evictionConfigHolder = EvictionConfigHolderCodec.decode(iterator);
        java.lang.String localUpdatePolicy = StringCodec.decode(iterator);
        com.hazelcast.config.NearCachePreloaderConfig preloaderConfig = CodecUtil.decodeNullable(iterator, NearCachePreloaderConfigCodec::decode);

        fastForwardToEndFrame(iterator);

        return new com.hazelcast.client.impl.protocol.task.dynamicconfig.NearCacheConfigHolder(name, inMemoryFormat, serializeKeys, invalidateOnChange, timeToLiveSeconds, maxIdleSeconds, evictionConfigHolder, cacheLocalEntries, localUpdatePolicy, preloaderConfig);
    }
}
