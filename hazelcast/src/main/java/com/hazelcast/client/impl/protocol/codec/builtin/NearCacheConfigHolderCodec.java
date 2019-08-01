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
import com.hazelcast.client.impl.protocol.task.dynamicconfig.NearCacheConfigHolder;
import com.hazelcast.config.NearCachePreloaderConfig;
import com.hazelcast.nio.Bits;

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

public final class NearCacheConfigHolderCodec {
    private static final int SERIALIZE_KEYS_OFFSET = 0;
    private static final int INVALIDATE_ON_CHANGE_OFFSET = SERIALIZE_KEYS_OFFSET + Bits.BOOLEAN_SIZE_IN_BYTES;
    private static final int TIME_TO_LIVE_SECONDS_OFFSET = INVALIDATE_ON_CHANGE_OFFSET + Bits.BOOLEAN_SIZE_IN_BYTES;
    private static final int MAX_IDLE_SECONDS_OFFSET = TIME_TO_LIVE_SECONDS_OFFSET + Bits.INT_SIZE_IN_BYTES;
    private static final int CACHE_LOCAL_ENTRIES_OFFSET = MAX_IDLE_SECONDS_OFFSET + Bits.INT_SIZE_IN_BYTES;
    private static final int INITIAL_FRAME_SIZE = CACHE_LOCAL_ENTRIES_OFFSET + Bits.BOOLEAN_SIZE_IN_BYTES;

    private NearCacheConfigHolderCodec() {
    }

    public static void encode(ClientMessage clientMessage, NearCacheConfigHolder configHolder) {
        clientMessage.add(BEGIN_FRAME);

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeBoolean(initialFrame.content, SERIALIZE_KEYS_OFFSET, configHolder.isSerializeKeys());
        encodeBoolean(initialFrame.content, INVALIDATE_ON_CHANGE_OFFSET, configHolder.isInvalidateOnChange());
        encodeInt(initialFrame.content, TIME_TO_LIVE_SECONDS_OFFSET, configHolder.getTimeToLiveSeconds());
        encodeInt(initialFrame.content, MAX_IDLE_SECONDS_OFFSET, configHolder.getMaxIdleSeconds());
        encodeBoolean(initialFrame.content, CACHE_LOCAL_ENTRIES_OFFSET, configHolder.isCacheLocalEntries());
        clientMessage.add(initialFrame);

        StringCodec.encode(clientMessage, configHolder.getName());
        StringCodec.encode(clientMessage, configHolder.getInMemoryFormat());
        EvictionConfigHolderCodec.encode(clientMessage, configHolder.getEvictionConfigHolder());
        StringCodec.encode(clientMessage, configHolder.getLocalUpdatePolicy());
        encodeNullable(clientMessage, configHolder.getPreloaderConfig(), NearCachePreloaderConfigCodec::encode);

        clientMessage.add(END_FRAME);
    }

    public static NearCacheConfigHolder decode(ListIterator<ClientMessage.Frame> iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        boolean serializeKeys = decodeBoolean(initialFrame.content, SERIALIZE_KEYS_OFFSET);
        boolean invalidateOnChange = decodeBoolean(initialFrame.content, INVALIDATE_ON_CHANGE_OFFSET);
        int timeToLiveSeconds = decodeInt(initialFrame.content, TIME_TO_LIVE_SECONDS_OFFSET);
        int maxIdleSeconds = decodeInt(initialFrame.content, MAX_IDLE_SECONDS_OFFSET);
        boolean cacheLocalEntries = decodeBoolean(initialFrame.content, CACHE_LOCAL_ENTRIES_OFFSET);

        String name = StringCodec.decode(iterator);
        String inMemoryFormat = StringCodec.decode(iterator);
        EvictionConfigHolder evictionConfigHolder = EvictionConfigHolderCodec.decode(iterator);
        String localUpdatePolicy = StringCodec.decode(iterator);
        NearCachePreloaderConfig preloaderConfig = decodeNullable(iterator, NearCachePreloaderConfigCodec::decode);

        fastForwardToEndFrame(iterator);

        return new NearCacheConfigHolder(name, inMemoryFormat, serializeKeys, invalidateOnChange, timeToLiveSeconds,
                maxIdleSeconds, evictionConfigHolder, cacheLocalEntries, localUpdatePolicy, preloaderConfig);
    }
}
