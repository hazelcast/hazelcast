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

@Generated("12cf57195d90dbb72046152464d26bd5")
public final class CacheSimpleEntryListenerConfigCodec {
    private static final int OLD_VALUE_REQUIRED_FIELD_OFFSET = 0;
    private static final int SYNCHRONOUS_FIELD_OFFSET = OLD_VALUE_REQUIRED_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int INITIAL_FRAME_SIZE = SYNCHRONOUS_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;

    private CacheSimpleEntryListenerConfigCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.config.CacheSimpleEntryListenerConfig cacheSimpleEntryListenerConfig) {
        clientMessage.add(BEGIN_FRAME.copy());

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeBoolean(initialFrame.content, OLD_VALUE_REQUIRED_FIELD_OFFSET, cacheSimpleEntryListenerConfig.isOldValueRequired());
        encodeBoolean(initialFrame.content, SYNCHRONOUS_FIELD_OFFSET, cacheSimpleEntryListenerConfig.isSynchronous());
        clientMessage.add(initialFrame);

        CodecUtil.encodeNullable(clientMessage, cacheSimpleEntryListenerConfig.getCacheEntryListenerFactory(), StringCodec::encode);
        CodecUtil.encodeNullable(clientMessage, cacheSimpleEntryListenerConfig.getCacheEntryEventFilterFactory(), StringCodec::encode);

        clientMessage.add(END_FRAME.copy());
    }

    public static com.hazelcast.config.CacheSimpleEntryListenerConfig decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        boolean oldValueRequired = decodeBoolean(initialFrame.content, OLD_VALUE_REQUIRED_FIELD_OFFSET);
        boolean synchronous = decodeBoolean(initialFrame.content, SYNCHRONOUS_FIELD_OFFSET);

        java.lang.String cacheEntryListenerFactory = CodecUtil.decodeNullable(iterator, StringCodec::decode);
        java.lang.String cacheEntryEventFilterFactory = CodecUtil.decodeNullable(iterator, StringCodec::decode);

        fastForwardToEndFrame(iterator);

        return CustomTypeFactory.createCacheSimpleEntryListenerConfig(oldValueRequired, synchronous, cacheEntryListenerFactory, cacheEntryEventFilterFactory);
    }
}
