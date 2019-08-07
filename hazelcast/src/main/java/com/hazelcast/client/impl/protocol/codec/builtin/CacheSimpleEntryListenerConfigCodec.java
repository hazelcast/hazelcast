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
import com.hazelcast.config.CacheSimpleEntryListenerConfig;
import com.hazelcast.nio.Bits;

import java.util.ListIterator;

import static com.hazelcast.client.impl.protocol.ClientMessage.BEGIN_FRAME;
import static com.hazelcast.client.impl.protocol.ClientMessage.END_FRAME;
import static com.hazelcast.client.impl.protocol.codec.builtin.CodecUtil.decodeNullable;
import static com.hazelcast.client.impl.protocol.codec.builtin.CodecUtil.encodeNullable;
import static com.hazelcast.client.impl.protocol.codec.builtin.CodecUtil.fastForwardToEndFrame;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.decodeBoolean;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.encodeBoolean;

public final class CacheSimpleEntryListenerConfigCodec {
    private static final int OLD_VALUE_REQUIRED_OFFSET = 0;
    private static final int SYNCHRONOUS_OFFSET = OLD_VALUE_REQUIRED_OFFSET + Bits.BOOLEAN_SIZE_IN_BYTES;
    private static final int INITIAL_FRAME_SIZE = SYNCHRONOUS_OFFSET + Bits.BOOLEAN_SIZE_IN_BYTES;

    private CacheSimpleEntryListenerConfigCodec() {
    }

    public static void encode(ClientMessage clientMessage, CacheSimpleEntryListenerConfig config) {
        clientMessage.add(BEGIN_FRAME);

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeBoolean(initialFrame.content, OLD_VALUE_REQUIRED_OFFSET, config.isOldValueRequired());
        encodeBoolean(initialFrame.content, SYNCHRONOUS_OFFSET, config.isSynchronous());
        clientMessage.add(initialFrame);

        encodeNullable(clientMessage, config.getCacheEntryListenerFactory(), StringCodec::encode);
        encodeNullable(clientMessage, config.getCacheEntryEventFilterFactory(), StringCodec::encode);

        clientMessage.add(END_FRAME);
    }

    public static CacheSimpleEntryListenerConfig decode(ListIterator<ClientMessage.Frame> iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        boolean oldValueRequired = decodeBoolean(initialFrame.content, OLD_VALUE_REQUIRED_OFFSET);
        boolean synchronous = decodeBoolean(initialFrame.content, SYNCHRONOUS_OFFSET);

        String cacheEntryListenerFactory = decodeNullable(iterator, StringCodec::decode);
        String cacheEntryEventFilterFactory = decodeNullable(iterator, StringCodec::decode);

        fastForwardToEndFrame(iterator);

        CacheSimpleEntryListenerConfig config = new CacheSimpleEntryListenerConfig();
        config.setOldValueRequired(oldValueRequired);
        config.setSynchronous(synchronous);
        config.setCacheEntryListenerFactory(cacheEntryListenerFactory);
        config.setCacheEntryEventFilterFactory(cacheEntryEventFilterFactory);
        return config;
    }
}
