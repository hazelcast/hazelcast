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
import com.hazelcast.client.impl.protocol.task.dynamicconfig.RingbufferStoreConfigHolder;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.serialization.Data;

import java.util.ListIterator;
import java.util.Map;

import static com.hazelcast.client.impl.protocol.ClientMessage.BEGIN_FRAME;
import static com.hazelcast.client.impl.protocol.ClientMessage.END_FRAME;
import static com.hazelcast.client.impl.protocol.codec.builtin.CodecUtil.decodeNullable;
import static com.hazelcast.client.impl.protocol.codec.builtin.CodecUtil.encodeNullable;
import static com.hazelcast.client.impl.protocol.codec.builtin.CodecUtil.fastForwardToEndFrame;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.decodeBoolean;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.encodeBoolean;

public final class RingbufferStoreConfigHolderCodec {
    private static final int ENABLED_OFFSET = 0;
    private static final int INITIAL_FRAME_SIZE = ENABLED_OFFSET + Bits.BOOLEAN_SIZE_IN_BYTES;

    private RingbufferStoreConfigHolderCodec() {
    }

    public static void encode(ClientMessage clientMessage, RingbufferStoreConfigHolder configHolder) {
        clientMessage.add(BEGIN_FRAME);

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeBoolean(initialFrame.content, ENABLED_OFFSET, configHolder.isEnabled());
        clientMessage.add(initialFrame);

        encodeNullable(clientMessage, configHolder.getClassName(), StringCodec::encode);
        encodeNullable(clientMessage, configHolder.getImplementation(), DataCodec::encode);
        encodeNullable(clientMessage, configHolder.getFactoryClassName(), StringCodec::encode);
        encodeNullable(clientMessage, configHolder.getFactoryImplementation(), DataCodec::encode);
        MapCodec.encodeNullable(clientMessage, configHolder.getProperties().entrySet(), StringCodec::encode, StringCodec::encode);

        clientMessage.add(END_FRAME);
    }

    public static RingbufferStoreConfigHolder decode(ListIterator<ClientMessage.Frame> iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        boolean enabled = decodeBoolean(initialFrame.content, ENABLED_OFFSET);

        String className = decodeNullable(iterator, StringCodec::decode);
        Data implementation = decodeNullable(iterator, DataCodec::decode);
        String factoryClassName = decodeNullable(iterator, StringCodec::decode);
        Data factoryImplementation = decodeNullable(iterator, DataCodec::decode);
        Map<String, String> properties = MapCodec.decodeToNullableMap(iterator, StringCodec::decode, StringCodec::decode);

        fastForwardToEndFrame(iterator);

        return new RingbufferStoreConfigHolder(className, factoryClassName, implementation, factoryImplementation, properties, enabled);
    }
}
