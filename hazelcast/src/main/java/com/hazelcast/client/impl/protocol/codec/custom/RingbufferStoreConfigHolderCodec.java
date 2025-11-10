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
@Generated("d46755d932b866895fe7c5e3b529ef53")
public final class RingbufferStoreConfigHolderCodec {
    private static final int ENABLED_FIELD_OFFSET = 0;
    private static final int INITIAL_FRAME_SIZE = ENABLED_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;

    private RingbufferStoreConfigHolderCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.client.impl.protocol.task.dynamicconfig.RingbufferStoreConfigHolder ringbufferStoreConfigHolder) {
        clientMessage.add(BEGIN_FRAME.copy());

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeBoolean(initialFrame.content, ENABLED_FIELD_OFFSET, ringbufferStoreConfigHolder.isEnabled());
        clientMessage.add(initialFrame);

        CodecUtil.encodeNullable(clientMessage, ringbufferStoreConfigHolder.getClassName(), StringCodec::encode);
        CodecUtil.encodeNullable(clientMessage, ringbufferStoreConfigHolder.getFactoryClassName(), StringCodec::encode);
        DataCodec.encodeNullable(clientMessage, ringbufferStoreConfigHolder.getImplementation());
        DataCodec.encodeNullable(clientMessage, ringbufferStoreConfigHolder.getFactoryImplementation());
        MapCodec.encodeNullable(clientMessage, ringbufferStoreConfigHolder.getProperties(), StringCodec::encode, StringCodec::encode);

        clientMessage.add(END_FRAME.copy());
    }

    public static com.hazelcast.client.impl.protocol.task.dynamicconfig.RingbufferStoreConfigHolder decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        boolean enabled = decodeBoolean(initialFrame.content, ENABLED_FIELD_OFFSET);

        java.lang.String className = CodecUtil.decodeNullable(iterator, StringCodec::decode);
        java.lang.String factoryClassName = CodecUtil.decodeNullable(iterator, StringCodec::decode);
        com.hazelcast.internal.serialization.Data implementation = DataCodec.decodeNullable(iterator);
        com.hazelcast.internal.serialization.Data factoryImplementation = DataCodec.decodeNullable(iterator);
        java.util.Map<java.lang.String, java.lang.String> properties = MapCodec.decodeNullable(iterator, StringCodec::decode, StringCodec::decode);

        fastForwardToEndFrame(iterator);

        return new com.hazelcast.client.impl.protocol.task.dynamicconfig.RingbufferStoreConfigHolder(className, factoryClassName, implementation, factoryImplementation, properties, enabled);
    }
}
