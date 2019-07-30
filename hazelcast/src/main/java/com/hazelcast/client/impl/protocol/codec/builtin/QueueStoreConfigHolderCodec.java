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
import com.hazelcast.client.impl.protocol.task.dynamicconfig.QueueStoreConfigHolder;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.serialization.Data;

import java.util.ListIterator;
import java.util.Map;

import static com.hazelcast.client.impl.protocol.ClientMessage.BEGIN_FRAME;
import static com.hazelcast.client.impl.protocol.ClientMessage.END_FRAME;
import static com.hazelcast.client.impl.protocol.codec.builtin.CodecUtil.fastForwardToEndFrame;

public class QueueStoreConfigHolderCodec {
    private static final int ENABLED_OFFSET = 0;
    private static final int INITIAL_FRAME_SIZE = ENABLED_OFFSET + Bits.BOOLEAN_SIZE_IN_BYTES;

    public static void encode(ClientMessage clientMessage, QueueStoreConfigHolder configHolder) {
        clientMessage.addFrame(BEGIN_FRAME);

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        FixedSizeTypesCodec.encodeBoolean(initialFrame.content, ENABLED_OFFSET, configHolder.isEnabled());
        clientMessage.addFrame(initialFrame);

        CodecUtil.encodeNullable(clientMessage, configHolder.getClassName(), StringCodec::encode);
        CodecUtil.encodeNullable(clientMessage, configHolder.getImplementation(), DataCodec::encode);
        CodecUtil.encodeNullable(clientMessage, configHolder.getFactoryClassName(), StringCodec::encode);
        CodecUtil.encodeNullable(clientMessage, configHolder.getFactoryImplementation(), DataCodec::encode);
        MapCodec.encodeNullable(clientMessage, configHolder.getProperties().entrySet(), StringCodec::encode, StringCodec::encode);

        clientMessage.addFrame(END_FRAME);
    }

    public static QueueStoreConfigHolder decode(ListIterator<ClientMessage.Frame> iterator) {
        iterator.next(); // begin frame

        ClientMessage.Frame initialFrame = iterator.next();
        boolean enabled = FixedSizeTypesCodec.decodeBoolean(initialFrame.content, ENABLED_OFFSET);

        String className = CodecUtil.decodeNullable(iterator, StringCodec::decode);
        Data implementation = CodecUtil.decodeNullable(iterator, DataCodec::decode);
        String factoryClassName = CodecUtil.decodeNullable(iterator, StringCodec::decode);
        Data factoryImplementation = CodecUtil.decodeNullable(iterator, DataCodec::decode);
        Map<String, String> properties = MapCodec.decodeToNullableMap(iterator, StringCodec::decode, StringCodec::decode);

        fastForwardToEndFrame(iterator);

        return new QueueStoreConfigHolder(className, factoryClassName, implementation, factoryImplementation, properties, enabled);
    }
}
