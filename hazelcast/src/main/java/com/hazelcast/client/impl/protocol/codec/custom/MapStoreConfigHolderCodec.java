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
@Generated("6577a25ffa90d34a4fa834a47eff14e7")
public final class MapStoreConfigHolderCodec {
    private static final int ENABLED_FIELD_OFFSET = 0;
    private static final int WRITE_COALESCING_FIELD_OFFSET = ENABLED_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int WRITE_DELAY_SECONDS_FIELD_OFFSET = WRITE_COALESCING_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int WRITE_BATCH_SIZE_FIELD_OFFSET = WRITE_DELAY_SECONDS_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int OFFLOAD_FIELD_OFFSET = WRITE_BATCH_SIZE_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int INITIAL_FRAME_SIZE = OFFLOAD_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;

    private MapStoreConfigHolderCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.client.impl.protocol.task.dynamicconfig.MapStoreConfigHolder mapStoreConfigHolder) {
        clientMessage.add(BEGIN_FRAME.copy());

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeBoolean(initialFrame.content, ENABLED_FIELD_OFFSET, mapStoreConfigHolder.isEnabled());
        encodeBoolean(initialFrame.content, WRITE_COALESCING_FIELD_OFFSET, mapStoreConfigHolder.isWriteCoalescing());
        encodeInt(initialFrame.content, WRITE_DELAY_SECONDS_FIELD_OFFSET, mapStoreConfigHolder.getWriteDelaySeconds());
        encodeInt(initialFrame.content, WRITE_BATCH_SIZE_FIELD_OFFSET, mapStoreConfigHolder.getWriteBatchSize());
        encodeBoolean(initialFrame.content, OFFLOAD_FIELD_OFFSET, mapStoreConfigHolder.isOffload());
        clientMessage.add(initialFrame);

        CodecUtil.encodeNullable(clientMessage, mapStoreConfigHolder.getClassName(), StringCodec::encode);
        DataCodec.encodeNullable(clientMessage, mapStoreConfigHolder.getImplementation());
        CodecUtil.encodeNullable(clientMessage, mapStoreConfigHolder.getFactoryClassName(), StringCodec::encode);
        DataCodec.encodeNullable(clientMessage, mapStoreConfigHolder.getFactoryImplementation());
        MapCodec.encodeNullable(clientMessage, mapStoreConfigHolder.getProperties(), StringCodec::encode, StringCodec::encode);
        StringCodec.encode(clientMessage, mapStoreConfigHolder.getInitialLoadMode());

        clientMessage.add(END_FRAME.copy());
    }

    public static com.hazelcast.client.impl.protocol.task.dynamicconfig.MapStoreConfigHolder decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        boolean enabled = decodeBoolean(initialFrame.content, ENABLED_FIELD_OFFSET);
        boolean writeCoalescing = decodeBoolean(initialFrame.content, WRITE_COALESCING_FIELD_OFFSET);
        int writeDelaySeconds = decodeInt(initialFrame.content, WRITE_DELAY_SECONDS_FIELD_OFFSET);
        int writeBatchSize = decodeInt(initialFrame.content, WRITE_BATCH_SIZE_FIELD_OFFSET);
        boolean isOffloadExists = false;
        boolean offload = false;
        if (initialFrame.content.length >= OFFLOAD_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES) {
            offload = decodeBoolean(initialFrame.content, OFFLOAD_FIELD_OFFSET);
            isOffloadExists = true;
        }

        java.lang.String className = CodecUtil.decodeNullable(iterator, StringCodec::decode);
        com.hazelcast.internal.serialization.Data implementation = DataCodec.decodeNullable(iterator);
        java.lang.String factoryClassName = CodecUtil.decodeNullable(iterator, StringCodec::decode);
        com.hazelcast.internal.serialization.Data factoryImplementation = DataCodec.decodeNullable(iterator);
        java.util.Map<java.lang.String, java.lang.String> properties = MapCodec.decodeNullable(iterator, StringCodec::decode, StringCodec::decode);
        java.lang.String initialLoadMode = StringCodec.decode(iterator);

        fastForwardToEndFrame(iterator);

        return new com.hazelcast.client.impl.protocol.task.dynamicconfig.MapStoreConfigHolder(enabled, writeCoalescing, writeDelaySeconds, writeBatchSize, className, implementation, factoryClassName, factoryImplementation, properties, initialLoadMode, isOffloadExists, offload);
    }
}
