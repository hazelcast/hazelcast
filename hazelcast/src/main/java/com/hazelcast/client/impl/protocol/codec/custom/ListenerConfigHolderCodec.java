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
@Generated("cd2de6fe8d410031aaae14556564c205")
public final class ListenerConfigHolderCodec {
    private static final int LISTENER_TYPE_FIELD_OFFSET = 0;
    private static final int INCLUDE_VALUE_FIELD_OFFSET = LISTENER_TYPE_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int LOCAL_FIELD_OFFSET = INCLUDE_VALUE_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int INITIAL_FRAME_SIZE = LOCAL_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;

    private ListenerConfigHolderCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.client.impl.protocol.task.dynamicconfig.ListenerConfigHolder listenerConfigHolder) {
        clientMessage.add(BEGIN_FRAME.copy());

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeInt(initialFrame.content, LISTENER_TYPE_FIELD_OFFSET, listenerConfigHolder.getListenerType());
        encodeBoolean(initialFrame.content, INCLUDE_VALUE_FIELD_OFFSET, listenerConfigHolder.isIncludeValue());
        encodeBoolean(initialFrame.content, LOCAL_FIELD_OFFSET, listenerConfigHolder.isLocal());
        clientMessage.add(initialFrame);

        DataCodec.encodeNullable(clientMessage, listenerConfigHolder.getListenerImplementation());
        CodecUtil.encodeNullable(clientMessage, listenerConfigHolder.getClassName(), StringCodec::encode);

        clientMessage.add(END_FRAME.copy());
    }

    public static com.hazelcast.client.impl.protocol.task.dynamicconfig.ListenerConfigHolder decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        int listenerType = decodeInt(initialFrame.content, LISTENER_TYPE_FIELD_OFFSET);
        boolean includeValue = decodeBoolean(initialFrame.content, INCLUDE_VALUE_FIELD_OFFSET);
        boolean local = decodeBoolean(initialFrame.content, LOCAL_FIELD_OFFSET);

        com.hazelcast.internal.serialization.Data listenerImplementation = DataCodec.decodeNullable(iterator);
        java.lang.String className = CodecUtil.decodeNullable(iterator, StringCodec::decode);

        fastForwardToEndFrame(iterator);

        return new com.hazelcast.client.impl.protocol.task.dynamicconfig.ListenerConfigHolder(listenerType, listenerImplementation, className, includeValue, local);
    }
}
