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
import com.hazelcast.client.impl.protocol.task.dynamicconfig.ListenerConfigHolder;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.serialization.Data;

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

public final class ListenerConfigHolderCodec {
    private static final int INCLUDE_VALUE_OFFSET = 0;
    private static final int LOCAL_OFFSET = INCLUDE_VALUE_OFFSET + Bits.BOOLEAN_SIZE_IN_BYTES;
    private static final int LISTENER_TYPE_OFFSET = LOCAL_OFFSET + Bits.BOOLEAN_SIZE_IN_BYTES;
    private static final int INITIAL_FRAME_SIZE = LISTENER_TYPE_OFFSET + Bits.INT_SIZE_IN_BYTES;

    private ListenerConfigHolderCodec() {
    }

    public static void encode(ClientMessage clientMessage, ListenerConfigHolder configHolder) {
        clientMessage.add(BEGIN_FRAME);

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeBoolean(initialFrame.content, INCLUDE_VALUE_OFFSET, configHolder.isIncludeValue());
        encodeBoolean(initialFrame.content, LOCAL_OFFSET, configHolder.isLocal());
        encodeInt(initialFrame.content, LISTENER_TYPE_OFFSET, configHolder.getListenerType());
        clientMessage.add(initialFrame);

        encodeNullable(clientMessage, configHolder.getClassName(), StringCodec::encode);
        encodeNullable(clientMessage, configHolder.getListenerImplementation(), DataCodec::encode);

        clientMessage.add(END_FRAME);
    }

    public static ListenerConfigHolder decode(ListIterator<ClientMessage.Frame> iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        boolean includeValue = decodeBoolean(initialFrame.content, INCLUDE_VALUE_OFFSET);
        boolean local = decodeBoolean(initialFrame.content, LOCAL_OFFSET);
        int listenerType = decodeInt(initialFrame.content, LISTENER_TYPE_OFFSET);

        String className = decodeNullable(iterator, StringCodec::decode);
        Data listenerImplementation = decodeNullable(iterator, DataCodec::decode);

        fastForwardToEndFrame(iterator);

        if (className == null) {
            return new ListenerConfigHolder(listenerType, listenerImplementation, includeValue, local);
        } else {
            return new ListenerConfigHolder(listenerType, className, includeValue, local);
        }
    }
}
