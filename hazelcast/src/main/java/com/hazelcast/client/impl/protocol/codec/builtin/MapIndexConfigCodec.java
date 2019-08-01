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
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.nio.Bits;

import java.util.ListIterator;

import static com.hazelcast.client.impl.protocol.ClientMessage.BEGIN_FRAME;
import static com.hazelcast.client.impl.protocol.ClientMessage.END_FRAME;
import static com.hazelcast.client.impl.protocol.codec.builtin.CodecUtil.fastForwardToEndFrame;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.decodeBoolean;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.encodeBoolean;

public final class MapIndexConfigCodec {
    private static final int ORDERED_OFFSET = 0;
    private static final int INITIAL_FRAME_SIZE = ORDERED_OFFSET + Bits.BOOLEAN_SIZE_IN_BYTES;

    private MapIndexConfigCodec() {
    }

    public static void encode(ClientMessage clientMessage, MapIndexConfig config) {
        clientMessage.add(BEGIN_FRAME);

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeBoolean(initialFrame.content, ORDERED_OFFSET, config.isOrdered());
        clientMessage.add(initialFrame);

        StringCodec.encode(clientMessage, config.getAttribute());

        clientMessage.add(END_FRAME);
    }

    public static MapIndexConfig decode(ListIterator<ClientMessage.Frame> iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        boolean ordered = decodeBoolean(initialFrame.content, ORDERED_OFFSET);

        String attribute = StringCodec.decode(iterator);

        fastForwardToEndFrame(iterator);

        return new MapIndexConfig(attribute, ordered);
    }
}
