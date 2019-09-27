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

package com.hazelcast.client.impl.protocol.codec.custom;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.Generated;
import com.hazelcast.client.impl.protocol.codec.builtin.*;

import java.util.ListIterator;

import static com.hazelcast.client.impl.protocol.codec.builtin.CodecUtil.fastForwardToEndFrame;
import static com.hazelcast.client.impl.protocol.ClientMessage.*;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.*;

@Generated("f7925914d87b2a471b3d28ecbadd4401")
public final class MapIndexConfigCodec {
    private static final int ORDERED_FIELD_OFFSET = 0;
    private static final int INITIAL_FRAME_SIZE = ORDERED_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;

    private MapIndexConfigCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.config.MapIndexConfig mapIndexConfig) {
        clientMessage.add(BEGIN_FRAME);

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeBoolean(initialFrame.content, ORDERED_FIELD_OFFSET, mapIndexConfig.isOrdered());
        clientMessage.add(initialFrame);

        StringCodec.encode(clientMessage, mapIndexConfig.getAttribute());

        clientMessage.add(END_FRAME);
    }

    public static com.hazelcast.config.MapIndexConfig decode(ListIterator<ClientMessage.Frame> iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        boolean ordered = decodeBoolean(initialFrame.content, ORDERED_FIELD_OFFSET);

        java.lang.String attribute = StringCodec.decode(iterator);

        fastForwardToEndFrame(iterator);

        return new com.hazelcast.config.MapIndexConfig(attribute, ordered);
    }
}
