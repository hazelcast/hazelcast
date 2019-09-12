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
import com.hazelcast.config.IndexAttributeConfig;
import com.hazelcast.nio.Bits;

import java.util.ListIterator;

import static com.hazelcast.client.impl.protocol.ClientMessage.BEGIN_FRAME;
import static com.hazelcast.client.impl.protocol.ClientMessage.END_FRAME;
import static com.hazelcast.client.impl.protocol.codec.builtin.CodecUtil.fastForwardToEndFrame;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.decodeBoolean;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.encodeBoolean;

public final class IndexAttributeConfigCodec {
    private static final int ASC_OFFSET = 0;
    private static final int INITIAL_FRAME_SIZE = ASC_OFFSET + Bits.BOOLEAN_SIZE_IN_BYTES;

    private IndexAttributeConfigCodec() {
    }

    public static void encode(ClientMessage clientMessage, IndexAttributeConfig config) {
        clientMessage.add(BEGIN_FRAME);

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeBoolean(initialFrame.content, ASC_OFFSET, config.isAscending());
        clientMessage.add(initialFrame);

        StringCodec.encode(clientMessage, config.getName());

        clientMessage.add(END_FRAME);
    }

    public static IndexAttributeConfig decode(ListIterator<ClientMessage.Frame> iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        boolean asc = decodeBoolean(initialFrame.content, ASC_OFFSET);

        String column = StringCodec.decode(iterator);

        fastForwardToEndFrame(iterator);

        return new IndexAttributeConfig(new IndexAttributeConfig().setAscending(asc).setName(column));
    }
}
