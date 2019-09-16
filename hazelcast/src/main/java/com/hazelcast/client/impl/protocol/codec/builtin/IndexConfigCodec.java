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
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.nio.Bits;

import java.util.List;
import java.util.ListIterator;

import static com.hazelcast.client.impl.protocol.ClientMessage.BEGIN_FRAME;
import static com.hazelcast.client.impl.protocol.ClientMessage.END_FRAME;
import static com.hazelcast.client.impl.protocol.codec.builtin.CodecUtil.fastForwardToEndFrame;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.decodeInt;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.encodeInt;

public final class IndexConfigCodec {
    private static final int TYPE_OFFSET = 0;
    private static final int INITIAL_FRAME_SIZE = TYPE_OFFSET + Bits.INT_SIZE_IN_BYTES;

    private IndexConfigCodec() {
    }

    public static void encode(ClientMessage clientMessage, IndexConfig config) {
        clientMessage.add(BEGIN_FRAME);

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeInt(initialFrame.content, TYPE_OFFSET, config.getType().getId());
        clientMessage.add(initialFrame);

        CodecUtil.encodeNullable(clientMessage, config.getName(), StringCodec::encode);
        ListMultiFrameCodec.encode(clientMessage, config.getAttributes(), StringCodec::encode);

        clientMessage.add(END_FRAME);
    }

    public static IndexConfig decode(ListIterator<ClientMessage.Frame> iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        int type = decodeInt(initialFrame.content, TYPE_OFFSET);

        String name = CodecUtil.decodeNullable(iterator, StringCodec::decode);

        List<String> columns = ListMultiFrameCodec.decode(iterator, StringCodec::decode);

        fastForwardToEndFrame(iterator);

        return new IndexConfig().setName(name).setType(IndexType.getById(type)).setAttributes(columns);
    }
}
