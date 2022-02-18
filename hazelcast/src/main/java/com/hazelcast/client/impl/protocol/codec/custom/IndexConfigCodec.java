/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

@Generated("236de3bb92bd650a7905dab47a7c4392")
public final class IndexConfigCodec {
    private static final int TYPE_FIELD_OFFSET = 0;
    private static final int INITIAL_FRAME_SIZE = TYPE_FIELD_OFFSET + INT_SIZE_IN_BYTES;

    private IndexConfigCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.config.IndexConfig indexConfig) {
        clientMessage.add(BEGIN_FRAME.copy());

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, indexConfig.getType());
        clientMessage.add(initialFrame);

        CodecUtil.encodeNullable(clientMessage, indexConfig.getName(), StringCodec::encode);
        ListMultiFrameCodec.encode(clientMessage, indexConfig.getAttributes(), StringCodec::encode);
        CodecUtil.encodeNullable(clientMessage, indexConfig.getBitmapIndexOptions(), BitmapIndexOptionsCodec::encode);

        clientMessage.add(END_FRAME.copy());
    }

    public static com.hazelcast.config.IndexConfig decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        int type = decodeInt(initialFrame.content, TYPE_FIELD_OFFSET);

        java.lang.String name = CodecUtil.decodeNullable(iterator, StringCodec::decode);
        java.util.List<java.lang.String> attributes = ListMultiFrameCodec.decode(iterator, StringCodec::decode);
        com.hazelcast.config.BitmapIndexOptions bitmapIndexOptions = CodecUtil.decodeNullable(iterator, BitmapIndexOptionsCodec::decode);

        fastForwardToEndFrame(iterator);

        return CustomTypeFactory.createIndexConfig(name, type, attributes, bitmapIndexOptions);
    }
}
