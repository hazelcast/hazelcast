/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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
@Generated("c393fd32d077f676c1e57018e3ddc52a")
public final class VectorPairCodec {
    private static final int TYPE_FIELD_OFFSET = 0;
    private static final int INITIAL_FRAME_SIZE = TYPE_FIELD_OFFSET + BYTE_SIZE_IN_BYTES;

    private VectorPairCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.client.impl.protocol.codec.holder.VectorPairHolder vectorPair) {
        clientMessage.add(BEGIN_FRAME.copy());

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeByte(initialFrame.content, TYPE_FIELD_OFFSET, vectorPair.getType());
        clientMessage.add(initialFrame);

        StringCodec.encode(clientMessage, vectorPair.getName());
        CodecUtil.encodeNullable(clientMessage, vectorPair.getVector(), FloatArrayCodec::encode);

        clientMessage.add(END_FRAME.copy());
    }

    public static com.hazelcast.client.impl.protocol.codec.holder.VectorPairHolder decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        byte type = decodeByte(initialFrame.content, TYPE_FIELD_OFFSET);

        java.lang.String name = StringCodec.decode(iterator);
        float[] vector = CodecUtil.decodeNullable(iterator, FloatArrayCodec::decode);

        fastForwardToEndFrame(iterator);

        return new com.hazelcast.client.impl.protocol.codec.holder.VectorPairHolder(name, type, vector);
    }
}
