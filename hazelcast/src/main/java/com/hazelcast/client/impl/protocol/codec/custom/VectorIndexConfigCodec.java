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
@Generated("68fdb687656a1ea263358c750a00aa99")
public final class VectorIndexConfigCodec {
    private static final int METRIC_FIELD_OFFSET = 0;
    private static final int DIMENSION_FIELD_OFFSET = METRIC_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int INITIAL_FRAME_SIZE = DIMENSION_FIELD_OFFSET + INT_SIZE_IN_BYTES;

    private VectorIndexConfigCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.config.vector.VectorIndexConfig vectorIndexConfig) {
        clientMessage.add(BEGIN_FRAME.copy());

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeInt(initialFrame.content, METRIC_FIELD_OFFSET, vectorIndexConfig.getMetric());
        encodeInt(initialFrame.content, DIMENSION_FIELD_OFFSET, vectorIndexConfig.getDimension());
        clientMessage.add(initialFrame);

        CodecUtil.encodeNullable(clientMessage, vectorIndexConfig.getName(), StringCodec::encode);

        clientMessage.add(END_FRAME.copy());
    }

    public static com.hazelcast.config.vector.VectorIndexConfig decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        int metric = decodeInt(initialFrame.content, METRIC_FIELD_OFFSET);
        int dimension = decodeInt(initialFrame.content, DIMENSION_FIELD_OFFSET);

        java.lang.String name = CodecUtil.decodeNullable(iterator, StringCodec::decode);

        fastForwardToEndFrame(iterator);

        return CustomTypeFactory.createVectorIndexConfig(name, metric, dimension);
    }
}
