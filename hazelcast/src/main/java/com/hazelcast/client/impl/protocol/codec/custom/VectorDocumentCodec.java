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
@Generated("522ba97d256923b083e9d7626de2696c")
public final class VectorDocumentCodec {

    private VectorDocumentCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.vector.impl.DataVectorDocument vectorDocument) {
        clientMessage.add(BEGIN_FRAME.copy());

        DataCodec.encode(clientMessage, vectorDocument.getValue());
        ListMultiFrameCodec.encode(clientMessage, vectorDocument.getVectors(), VectorPairCodec::encode);

        clientMessage.add(END_FRAME.copy());
    }

    public static com.hazelcast.vector.impl.DataVectorDocument decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        com.hazelcast.internal.serialization.Data value = DataCodec.decode(iterator);
        java.util.List<com.hazelcast.client.impl.protocol.codec.holder.VectorPairHolder> vectors = ListMultiFrameCodec.decode(iterator, VectorPairCodec::decode);

        fastForwardToEndFrame(iterator);

        return CustomTypeFactory.createVectorDocument(value, vectors);
    }
}
