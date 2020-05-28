/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

@Generated("2ef402bf764a8ab929c01684a9b742a3")
public final class EndpointQualifierCodec {
    private static final int TYPE_FIELD_OFFSET = 0;
    private static final int INITIAL_FRAME_SIZE = TYPE_FIELD_OFFSET + INT_SIZE_IN_BYTES;

    private EndpointQualifierCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.instance.EndpointQualifier endpointQualifier) {
        clientMessage.add(BEGIN_FRAME.copy());

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, endpointQualifier.getType());
        clientMessage.add(initialFrame);

        CodecUtil.encodeNullable(clientMessage, endpointQualifier.getIdentifier(), StringCodec::encode);

        clientMessage.add(END_FRAME.copy());
    }

    public static com.hazelcast.instance.EndpointQualifier decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        int type = decodeInt(initialFrame.content, TYPE_FIELD_OFFSET);

        java.lang.String identifier = CodecUtil.decodeNullable(iterator, StringCodec::decode);

        fastForwardToEndFrame(iterator);

        return CustomTypeFactory.createEndpointQualifier(type, identifier);
    }
}
