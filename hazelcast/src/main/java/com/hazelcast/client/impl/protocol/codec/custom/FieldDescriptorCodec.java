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

@Generated("50feb82def1b8832584347f78d61b551")
public final class FieldDescriptorCodec {
    private static final int KIND_FIELD_OFFSET = 0;
    private static final int INITIAL_FRAME_SIZE = KIND_FIELD_OFFSET + INT_SIZE_IN_BYTES;

    private FieldDescriptorCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.internal.serialization.impl.compact.FieldDescriptor fieldDescriptor) {
        clientMessage.add(BEGIN_FRAME.copy());

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeInt(initialFrame.content, KIND_FIELD_OFFSET, fieldDescriptor.getKind());
        clientMessage.add(initialFrame);

        StringCodec.encode(clientMessage, fieldDescriptor.getFieldName());

        clientMessage.add(END_FRAME.copy());
    }

    public static com.hazelcast.internal.serialization.impl.compact.FieldDescriptor decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        int kind = decodeInt(initialFrame.content, KIND_FIELD_OFFSET);

        java.lang.String fieldName = StringCodec.decode(iterator);

        fastForwardToEndFrame(iterator);

        return CustomTypeFactory.createFieldDescriptor(fieldName, kind);
    }
}
