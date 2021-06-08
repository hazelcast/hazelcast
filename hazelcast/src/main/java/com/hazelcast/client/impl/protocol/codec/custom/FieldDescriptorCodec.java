/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

@Generated("8731ffbea4ef00110e54e6f340181a66")
public final class FieldDescriptorCodec {
    private static final int TYPE_FIELD_OFFSET = 0;
    private static final int INDEX_FIELD_OFFSET = TYPE_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int OFFSET_FIELD_OFFSET = INDEX_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int BIT_OFFSET_FIELD_OFFSET = OFFSET_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int INITIAL_FRAME_SIZE = BIT_OFFSET_FIELD_OFFSET + BYTE_SIZE_IN_BYTES;

    private FieldDescriptorCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.internal.serialization.impl.compact.FieldDescriptor fieldDescriptor) {
        clientMessage.add(BEGIN_FRAME.copy());

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, fieldDescriptor.getType());
        encodeInt(initialFrame.content, INDEX_FIELD_OFFSET, fieldDescriptor.getIndex());
        encodeInt(initialFrame.content, OFFSET_FIELD_OFFSET, fieldDescriptor.getOffset());
        encodeByte(initialFrame.content, BIT_OFFSET_FIELD_OFFSET, fieldDescriptor.getBitOffset());
        clientMessage.add(initialFrame);

        StringCodec.encode(clientMessage, fieldDescriptor.getFieldName());

        clientMessage.add(END_FRAME.copy());
    }

    public static com.hazelcast.internal.serialization.impl.compact.FieldDescriptor decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        int type = decodeInt(initialFrame.content, TYPE_FIELD_OFFSET);
        int index = decodeInt(initialFrame.content, INDEX_FIELD_OFFSET);
        int offset = decodeInt(initialFrame.content, OFFSET_FIELD_OFFSET);
        byte bitOffset = decodeByte(initialFrame.content, BIT_OFFSET_FIELD_OFFSET);

        java.lang.String fieldName = StringCodec.decode(iterator);

        fastForwardToEndFrame(iterator);

        return CustomTypeFactory.createFieldDescriptor(fieldName, type, index, offset, bitOffset);
    }
}
