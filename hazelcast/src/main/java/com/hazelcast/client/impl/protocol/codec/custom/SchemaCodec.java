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

@Generated("cdacd186075c96de39e267ed6966edfc")
public final class SchemaCodec {

    private SchemaCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.internal.serialization.impl.compact.Schema schema) {
        clientMessage.add(BEGIN_FRAME.copy());

        StringCodec.encode(clientMessage, schema.getTypeName());
        ListMultiFrameCodec.encode(clientMessage, schema.getFields(), FieldDescriptorCodec::encode);

        clientMessage.add(END_FRAME.copy());
    }

    public static com.hazelcast.internal.serialization.impl.compact.Schema decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        java.lang.String typeName = StringCodec.decode(iterator);
        java.util.List<com.hazelcast.internal.serialization.impl.compact.FieldDescriptor> fields = ListMultiFrameCodec.decode(iterator, FieldDescriptorCodec::decode);

        fastForwardToEndFrame(iterator);

        return CustomTypeFactory.createSchema(typeName, fields);
    }
}
