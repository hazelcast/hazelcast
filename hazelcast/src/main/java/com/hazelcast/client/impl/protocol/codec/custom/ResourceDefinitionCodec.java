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
@Generated("b67aba5eb490e326b898a26fde5f8219")
public final class ResourceDefinitionCodec {
    private static final int RESOURCE_TYPE_FIELD_OFFSET = 0;
    private static final int INITIAL_FRAME_SIZE = RESOURCE_TYPE_FIELD_OFFSET + INT_SIZE_IN_BYTES;

    private ResourceDefinitionCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.client.impl.protocol.task.dynamicconfig.ResourceDefinitionHolder resourceDefinition) {
        clientMessage.add(BEGIN_FRAME.copy());

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeInt(initialFrame.content, RESOURCE_TYPE_FIELD_OFFSET, resourceDefinition.getResourceType());
        clientMessage.add(initialFrame);

        StringCodec.encode(clientMessage, resourceDefinition.getId());
        CodecUtil.encodeNullable(clientMessage, resourceDefinition.getPayload(), ByteArrayCodec::encode);
        CodecUtil.encodeNullable(clientMessage, resourceDefinition.getResourceUrl(), StringCodec::encode);

        clientMessage.add(END_FRAME.copy());
    }

    public static com.hazelcast.client.impl.protocol.task.dynamicconfig.ResourceDefinitionHolder decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        int resourceType = decodeInt(initialFrame.content, RESOURCE_TYPE_FIELD_OFFSET);

        java.lang.String id = StringCodec.decode(iterator);
        byte[] payload = CodecUtil.decodeNullable(iterator, ByteArrayCodec::decode);
        java.lang.String resourceUrl = CodecUtil.decodeNullable(iterator, StringCodec::decode);

        fastForwardToEndFrame(iterator);

        return new com.hazelcast.client.impl.protocol.task.dynamicconfig.ResourceDefinitionHolder(id, resourceType, payload, resourceUrl);
    }
}
