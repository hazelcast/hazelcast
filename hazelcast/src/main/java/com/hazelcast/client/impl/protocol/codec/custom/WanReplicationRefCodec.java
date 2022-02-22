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

@Generated("7e22584abefa455670fdf95c62a414ea")
public final class WanReplicationRefCodec {
    private static final int REPUBLISHING_ENABLED_FIELD_OFFSET = 0;
    private static final int INITIAL_FRAME_SIZE = REPUBLISHING_ENABLED_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;

    private WanReplicationRefCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.config.WanReplicationRef wanReplicationRef) {
        clientMessage.add(BEGIN_FRAME.copy());

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeBoolean(initialFrame.content, REPUBLISHING_ENABLED_FIELD_OFFSET, wanReplicationRef.isRepublishingEnabled());
        clientMessage.add(initialFrame);

        StringCodec.encode(clientMessage, wanReplicationRef.getName());
        StringCodec.encode(clientMessage, wanReplicationRef.getMergePolicyClassName());
        ListMultiFrameCodec.encodeNullable(clientMessage, wanReplicationRef.getFilters(), StringCodec::encode);

        clientMessage.add(END_FRAME.copy());
    }

    public static com.hazelcast.config.WanReplicationRef decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        boolean republishingEnabled = decodeBoolean(initialFrame.content, REPUBLISHING_ENABLED_FIELD_OFFSET);

        java.lang.String name = StringCodec.decode(iterator);
        java.lang.String mergePolicyClassName = StringCodec.decode(iterator);
        java.util.List<java.lang.String> filters = ListMultiFrameCodec.decodeNullable(iterator, StringCodec::decode);

        fastForwardToEndFrame(iterator);

        return new com.hazelcast.config.WanReplicationRef(name, mergePolicyClassName, filters, republishingEnabled);
    }
}
