/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

@Generated("8b2623b0a7f8bfed37075c323699e5e4")
public final class GcpConfigCodec {
    private static final int ENABLED_FIELD_OFFSET = 0;
    private static final int USE_PUBLIC_IP_FIELD_OFFSET = ENABLED_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int INITIAL_FRAME_SIZE = USE_PUBLIC_IP_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;

    private GcpConfigCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.config.GcpConfig gcpConfig) {
        clientMessage.add(BEGIN_FRAME.copy());

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeBoolean(initialFrame.content, ENABLED_FIELD_OFFSET, gcpConfig.isEnabled());
        encodeBoolean(initialFrame.content, USE_PUBLIC_IP_FIELD_OFFSET, gcpConfig.isUsePublicIp());
        clientMessage.add(initialFrame);

        StringCodec.encode(clientMessage, gcpConfig.getTag());
        MapCodec.encode(clientMessage, gcpConfig.getProperties(), StringCodec::encode, StringCodec::encode);

        clientMessage.add(END_FRAME.copy());
    }

    public static com.hazelcast.config.GcpConfig decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        boolean enabled = decodeBoolean(initialFrame.content, ENABLED_FIELD_OFFSET);
        boolean usePublicIp = decodeBoolean(initialFrame.content, USE_PUBLIC_IP_FIELD_OFFSET);

        java.lang.String tag = StringCodec.decode(iterator);
        java.util.Map<java.lang.String, java.lang.String> properties = MapCodec.decode(iterator, StringCodec::decode, StringCodec::decode);

        fastForwardToEndFrame(iterator);

        return new com.hazelcast.config.GcpConfig(tag, enabled, usePublicIp, properties);
    }
}
