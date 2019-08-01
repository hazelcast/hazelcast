/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.codec.builtin;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.nio.Bits;

import java.util.List;
import java.util.ListIterator;

import static com.hazelcast.client.impl.protocol.ClientMessage.BEGIN_FRAME;
import static com.hazelcast.client.impl.protocol.ClientMessage.END_FRAME;
import static com.hazelcast.client.impl.protocol.codec.builtin.CodecUtil.fastForwardToEndFrame;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.decodeBoolean;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.encodeBoolean;

public final class WanReplicationRefCodec {
    private static final int REPUBLISHING_ENABLED_OFFSET = 0;
    private static final int INITIAL_FRAME_SIZE = REPUBLISHING_ENABLED_OFFSET + Bits.BOOLEAN_SIZE_IN_BYTES;

    private WanReplicationRefCodec() {
    }

    public static void encode(ClientMessage clientMessage, WanReplicationRef wanReplicationRef) {
        clientMessage.add(BEGIN_FRAME);

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeBoolean(initialFrame.content, REPUBLISHING_ENABLED_OFFSET, wanReplicationRef.isRepublishingEnabled());
        clientMessage.add(initialFrame);

        StringCodec.encode(clientMessage, wanReplicationRef.getName());
        StringCodec.encode(clientMessage, wanReplicationRef.getMergePolicy());
        ListMultiFrameCodec.encodeNullable(clientMessage, wanReplicationRef.getFilters(), StringCodec::encode);

        clientMessage.add(END_FRAME);
    }

    public static WanReplicationRef decode(ListIterator<ClientMessage.Frame> iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        boolean republishingEnabled = decodeBoolean(initialFrame.content, REPUBLISHING_ENABLED_OFFSET);

        String name = StringCodec.decode(iterator);
        String mergePolicy = StringCodec.decode(iterator);
        List<String> filters = ListMultiFrameCodec.decodeNullable(iterator, StringCodec::decode);

        fastForwardToEndFrame(iterator);

        return new WanReplicationRef(name, mergePolicy, filters, republishingEnabled);
    }
}
