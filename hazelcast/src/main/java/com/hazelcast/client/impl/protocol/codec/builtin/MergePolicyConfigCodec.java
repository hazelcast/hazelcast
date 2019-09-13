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
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.nio.Bits;

import java.util.ListIterator;

import static com.hazelcast.client.impl.protocol.ClientMessage.BEGIN_FRAME;
import static com.hazelcast.client.impl.protocol.ClientMessage.END_FRAME;
import static com.hazelcast.client.impl.protocol.codec.builtin.CodecUtil.fastForwardToEndFrame;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.decodeInt;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.encodeInt;

public final class MergePolicyConfigCodec {
    private static final int BATCH_SIZE_OFFSET = 0;
    private static final int INITIAL_FRAME_SIZE = BATCH_SIZE_OFFSET + Bits.INT_SIZE_IN_BYTES;

    private MergePolicyConfigCodec() {
    }

    public static void encode(ClientMessage clientMessage, MergePolicyConfig config) {
        clientMessage.add(BEGIN_FRAME);
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeInt(initialFrame.content, BATCH_SIZE_OFFSET, config.getBatchSize());
        clientMessage.add(initialFrame);
        StringCodec.encode(clientMessage, config.getPolicy());
        clientMessage.add(END_FRAME);
    }

    public static MergePolicyConfig decode(ListIterator<ClientMessage.Frame> iterator) {
        // begin frame
        iterator.next();
        ClientMessage.Frame initialFrame = iterator.next();
        int batchSize = decodeInt(initialFrame.content, BATCH_SIZE_OFFSET);
        String policy = StringCodec.decode(iterator);
        fastForwardToEndFrame(iterator);
        return new MergePolicyConfig(policy, batchSize);
    }
}
