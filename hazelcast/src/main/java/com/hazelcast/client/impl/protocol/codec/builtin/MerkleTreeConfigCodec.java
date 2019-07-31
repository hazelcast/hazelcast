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
import com.hazelcast.config.MerkleTreeConfig;
import com.hazelcast.nio.Bits;

import java.util.ListIterator;

import static com.hazelcast.client.impl.protocol.ClientMessage.BEGIN_FRAME;
import static com.hazelcast.client.impl.protocol.ClientMessage.END_FRAME;
import static com.hazelcast.client.impl.protocol.codec.builtin.CodecUtil.fastForwardToEndFrame;

public final class MerkleTreeConfigCodec {

    private static final int ENABLED_OFFSET = 0;
    private static final int DEPTH_OFFSET = ENABLED_OFFSET + Bits.BOOLEAN_SIZE_IN_BYTES;
    private static final int INITIAL_FRAME_SIZE = DEPTH_OFFSET + Bits.INT_SIZE_IN_BYTES;

    private MerkleTreeConfigCodec() {
    }

    public static void encode(ClientMessage clientMessage, MerkleTreeConfig merkleTreeConfig) {
        clientMessage.addFrame(BEGIN_FRAME);

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        FixedSizeTypesCodec.encodeBoolean(initialFrame.content, ENABLED_OFFSET, merkleTreeConfig.isEnabled());
        FixedSizeTypesCodec.encodeInt(initialFrame.content, DEPTH_OFFSET, merkleTreeConfig.getDepth());
        clientMessage.addFrame(initialFrame);

        clientMessage.addFrame(END_FRAME);

    }

    public static MerkleTreeConfig decode(ListIterator<ClientMessage.Frame> iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        boolean enabled = FixedSizeTypesCodec.decodeBoolean(initialFrame.content, ENABLED_OFFSET);
        int depth = FixedSizeTypesCodec.decodeInt(initialFrame.content, DEPTH_OFFSET);

        fastForwardToEndFrame(iterator);

        MerkleTreeConfig merkleTreeConfig = new MerkleTreeConfig();
        merkleTreeConfig.setEnabled(enabled);
        merkleTreeConfig.setDepth(depth);
        return merkleTreeConfig;
    }
}
