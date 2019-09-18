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
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.nio.Bits;

import java.util.ListIterator;

import static com.hazelcast.client.impl.protocol.ClientMessage.BEGIN_FRAME;
import static com.hazelcast.client.impl.protocol.ClientMessage.END_FRAME;
import static com.hazelcast.client.impl.protocol.codec.builtin.CodecUtil.fastForwardToEndFrame;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.decodeLong;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.encodeLong;

public final class RaftGroupIdCodec {
    private static final int SEED_OFFSET = 0;
    private static final int COMMIT_INDEX_OFFSET = SEED_OFFSET + Bits.LONG_SIZE_IN_BYTES;
    private static final int INITIAL_FRAME_SIZE = COMMIT_INDEX_OFFSET + Bits.LONG_SIZE_IN_BYTES;

    private RaftGroupIdCodec() {
    }

    public static void encode(ClientMessage clientMessage, RaftGroupId groupId) {
        clientMessage.add(BEGIN_FRAME);

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeLong(initialFrame.content, SEED_OFFSET, groupId.seed());
        encodeLong(initialFrame.content, COMMIT_INDEX_OFFSET, groupId.id());
        clientMessage.add(initialFrame);

        StringCodec.encode(clientMessage, groupId.name());

        clientMessage.add(END_FRAME);
    }

    public static RaftGroupId decode(ListIterator<ClientMessage.Frame> iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        long seed = decodeLong(initialFrame.content, SEED_OFFSET);
        long commitIndex = decodeLong(initialFrame.content, COMMIT_INDEX_OFFSET);

        String name = StringCodec.decode(iterator);

        fastForwardToEndFrame(iterator);

        return new RaftGroupId(name, seed, commitIndex);
    }
}
