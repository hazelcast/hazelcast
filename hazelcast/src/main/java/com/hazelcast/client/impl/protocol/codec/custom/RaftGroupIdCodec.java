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

@Generated("b7b05ba2e1ef26ef196ebd2b861074d4")
public final class RaftGroupIdCodec {
    private static final int SEED_FIELD_OFFSET = 0;
    private static final int ID_FIELD_OFFSET = SEED_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int INITIAL_FRAME_SIZE = ID_FIELD_OFFSET + LONG_SIZE_IN_BYTES;

    private RaftGroupIdCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.cp.internal.RaftGroupId raftGroupId) {
        clientMessage.add(BEGIN_FRAME.copy());

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeLong(initialFrame.content, SEED_FIELD_OFFSET, raftGroupId.getSeed());
        encodeLong(initialFrame.content, ID_FIELD_OFFSET, raftGroupId.getId());
        clientMessage.add(initialFrame);

        StringCodec.encode(clientMessage, raftGroupId.getName());

        clientMessage.add(END_FRAME.copy());
    }

    public static com.hazelcast.cp.internal.RaftGroupId decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        long seed = decodeLong(initialFrame.content, SEED_FIELD_OFFSET);
        long id = decodeLong(initialFrame.content, ID_FIELD_OFFSET);

        java.lang.String name = StringCodec.decode(iterator);

        fastForwardToEndFrame(iterator);

        return new com.hazelcast.cp.internal.RaftGroupId(name, seed, id);
    }
}
