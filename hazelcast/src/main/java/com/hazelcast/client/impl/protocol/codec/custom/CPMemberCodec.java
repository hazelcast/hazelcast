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

@Generated("51e2687e2f0c3dbe298bcb7a33dfa3bb")
public final class CPMemberCodec {
    private static final int UUID_FIELD_OFFSET = 0;
    private static final int INITIAL_FRAME_SIZE = UUID_FIELD_OFFSET + UUID_SIZE_IN_BYTES;

    private CPMemberCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.cp.CPMember cPMember) {
        clientMessage.add(BEGIN_FRAME.copy());

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeUUID(initialFrame.content, UUID_FIELD_OFFSET, cPMember.getUuid());
        clientMessage.add(initialFrame);

        AddressCodec.encode(clientMessage, cPMember.getAddress());

        clientMessage.add(END_FRAME.copy());
    }

    public static com.hazelcast.cp.internal.CPMemberInfo decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        java.util.UUID uuid = decodeUUID(initialFrame.content, UUID_FIELD_OFFSET);

        com.hazelcast.cluster.Address address = AddressCodec.decode(iterator);

        fastForwardToEndFrame(iterator);

        return new com.hazelcast.cp.internal.CPMemberInfo(uuid, address);
    }
}
