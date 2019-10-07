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

package com.hazelcast.client.impl.protocol.codec.custom;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.Generated;
import com.hazelcast.client.impl.protocol.codec.builtin.*;

import static com.hazelcast.client.impl.protocol.codec.builtin.CodecUtil.fastForwardToEndFrame;
import static com.hazelcast.client.impl.protocol.ClientMessage.*;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.*;

@Generated("5ba9b3e42fee60fcf448f8a59a2ed831")
public final class MemberCodec {
    private static final int UUID_FIELD_OFFSET = 0;
    private static final int LITE_MEMBER_FIELD_OFFSET = UUID_FIELD_OFFSET + UUID_SIZE_IN_BYTES;
    private static final int INITIAL_FRAME_SIZE = LITE_MEMBER_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;

    private MemberCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.cluster.Member member) {
        clientMessage.add(BEGIN_FRAME.copy());

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeUUID(initialFrame.content, UUID_FIELD_OFFSET, member.getUuid());
        encodeBoolean(initialFrame.content, LITE_MEMBER_FIELD_OFFSET, member.isLiteMember());
        clientMessage.add(initialFrame);

        AddressCodec.encode(clientMessage, member.getAddress());
        MapCodec.encode(clientMessage, member.getAttributes(), StringCodec::encode, StringCodec::encode);

        clientMessage.add(END_FRAME.copy());
    }

    public static com.hazelcast.client.impl.MemberImpl decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        java.util.UUID uuid = decodeUUID(initialFrame.content, UUID_FIELD_OFFSET);
        boolean liteMember = decodeBoolean(initialFrame.content, LITE_MEMBER_FIELD_OFFSET);

        com.hazelcast.cluster.Address address = AddressCodec.decode(iterator);
        java.util.Map<java.lang.String, java.lang.String> attributes = MapCodec.decode(iterator, StringCodec::decode, StringCodec::decode);

        fastForwardToEndFrame(iterator);

        return new com.hazelcast.client.impl.MemberImpl(address, uuid, attributes, liteMember);
    }
}
