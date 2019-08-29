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

import com.hazelcast.client.impl.MemberImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.cluster.Member;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Bits;

import java.util.ListIterator;
import java.util.Map;
import java.util.UUID;

import static com.hazelcast.client.impl.protocol.ClientMessage.BEGIN_FRAME;
import static com.hazelcast.client.impl.protocol.ClientMessage.END_FRAME;
import static com.hazelcast.client.impl.protocol.codec.builtin.CodecUtil.fastForwardToEndFrame;

public final class MemberCodec {

    private static final int LITE_MEMBER_OFFSET = 0;
    private static final int UUID_OFFSET = LITE_MEMBER_OFFSET + Bits.BOOLEAN_SIZE_IN_BYTES;
    private static final int INITIAL_FRAME_SIZE = UUID_OFFSET + FixedSizeTypesCodec.UUID_SIZE_IN_BYTES;

    private MemberCodec() {
    }

    public static void encode(ClientMessage clientMessage, Member member) {
        clientMessage.add(BEGIN_FRAME);

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        FixedSizeTypesCodec.encodeBoolean(initialFrame.content, LITE_MEMBER_OFFSET, member.isLiteMember());
        FixedSizeTypesCodec.encodeUUID(initialFrame.content, UUID_OFFSET, UUID.fromString(member.getUuid()));
        clientMessage.add(initialFrame);

        AddressCodec.encode(clientMessage, member.getAddress());
        MapCodec.encode(clientMessage, member.getAttributes(), StringCodec::encode, StringCodec::encode);

        clientMessage.add(END_FRAME);
    }

    public static Member decode(ListIterator<ClientMessage.Frame> iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        boolean isLiteMember = FixedSizeTypesCodec.decodeBoolean(initialFrame.content, LITE_MEMBER_OFFSET);
        UUID uuid = FixedSizeTypesCodec.decodeUUID(initialFrame.content, UUID_OFFSET);

        Address address = AddressCodec.decode(iterator);
        Map<String, String> attributes = MapCodec.decodeToMap(iterator, StringCodec::decode, StringCodec::decode);

        fastForwardToEndFrame(iterator);

        return new MemberImpl(address, uuid.toString(), attributes, isLiteMember);
    }
}
