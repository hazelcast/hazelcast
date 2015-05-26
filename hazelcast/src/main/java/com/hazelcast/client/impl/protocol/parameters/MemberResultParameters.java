/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.parameters;

import com.hazelcast.client.impl.MemberImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.ClientMessageType;
import com.hazelcast.nio.Bits;

@edu.umd.cs.findbugs.annotations.SuppressWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
public class MemberResultParameters {
    public static final int MEMBER_ADDED = 1;
    public static final int MEMBER_REMOVED = 2;

    public static final ClientMessageType TYPE = ClientMessageType.MEMBER_RESULT;

    public com.hazelcast.client.impl.MemberImpl member;
    public int eventType;

    public MemberResultParameters(ClientMessage clientMessage) {
        member = (MemberImpl) MemberCodec.decode(clientMessage);
        eventType = clientMessage.getInt();
    }

    public static MemberResultParameters decode(ClientMessage clientMessage) {
        return new MemberResultParameters(clientMessage);
    }

    public static ClientMessage encode(com.hazelcast.instance.MemberImpl member, int eventType) {
        final int requiredDataSize = calculateDataSize(member, eventType);
        ClientMessage clientMessage = ClientMessage.createForEncode(requiredDataSize);
        clientMessage.setMessageType(TYPE.id());

        MemberCodec.encode(member, clientMessage);
        clientMessage.set(eventType);

        clientMessage.addFlag(ClientMessage.LISTENER_EVENT_FLAG);
        clientMessage.updateFrameLength();
        return clientMessage;

    }

    public static int calculateDataSize(com.hazelcast.instance.MemberImpl member, int eventType) {
        return ClientMessage.HEADER_SIZE//
                + MemberCodec.calculateDataSize(member)//
                + Bits.INT_SIZE_IN_BYTES;
    }

}
