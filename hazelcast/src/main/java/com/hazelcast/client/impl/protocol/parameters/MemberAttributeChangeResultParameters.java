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

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.ClientMessageType;
import com.hazelcast.cluster.client.MemberAttributeChange;

@edu.umd.cs.findbugs.annotations.SuppressWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
public class MemberAttributeChangeResultParameters {
    public static final ClientMessageType TYPE = ClientMessageType.MEMBER_ATTRIBUTE_RESULT;

    public com.hazelcast.client.impl.MemberImpl member;
    public MemberAttributeChange memberAttributeChange;

    public MemberAttributeChangeResultParameters(ClientMessage clientMessage) {
        member = MemberCodec.decode(clientMessage);
        memberAttributeChange = MemberAttributeChangeCodec.decode(clientMessage);
    }

    public static MemberAttributeChangeResultParameters decode(ClientMessage clientMessage) {
        return new MemberAttributeChangeResultParameters(clientMessage);
    }

    public static ClientMessage encode(com.hazelcast.instance.MemberImpl member, MemberAttributeChange memberAttributeChange) {
        final int requiredDataSize = calculateDataSize(member, memberAttributeChange);
        ClientMessage clientMessage = ClientMessage.createForEncode(requiredDataSize);
        clientMessage.setMessageType(TYPE.id());
        clientMessage.ensureCapacity(requiredDataSize);

        MemberCodec.encode(member, clientMessage);
        MemberAttributeChangeCodec.encode(memberAttributeChange, clientMessage);

        clientMessage.setFlags(ClientMessage.LISTENER_EVENT_FLAG);
        clientMessage.updateFrameLength();
        return clientMessage;
    }

    public static int calculateDataSize(com.hazelcast.instance.MemberImpl member, MemberAttributeChange memberAttributeChange) {
        return ClientMessage.HEADER_SIZE//
                + MemberCodec.calculateDataSize(member)//
                + MemberAttributeChangeCodec.calculateDataSize(memberAttributeChange);
    }

}
