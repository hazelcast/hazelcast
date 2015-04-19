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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@edu.umd.cs.findbugs.annotations.SuppressWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
public class MemberListResultParameters {

    public static final ClientMessageType TYPE = ClientMessageType.MEMBER_LIST_RESULT;

    public Collection<com.hazelcast.client.impl.MemberImpl> memberList;

    public MemberListResultParameters(ClientMessage clientMessage) {
        int size = clientMessage.getInt();
        List<com.hazelcast.client.impl.MemberImpl> members = new ArrayList<com.hazelcast.client.impl.MemberImpl>(size);
        for (int i = 0; i < size; i++) {
            members.add(MemberCodec.decode(clientMessage));
        }
        memberList = members;
    }

    public static MemberListResultParameters decode(ClientMessage clientMessage) {
        return new MemberListResultParameters(clientMessage);
    }

    public static ClientMessage encode(Collection<com.hazelcast.instance.MemberImpl> members) {

        final int requiredDataSize = calculateDataSize(members);
        ClientMessage clientMessage = ClientMessage.createForEncode(requiredDataSize);
        clientMessage.setMessageType(TYPE.id());
        clientMessage.ensureCapacity(requiredDataSize);

        clientMessage.set(members.size());
        for (com.hazelcast.instance.MemberImpl m : members) {
            MemberCodec.encode(m, clientMessage);
        }

        clientMessage.setFlags(ClientMessage.LISTENER_EVENT_FLAG);
        clientMessage.updateFrameLength();
        return clientMessage;

    }

    public static int calculateDataSize(Collection<com.hazelcast.instance.MemberImpl> members) {
        int dataSize = ClientMessage.HEADER_SIZE;
        for (com.hazelcast.instance.MemberImpl m : members) {
            dataSize += MemberCodec.calculateDataSize(m);
        }
        return dataSize;
    }

}
