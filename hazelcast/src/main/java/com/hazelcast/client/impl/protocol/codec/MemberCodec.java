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

package com.hazelcast.client.impl.protocol.codec;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.util.ParameterUtil;
import com.hazelcast.core.Member;
import com.hazelcast.instance.AbstractMember;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Bits;

import java.util.HashMap;
import java.util.Map;

public final class MemberCodec {

    private MemberCodec() {
    }

    public static Member decode(ClientMessage clientMessage) {
        final Address address = AddressCodec.decode(clientMessage);
        String uuid = clientMessage.getStringUtf8();
        int attributeSize = clientMessage.getInt();
        Map<String, Object> attributes = new HashMap<String, Object>();
        for (int i = 0; i < attributeSize; i++) {
            String key = clientMessage.getStringUtf8();
            String value = clientMessage.getStringUtf8();
            attributes.put(key, value);
        }

        return new com.hazelcast.client.impl.MemberImpl(address, uuid, attributes);
    }

    public static void encode(Member member, ClientMessage clientMessage) {
        AddressCodec.encode(((AbstractMember) member).getAddress(), clientMessage);
        clientMessage.set(member.getUuid());
        Map<String, Object> attributes = new HashMap<String, Object>(member.getAttributes());
        clientMessage.set(attributes.size());
        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
            clientMessage.set(entry.getKey());
            Object value = entry.getValue();
            clientMessage.set(value.toString());

        }
    }

    public static int calculateDataSize(Member member) {
        int dataSize = AddressCodec.calculateDataSize(((AbstractMember) member).getAddress());
        dataSize += ParameterUtil.calculateStringDataSize(member.getUuid());
        dataSize += Bits.INT_SIZE_IN_BYTES;
        Map<String, Object> attributes = member.getAttributes();
        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
            dataSize += ParameterUtil.calculateStringDataSize(entry.getKey());
            Object value = entry.getValue();
            //TODO: this is costly to use toString
            dataSize += ParameterUtil.calculateStringDataSize(value.toString());
        }
        return dataSize;
    }
}
