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
import com.hazelcast.client.impl.protocol.util.ParameterUtil;
import com.hazelcast.nio.Address;

import java.net.UnknownHostException;

/**
 * AuthenticationResultParameters
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
public class AuthenticationResultParameters {

    public static final ClientMessageType TYPE = ClientMessageType.AUTHENTICATION_RESULT;
    public Address address;
    public String uuid;
    public String ownerUuid;

    private AuthenticationResultParameters(ClientMessage flyweight) throws UnknownHostException {
        address = ParameterUtil.decodeAddress(flyweight);
        uuid = flyweight.getStringUtf8();
        ownerUuid = flyweight.getStringUtf8();
    }

    public static AuthenticationResultParameters decode(ClientMessage flyweight) throws UnknownHostException {
        return new AuthenticationResultParameters(flyweight);
    }

    public static ClientMessage encode(Address address, String uuid, String ownerUuid) {
        final int requiredDataSize = calculateDataSize(address, uuid, ownerUuid);
        ClientMessage clientMessage = ClientMessage.createForEncode(requiredDataSize);
        clientMessage.ensureCapacity(requiredDataSize);
        clientMessage.setMessageType(TYPE.id());
        ParameterUtil.encodeAddress(clientMessage, address);
        clientMessage.set(uuid).set(ownerUuid);

        clientMessage.updateFrameLength();
        return clientMessage;
    }

    /**
     * sample data size estimation
     *
     * @return size
     */
    public static int calculateDataSize(Address address, String uuid, String ownerUuid) {
        return ClientMessage.HEADER_SIZE
                + ParameterUtil.calculateAddressDataSize(address)
                + ParameterUtil.calculateStringDataSize(uuid)
                + ParameterUtil.calculateStringDataSize(ownerUuid);
    }

}
