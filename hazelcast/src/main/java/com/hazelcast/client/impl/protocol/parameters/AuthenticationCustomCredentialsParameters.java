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
import com.hazelcast.client.impl.protocol.util.BitUtil;
import com.hazelcast.client.impl.protocol.util.ParameterUtil;

/**
 * AuthenticationParameters
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
public class AuthenticationCustomCredentialsParameters {

    /**
     * ClientMessageType of this message
     */
    public static final ClientMessageType TYPE = ClientMessageType.AUTHENTICATION_CUSTOM_REQUEST;
    public byte[] credentials;
    public String uuid;
    public String ownerUuid;
    public boolean isOwnerConnection;

    private AuthenticationCustomCredentialsParameters(ClientMessage flyweight) {
        credentials = flyweight.getByteArray();
        uuid = flyweight.getStringUtf8();
        ownerUuid = flyweight.getStringUtf8();
        isOwnerConnection = flyweight.getBoolean();
    }

    /**
     * Decode input byte array data into parameters
     * @param flyweight
     * @return AuthenticationCustomParameters
     */
    public static AuthenticationCustomCredentialsParameters decode(ClientMessage flyweight) {
        return new AuthenticationCustomCredentialsParameters(flyweight);
    }

    /**
     * Encode parameters into byte array, i.e. ClientMessage
     * @param credentials
     * @param uuid
     * @param ownerUuid
     * @param isOwnerConnection
     * @return encoded ClientMessage
     */
    public static ClientMessage encode(byte[] credentials, String uuid, String ownerUuid,
                                       boolean isOwnerConnection) {
        final int requiredDataSize = calculateDataSize(credentials, uuid, ownerUuid, isOwnerConnection);
        ClientMessage clientMessage = ClientMessage.createForEncode(requiredDataSize);
        clientMessage.ensureCapacity(requiredDataSize);
        clientMessage.setMessageType(TYPE.id());
        clientMessage.set(credentials).set(uuid).set(ownerUuid).set(isOwnerConnection);
        clientMessage.updateFrameLength();
        return clientMessage;
    }

    /**
     * sample data size estimation
     *
     * @return size
     */
    public static int calculateDataSize(byte[] credentials, String uuid, String ownerUuid, boolean isOwnerConnection) {
        return ClientMessage.HEADER_SIZE//
                + ParameterUtil.calculateByteArrayDataSize(credentials)
                + ParameterUtil.calculateStringDataSize(uuid)
                + ParameterUtil.calculateStringDataSize(ownerUuid)
                + BitUtil.SIZE_OF_BYTE;
    }

}
