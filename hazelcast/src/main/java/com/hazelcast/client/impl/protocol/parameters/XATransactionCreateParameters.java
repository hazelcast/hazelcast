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
import com.hazelcast.nio.Bits;
import com.hazelcast.transaction.impl.xa.SerializableXID;

@edu.umd.cs.findbugs.annotations.SuppressWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
public class XATransactionCreateParameters {

    public static final ClientMessageType TYPE = ClientMessageType.XA_TRANSACTION_CREATE;
    public SerializableXID xid;
    public long timeout;


    private XATransactionCreateParameters(ClientMessage clientMessage) {
        xid = SerializableXIDCodec.decode(clientMessage);
        timeout = clientMessage.getLong();
    }

    public static XATransactionCreateParameters decode(ClientMessage clientMessage) {
        return new XATransactionCreateParameters(clientMessage);
    }

    public static ClientMessage encode(SerializableXID xid, long timeout) {
        final int requiredDataSize = calculateDataSize(xid, timeout);
        ClientMessage clientMessage = ClientMessage.createForEncode(requiredDataSize);
        SerializableXIDCodec.encode(xid, clientMessage);
        clientMessage.set(timeout);
        clientMessage.setMessageType(TYPE.id());
        clientMessage.updateFrameLength();
        return clientMessage;
    }

    public static int calculateDataSize(SerializableXID xid, long timeout) {
        return ClientMessage.HEADER_SIZE
                + SerializableXIDCodec.calculateDataSize(xid)
                + Bits.LONG_SIZE_IN_BYTES;
    }

}
