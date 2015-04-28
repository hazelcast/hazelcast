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
import com.hazelcast.transaction.impl.SerializableXID;

@edu.umd.cs.findbugs.annotations.SuppressWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
public final class TransactionRecoverParameters {


    public static final ClientMessageType TYPE = ClientMessageType.TRANSACTION_RECOVER;
    public SerializableXID xid;
    public boolean commit;

    private TransactionRecoverParameters(ClientMessage clientMessage) {
        xid = XIDCodec.decode(clientMessage);
        commit = clientMessage.getBoolean();
    }

    public static TransactionRecoverParameters decode(ClientMessage clientMessage) {
        return new TransactionRecoverParameters(clientMessage);
    }

    public static ClientMessage encode(SerializableXID xid, boolean commit) {
        final int requiredDataSize = calculateDataSize(xid, commit);
        ClientMessage clientMessage = ClientMessage.createForEncode(requiredDataSize);
        clientMessage.ensureCapacity(requiredDataSize);
        XIDCodec.encode(xid, clientMessage);
        clientMessage.set(commit);
        clientMessage.setMessageType(TYPE.id());
        clientMessage.updateFrameLength();
        return clientMessage;
    }

    public static int calculateDataSize(SerializableXID xid, boolean commit) {
        return ClientMessage.HEADER_SIZE + BitUtil.SIZE_OF_BOOLEAN + XIDCodec.calculateDataSize(xid);
    }


}


