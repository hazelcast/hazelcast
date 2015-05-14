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
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.serialization.Data;

@edu.umd.cs.findbugs.annotations.SuppressWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
public class XAFinalizeTransactionParameters {

    public static final ClientMessageType TYPE = ClientMessageType.XA_TRANSACTION_FINALIZE;
    public Data xidData;
    public boolean isCommit;

    private XAFinalizeTransactionParameters(ClientMessage clientMessage) {
        this.xidData = clientMessage.getData();
        this.isCommit = clientMessage.getBoolean();
    }

    public static XAFinalizeTransactionParameters decode(ClientMessage clientMessage) {
        return new XAFinalizeTransactionParameters(clientMessage);
    }

    public static ClientMessage encode(Data xidData, boolean isCommit) {
        final int requiredDataSize = calculateDataSize(xidData, isCommit);
        ClientMessage clientMessage = ClientMessage.createForEncode(requiredDataSize);
        clientMessage.set(xidData);
        clientMessage.set(isCommit);
        clientMessage.setMessageType(TYPE.id());
        clientMessage.updateFrameLength();
        return clientMessage;
    }

    public static int calculateDataSize(Data xidData, boolean isCommit) {
        return ClientMessage.HEADER_SIZE
                + ParameterUtil.calculateDataSize(xidData)
                + Bits.BOOLEAN_SIZE_IN_BYTES;
    }
}
