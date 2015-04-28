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


@edu.umd.cs.findbugs.annotations.SuppressWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
public final class TransactionCommitParameters {


    public static final ClientMessageType TYPE = ClientMessageType.TRANSACTION_COMMIT;
    public String transactionId;
    public long threadId;
    public boolean prepareAndCommit;

    private TransactionCommitParameters(ClientMessage clientMessage) {
        transactionId = clientMessage.getStringUtf8();
        threadId = clientMessage.getLong();
        prepareAndCommit = clientMessage.getBoolean();
    }

    public static TransactionCommitParameters decode(ClientMessage clientMessage) {
        return new TransactionCommitParameters(clientMessage);
    }

    public static ClientMessage encode(long transactionId, String threadId, boolean prepareAndCommit) {
        final int requiredDataSize = calculateDataSize(transactionId, threadId, prepareAndCommit);
        ClientMessage clientMessage = ClientMessage.createForEncode(requiredDataSize);
        clientMessage.ensureCapacity(requiredDataSize);
        clientMessage.set(transactionId).set(threadId).set(prepareAndCommit);
        clientMessage.setMessageType(TYPE.id());
        clientMessage.updateFrameLength();
        return clientMessage;
    }

    public static int calculateDataSize(long transactionId, String threadId, boolean prepareAndCommit) {
        return ClientMessage.HEADER_SIZE
                + BitUtil.SIZE_OF_LONG
                + ParameterUtil.calculateStringDataSize(threadId)
                + BitUtil.SIZE_OF_BOOLEAN ;
    }


}
