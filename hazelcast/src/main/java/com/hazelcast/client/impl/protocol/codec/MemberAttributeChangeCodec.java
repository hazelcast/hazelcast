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
import com.hazelcast.cluster.MemberAttributeOperationType;
import com.hazelcast.cluster.client.MemberAttributeChange;
import com.hazelcast.nio.Bits;

import static com.hazelcast.cluster.MemberAttributeOperationType.PUT;

public final class MemberAttributeChangeCodec {

    private MemberAttributeChangeCodec() {
    }

    public static MemberAttributeChange decode(ClientMessage clientMessage) {
        String uuid = clientMessage.getStringUtf8();
        String key = clientMessage.getStringUtf8();
        MemberAttributeOperationType operationType = MemberAttributeOperationType.getValue(clientMessage.getInt());
        Object value = null;
        if (operationType == PUT) {
            value = clientMessage.getStringUtf8();
        }
        return new MemberAttributeChange(uuid, operationType, key, value);
    }

    public static void encode(MemberAttributeChange memberAttributeChange, ClientMessage clientMessage) {
        clientMessage.set(memberAttributeChange.getUuid());
        clientMessage.set(memberAttributeChange.getKey());

        MemberAttributeOperationType operationType = memberAttributeChange.getOperationType();
        clientMessage.set(operationType.getId());
        if (operationType == PUT) {
            clientMessage.set(memberAttributeChange.getValue().toString());
        }
    }

    public static int calculateDataSize(MemberAttributeChange memberAttributeChange) {
        if (memberAttributeChange == null) {
            return Bits.BOOLEAN_SIZE_IN_BYTES;
        }
        int dataSize = ParameterUtil.calculateStringDataSize(memberAttributeChange.getUuid());
        dataSize += ParameterUtil.calculateStringDataSize(memberAttributeChange.getKey());
        //operation type
        dataSize += Bits.INT_SIZE_IN_BYTES;
        MemberAttributeOperationType operationType = memberAttributeChange.getOperationType();
        if (operationType == PUT) {
            dataSize += ParameterUtil.calculateStringDataSize(memberAttributeChange.getValue().toString());
        }
        return dataSize;
    }
}
