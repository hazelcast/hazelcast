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

import com.hazelcast.client.impl.client.DistributedObjectInfo;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.ClientMessageType;
import com.hazelcast.client.impl.protocol.util.BitUtil;

import java.util.ArrayList;
import java.util.Collection;

@edu.umd.cs.findbugs.annotations.SuppressWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
public class GetDistributedObjectResultParameters {

    public static final ClientMessageType TYPE = ClientMessageType.GET_DISTRIBUTED_OBJECT;
    public Collection<DistributedObjectInfo> infoCollection;

    private GetDistributedObjectResultParameters(ClientMessage flyweight) {
        int size = flyweight.getInt();
        infoCollection = new ArrayList<DistributedObjectInfo>(size);
        for (int i = 0; i < size; i++) {
            infoCollection.add(DistributedObjectInfoCodec.decode(flyweight));
        }
    }

    public static GetDistributedObjectResultParameters decode(ClientMessage flyweight) {
        return new GetDistributedObjectResultParameters(flyweight);
    }

    public static ClientMessage encode(Collection<DistributedObjectInfo> infoCollection) {
        final int requiredDataSize = calculateDataSize(infoCollection);
        ClientMessage clientMessage = ClientMessage.createForEncode(requiredDataSize);
        clientMessage.setMessageType(TYPE.id());
        int size = infoCollection.size();
        clientMessage.set(size);
        for (DistributedObjectInfo distributedObjectInfo : infoCollection) {
            DistributedObjectInfoCodec.encode(distributedObjectInfo, clientMessage);
        }
        clientMessage.ensureCapacity(requiredDataSize);
        clientMessage.updateFrameLength();
        return clientMessage;
    }

    /**
     * sample data size estimation
     *
     * @return size
     */
    public static int calculateDataSize(Collection<DistributedObjectInfo> infoCollection) {
        int size = ClientMessage.HEADER_SIZE + BitUtil.SIZE_OF_INT;
        for (DistributedObjectInfo distributedObjectInfo : infoCollection) {
            size += DistributedObjectInfoCodec.calculateDataSize(distributedObjectInfo);
        }
        return size;
    }
}

