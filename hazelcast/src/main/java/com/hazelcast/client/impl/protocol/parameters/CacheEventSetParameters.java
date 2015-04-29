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

import com.hazelcast.cache.impl.CacheEventData;
import com.hazelcast.cache.impl.CacheEventType;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.ClientMessageType;
import com.hazelcast.client.impl.protocol.util.BitUtil;
import com.hazelcast.client.impl.protocol.util.ParameterUtil;
import com.hazelcast.nio.serialization.Data;

import java.util.HashSet;
import java.util.Set;

@edu.umd.cs.findbugs.annotations.SuppressWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
public class CacheEventSetParameters {

    public static final ClientMessageType TYPE = ClientMessageType.CACHE_EVENT_SET;
    public CacheEventType eventType;
    public Set<CacheEventData> events;
    public int completionId;

    private CacheEventSetParameters(ClientMessage clientMessage) {
        eventType = CacheEventType.getByType(clientMessage.getInt());
        int eventsSize = clientMessage.getInt();
        events = new HashSet<CacheEventData>(eventsSize);
        for(int i=0; i < eventsSize; i++) {
            final CacheEventData cacheEventData = CacheEventDataCodec.decode(clientMessage);
            events.add(cacheEventData);
        }
        completionId = clientMessage.getInt();
    }

    public static CacheEventSetParameters decode(ClientMessage clientMessage) {
        return new CacheEventSetParameters(clientMessage);
    }

    public static ClientMessage encode(CacheEventType eventType, Set<CacheEventData> events, int completionId) {
        final int requiredDataSize = calculateDataSize(eventType, events, completionId);
        ClientMessage clientMessage = ClientMessage.createForEncode(requiredDataSize);
        clientMessage.setMessageType(TYPE.id());
        clientMessage.ensureCapacity(requiredDataSize);

        clientMessage.set(eventType.getType());

        clientMessage.set(events.size());
        for(CacheEventData ced: events) {
            CacheEventDataCodec.encode(ced, clientMessage);
        }
        clientMessage.set(completionId);

        clientMessage.addFlag(ClientMessage.LISTENER_EVENT_FLAG);
        clientMessage.updateFrameLength();
        return clientMessage;
    }

    public static int calculateDataSize(CacheEventType eventType, Set<CacheEventData> events, int completionId) {
        int dataSize = ClientMessage.HEADER_SIZE;
        dataSize += BitUtil.SIZE_OF_INT;//eventType
        for(CacheEventData ced: events) {
            dataSize += CacheEventDataCodec.calculateDataSize(ced);
        }
        dataSize += BitUtil.SIZE_OF_INT;//completionId
        return dataSize;
    }

}
