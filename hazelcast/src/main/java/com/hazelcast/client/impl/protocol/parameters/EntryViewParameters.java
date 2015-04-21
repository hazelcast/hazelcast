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
import com.hazelcast.nio.serialization.Data;

@edu.umd.cs.findbugs.annotations.SuppressWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
public class EntryViewParameters {


    /**
     * ClientMessageType of this message
     */
    public static final ClientMessageType TYPE = ClientMessageType.ENTRY_VIEW;
    public Data key;
    public Data value;
    public long cost;
    public long creationTime;
    public long expirationTime;
    public long hits;
    public long lastAccessTime;
    public long lastStoredTime;
    public long lastUpdateTime;
    public long version;
    public long evictionCriteriaNumber;
    public long ttl;

    private EntryViewParameters(ClientMessage flyweight) {
        key = flyweight.getData();
        value = flyweight.getData();
        cost = flyweight.getLong();
        creationTime = flyweight.getLong();
        expirationTime = flyweight.getLong();
        hits = flyweight.getLong();
        lastAccessTime = flyweight.getLong();
        lastStoredTime = flyweight.getLong();
        lastUpdateTime = flyweight.getLong();
        version = flyweight.getLong();
        evictionCriteriaNumber = flyweight.getLong();
        ttl = flyweight.getLong();
    }

    public static EntryViewParameters decode(ClientMessage flyweight) {
        return new EntryViewParameters(flyweight);
    }

    public static ClientMessage encode(Data key, Data value, long cost, long creationTime,
                                       long expirationTime, long hits, long lastAccessTime,
                                       long lastStoredTime, long lastUpdateTime, long version,
                                       long evictionCriteriaNumber, long ttl) {
        final int requiredDataSize = calculateDataSize(key, value, cost, creationTime,
                expirationTime, hits, lastAccessTime,
                lastStoredTime, lastUpdateTime, version,
                evictionCriteriaNumber, ttl);
        ClientMessage clientMessage = ClientMessage.createForEncode(requiredDataSize);
        clientMessage.ensureCapacity(requiredDataSize);
        clientMessage.setMessageType(TYPE.id());
        clientMessage.set(key).set(value).set(cost).set(creationTime).set(expirationTime)
                .set(hits).set(lastAccessTime).set(lastStoredTime).set(lastUpdateTime)
                .set(version).set(evictionCriteriaNumber).set(ttl);
        clientMessage.updateFrameLength();
        return clientMessage;
    }

    /**
     * sample data size estimation
     *
     * @return size
     */
    public static int calculateDataSize(Data key, Data value, long cost, long creationTime,
                                        long expirationTime, long hits, long lastAccessTime,
                                        long lastStoredTime, long lastUpdateTime, long version,
                                        long evictionCriteriaNumber, long ttl) {
        return ClientMessage.HEADER_SIZE//
                + ParameterUtil.calculateDataSize(key)
                + ParameterUtil.calculateDataSize(value)
                + BitUtil.SIZE_OF_LONG * 10;
    }

}
