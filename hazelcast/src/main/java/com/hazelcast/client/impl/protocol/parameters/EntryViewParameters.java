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
import com.hazelcast.map.impl.SimpleEntryView;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.serialization.Data;

@edu.umd.cs.findbugs.annotations.SuppressWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
public class EntryViewParameters {

    /**
     * ClientMessageType of this message
     */
    public static final ClientMessageType TYPE = ClientMessageType.ENTRY_VIEW;

    public SimpleEntryView<Data, Data> dataEntryView;

    private EntryViewParameters(ClientMessage flyweight) {
        boolean isNull = flyweight.getBoolean();
        if (isNull) {
            return;
        }
        dataEntryView = new SimpleEntryView<Data, Data>();
        dataEntryView.setKey(flyweight.getData());
        dataEntryView.setValue(flyweight.getData());
        dataEntryView.setCost(flyweight.getLong());
        dataEntryView.setCreationTime(flyweight.getLong());
        dataEntryView.setExpirationTime(flyweight.getLong());
        dataEntryView.setHits(flyweight.getLong());
        dataEntryView.setLastAccessTime(flyweight.getLong());
        dataEntryView.setLastStoredTime(flyweight.getLong());
        dataEntryView.setLastUpdateTime(flyweight.getLong());
        dataEntryView.setVersion(flyweight.getLong());
        dataEntryView.setEvictionCriteriaNumber(flyweight.getLong());
        dataEntryView.setTtl(flyweight.getLong());
    }

    public static EntryViewParameters decode(ClientMessage flyweight) {
        return new EntryViewParameters(flyweight);
    }

    public static ClientMessage encode(SimpleEntryView<Data, Data> dataEntryView) {
        final int requiredDataSize = calculateDataSize(dataEntryView);


        ClientMessage clientMessage = ClientMessage.createForEncode(requiredDataSize);
        clientMessage.setMessageType(TYPE.id());

        boolean isNull;
        if (dataEntryView == null) {
            isNull = true;
            clientMessage.set(isNull);
            clientMessage.updateFrameLength();
            return clientMessage;
        }
        isNull = false;

        Data key = dataEntryView.getKey();
        Data value = dataEntryView.getValue();
        long cost = dataEntryView.getCost();
        long creationTime = dataEntryView.getCreationTime();
        long expirationTime = dataEntryView.getExpirationTime();
        long hits = dataEntryView.getHits();
        long lastAccessTime = dataEntryView.getLastAccessTime();
        long lastStoredTime = dataEntryView.getLastStoredTime();
        long lastUpdateTime = dataEntryView.getLastUpdateTime();
        long version = dataEntryView.getVersion();
        long ttl = dataEntryView.getTtl();
        long evictionCriteriaNumber = dataEntryView.getEvictionCriteriaNumber();


        clientMessage.set(isNull).set(key).set(value).set(cost).set(creationTime).set(expirationTime)
                .set(hits).set(lastAccessTime).set(lastStoredTime).set(lastUpdateTime)
                .set(version).set(evictionCriteriaNumber).set(ttl);
        clientMessage.updateFrameLength();
        return clientMessage;
    }

    public static int calculateDataSize(SimpleEntryView<Data, Data> dataEntryView) {
        int dataSize = ClientMessage.HEADER_SIZE;
        dataSize += Bits.BOOLEAN_SIZE_IN_BYTES;
        if (dataEntryView == null) {
            return dataSize;
        }
        Data key = dataEntryView.getKey();
        Data value = dataEntryView.getValue();
        return dataSize
                + ParameterUtil.calculateDataSize(key)
                + ParameterUtil.calculateDataSize(value)
                + Bits.LONG_SIZE_IN_BYTES * 10;
    }

}
