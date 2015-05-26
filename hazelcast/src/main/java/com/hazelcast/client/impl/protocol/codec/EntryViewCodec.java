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
import com.hazelcast.map.impl.SimpleEntryView;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.serialization.Data;

public final class EntryViewCodec {

    private EntryViewCodec() {
    }

    public static SimpleEntryView decode(ClientMessage clientMessage) {
        SimpleEntryView<Data, Data> dataEntryView = new SimpleEntryView<Data, Data>();
        dataEntryView.setKey(clientMessage.getData());
        dataEntryView.setValue(clientMessage.getData());
        dataEntryView.setCost(clientMessage.getLong());
        dataEntryView.setCreationTime(clientMessage.getLong());
        dataEntryView.setExpirationTime(clientMessage.getLong());
        dataEntryView.setHits(clientMessage.getLong());
        dataEntryView.setLastAccessTime(clientMessage.getLong());
        dataEntryView.setLastStoredTime(clientMessage.getLong());
        dataEntryView.setLastUpdateTime(clientMessage.getLong());
        dataEntryView.setVersion(clientMessage.getLong());
        dataEntryView.setEvictionCriteriaNumber(clientMessage.getLong());
        dataEntryView.setTtl(clientMessage.getLong());
        return dataEntryView;
    }

    public static void encode(SimpleEntryView<Data, Data> dataEntryView, ClientMessage clientMessage) {
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


        clientMessage.set(key).set(value).set(cost).set(creationTime).set(expirationTime)
                .set(hits).set(lastAccessTime).set(lastStoredTime).set(lastUpdateTime)
                .set(version).set(evictionCriteriaNumber).set(ttl);
    }


    public static int calculateDataSize(SimpleEntryView<Data, Data> entryView) {
        int dataSize = ClientMessage.HEADER_SIZE;
        Data key = entryView.getKey();
        Data value = entryView.getValue();
        return dataSize
                + ParameterUtil.calculateDataSize(key)
                + ParameterUtil.calculateDataSize(value)
                + Bits.LONG_SIZE_IN_BYTES * 10;
    }
}
