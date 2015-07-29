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

import com.hazelcast.annotation.Codec;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.util.ParameterUtil;
import com.hazelcast.map.impl.querycache.event.DefaultQueryCacheEventData;
import com.hazelcast.map.impl.querycache.event.QueryCacheEventData;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.serialization.Data;

/**
 * Codec that read and writes QueryCacheEventData(event of continous query cache) to client message
 * Used by generated code only.
 */
@Codec(QueryCacheEventData.class)
public final class QueryCacheEventDataCodec {

    private QueryCacheEventDataCodec() {
    }

    public static QueryCacheEventData decode(ClientMessage clientMessage) {
        DefaultQueryCacheEventData queryCacheEventData = new DefaultQueryCacheEventData();
        queryCacheEventData.setSequence(clientMessage.getLong());

        boolean isNullKey = clientMessage.getBoolean();
        if (!isNullKey) {
            queryCacheEventData.setDataKey(clientMessage.getData());
        }

        boolean isNullValue = clientMessage.getBoolean();
        if (!isNullValue) {
            queryCacheEventData.setDataNewValue(clientMessage.getData());
        }

        queryCacheEventData.setEventType(clientMessage.getInt());
        queryCacheEventData.setPartitionId(clientMessage.getInt());
        return queryCacheEventData;
    }

    public static void encode(QueryCacheEventData queryCacheEventData, ClientMessage clientMessage) {
        clientMessage.set(queryCacheEventData.getSequence());

        Data dataKey = queryCacheEventData.getDataKey();
        boolean isNullKey = dataKey == null;
        clientMessage.set(isNullKey);
        if (!isNullKey) {
            clientMessage.set(dataKey);
        }

        Data dataNewValue = queryCacheEventData.getDataNewValue();
        boolean isNullValue = dataNewValue == null;
        clientMessage.set(isNullValue);
        if (!isNullValue) {
            clientMessage.set(dataNewValue);
        }

        clientMessage.set(queryCacheEventData.getEventType());
        clientMessage.set(queryCacheEventData.getPartitionId());
    }

    public static int calculateDataSize(QueryCacheEventData queryCacheEventData) {
        int dataSize = Bits.LONG_SIZE_IN_BYTES;

        dataSize += Bits.BOOLEAN_SIZE_IN_BYTES;
        Data dataKey = queryCacheEventData.getDataKey();
        boolean isNullKey = dataKey == null;
        if (!isNullKey) {
            dataSize += ParameterUtil.calculateDataSize(dataKey);
        }

        dataSize += Bits.BOOLEAN_SIZE_IN_BYTES;
        Data dataNewValue = queryCacheEventData.getDataNewValue();
        boolean isNullValue = dataNewValue == null;
        if (!isNullValue) {
            dataSize += ParameterUtil.calculateDataSize(dataNewValue);
        }

        dataSize += Bits.INT_SIZE_IN_BYTES;
        dataSize += Bits.INT_SIZE_IN_BYTES;
        return dataSize;
    }
}
