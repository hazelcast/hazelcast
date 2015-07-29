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
import com.hazelcast.cache.impl.CacheEventData;
import com.hazelcast.cache.impl.CacheEventDataImpl;
import com.hazelcast.cache.impl.CacheEventType;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.util.ParameterUtil;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.serialization.Data;

@Codec(CacheEventData.class)
public final class CacheEventDataCodec {

    private CacheEventDataCodec() {
    }

    public static CacheEventData decode(ClientMessage clientMessage) {
        int typeId = clientMessage.getInt();
        String name = clientMessage.getStringUtf8();
        Data key = clientMessage.getData();
        boolean value_isNull = clientMessage.getBoolean();
        Data value = null;
        if (!value_isNull) {
            value = clientMessage.getData();
        }

        boolean dataOldValue_isNull = clientMessage.getBoolean();
        Data oldValue = null;
        if (!dataOldValue_isNull) {
            oldValue = clientMessage.getData();
        }
        boolean isOldValueAvailable = clientMessage.getBoolean();
        return new CacheEventDataImpl(name, CacheEventType.getByType(typeId), key, value, oldValue, isOldValueAvailable);
    }

    public static void encode(CacheEventData cacheEventData, ClientMessage clientMessage) {
        clientMessage.set(cacheEventData.getCacheEventType().getType());
        clientMessage.set(cacheEventData.getName());
        clientMessage.set(cacheEventData.getDataKey());

        Data dataValue = cacheEventData.getDataValue();
        boolean dataValue_isNull = dataValue == null;
        clientMessage.set(dataValue_isNull);
        if (!dataValue_isNull) {
            clientMessage.set(dataValue);
        }
        Data dataOldValue = cacheEventData.getDataOldValue();
        boolean dataOldValue_isNull = dataOldValue == null;
        clientMessage.set(dataOldValue_isNull);
        if (!dataOldValue_isNull) {
            clientMessage.set(dataOldValue);
        }

        clientMessage.set(cacheEventData.isOldValueAvailable());
    }

    public static int calculateDataSize(CacheEventData cacheEventData) {
        int dataSize = Bits.INT_SIZE_IN_BYTES;
        dataSize += ParameterUtil.calculateDataSize(cacheEventData.getName());
        dataSize += ParameterUtil.calculateDataSize(cacheEventData.getDataKey());
        Data dataValue = cacheEventData.getDataValue();
        if (dataValue == null) {
            dataSize += Bits.BOOLEAN_SIZE_IN_BYTES;
        } else {
            dataSize += ParameterUtil.calculateDataSize(dataValue);
        }
        Data dataOldValue = cacheEventData.getDataOldValue();
        if (dataOldValue == null) {
            dataSize += Bits.BOOLEAN_SIZE_IN_BYTES;
        } else {
            dataSize += ParameterUtil.calculateDataSize(dataOldValue);
        }
        dataSize += Bits.BOOLEAN_SIZE_IN_BYTES;//isOldValueAvailable
        return dataSize;
    }
}
