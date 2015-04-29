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
import com.hazelcast.cache.impl.CacheEventDataImpl;
import com.hazelcast.cache.impl.CacheEventType;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.util.BitUtil;
import com.hazelcast.client.impl.protocol.util.ParameterUtil;
import com.hazelcast.nio.serialization.Data;

public final class CacheEventDataCodec {

    private CacheEventDataCodec() {
    }

    public static CacheEventData decode(ClientMessage clientMessage) {
        int typeId = clientMessage.getInt();
        String name= clientMessage.getStringUtf8();
        Data key= ParameterUtil.convertToDataOrNull(clientMessage.getData());
        Data value= ParameterUtil.convertToDataOrNull(clientMessage.getData());
        Data oldValue = ParameterUtil.convertToDataOrNull(clientMessage.getData());
        boolean isOldValueAvailable= clientMessage.getBoolean();
        return new CacheEventDataImpl(name, CacheEventType.getByType(typeId), key,value, oldValue, isOldValueAvailable);
    }

    public static void encode(CacheEventData cacheEventData, ClientMessage clientMessage) {
        clientMessage.set(cacheEventData.getCacheEventType().getType());
        clientMessage.set(cacheEventData.getName());
        clientMessage.set(ParameterUtil.convertToNonNullData(cacheEventData.getDataKey()));
        clientMessage.set(ParameterUtil.convertToNonNullData(cacheEventData.getDataValue()));
        clientMessage.set(ParameterUtil.convertToNonNullData(cacheEventData.getDataOldValue()));
        clientMessage.set(cacheEventData.isOldValueAvailable());
    }

    public static int calculateDataSize(CacheEventData cacheEventData) {
        int dataSize = BitUtil.SIZE_OF_INT;//type
        dataSize += ParameterUtil.calculateStringDataSize(cacheEventData.getName());
        dataSize += ParameterUtil.calculateDataSize(ParameterUtil.convertToNonNullData(cacheEventData.getDataKey()));
        dataSize += ParameterUtil.calculateDataSize(ParameterUtil.convertToNonNullData(cacheEventData.getDataValue()));
        dataSize += ParameterUtil.calculateDataSize(ParameterUtil.convertToNonNullData(cacheEventData.getDataOldValue()));
        dataSize += BitUtil.SIZE_OF_BOOLEAN;//isOldValueAvailable
        return dataSize;
    }
}
