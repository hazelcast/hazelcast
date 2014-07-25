/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.impl.record;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;

public class CacheRecordFactory {


    private InMemoryFormat inMemoryFormat;
    private SerializationService serializationService;

    public CacheRecordFactory(InMemoryFormat inMemoryFormat, SerializationService serializationService) {
        this.inMemoryFormat = inMemoryFormat;
        this.serializationService = serializationService;
    }

    public CacheRecord newRecord(Data key, Object value) {
        return newRecordWithExpiry(key, value, -1);
    }

    public CacheRecord newRecordWithExpiry(Data key, Object value, long expiryTime) {
        final CacheRecord record;
        switch (inMemoryFormat) {
            case BINARY:
                Data dataValue = serializationService.toData(value);
                record = new CacheDataRecord(key, dataValue, expiryTime);
                break;
            case OBJECT:
                Object objectValue= serializationService.toObject(value);
                record = new CacheObjectRecord(key, objectValue, expiryTime);
                break;
            default:
                throw new IllegalArgumentException("Invalid storage format: " + inMemoryFormat);
        }
        return record;
    }

    /**
     * Determines if the Cache Entry associated with this value would be expired
     * at the specified time
     *
     * @param now time in milliseconds (since the Epoc)
     * @return true if the value would be expired at the specified time
     */
    public static boolean isExpiredAt(long expirationTime, long now) {
        return expirationTime > -1 && expirationTime <= now;
    }

}
