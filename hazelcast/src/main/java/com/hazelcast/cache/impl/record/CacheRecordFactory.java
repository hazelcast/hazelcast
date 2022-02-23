/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;

/**
 * Provides factory for {@link com.hazelcast.cache.impl.record.CacheRecord}.
 * <p>Key, value and expiryTime are packed into a subclass of
 * {@link com.hazelcast.cache.impl.record.AbstractCacheRecord}
 * depending on the configured inMemoryFormat.</p>
 */
public class CacheRecordFactory<R extends CacheRecord> {

    protected InMemoryFormat inMemoryFormat;
    protected SerializationService serializationService;

    public CacheRecordFactory(InMemoryFormat inMemoryFormat, SerializationService serializationService) {
        this.inMemoryFormat = inMemoryFormat;
        this.serializationService = serializationService;
    }

    public R newRecordWithExpiry(Object value, long creationTime, long expiryTime) {
        final R record;
        switch (inMemoryFormat) {
            case BINARY:
                Data dataValue = serializationService.toData(value);
                record = (R) createCacheDataRecord(dataValue, creationTime, expiryTime);
                break;
            case OBJECT:
                Object objectValue = serializationService.toObject(value);
                record = (R) createCacheObjectRecord(objectValue, creationTime, expiryTime);
                break;
            case NATIVE:
                throw new IllegalArgumentException("Native storage format is supported in Hazelcast Enterprise only. "
                        + "Make sure you have Hazelcast Enterprise JARs on your classpath!");
            default:
                throw new IllegalArgumentException("Invalid storage format: " + inMemoryFormat);
        }
        return record;
    }

    protected CacheRecord createCacheDataRecord(Data dataValue, long creationTime, long expiryTime) {
        return new CacheDataRecord(dataValue, creationTime, expiryTime);
    }

    protected CacheRecord createCacheObjectRecord(Object objectValue, long creationTime, long expiryTime) {
        return new CacheObjectRecord(objectValue, creationTime, expiryTime);
    }

    /**
     * Determines whether the Cache Entry associated with this value will expire
     * at the specified time.
     *
     * @param expirationTime the expiration time
     * @param now            the time in milliseconds (since the Epoc)
     * @return true if the value will expire at the specified time
     */
    public static boolean isExpiredAt(long expirationTime, long now) {
        return expirationTime > -1 && expirationTime <= now;
    }
}
