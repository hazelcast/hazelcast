/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.impl;

import com.hazelcast.cache.CacheEventType;
import com.hazelcast.cache.impl.operation.MutableOperation;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.nio.serialization.Data;

/**
 * Utility class to create {@link CacheEventContext} instances
 */
public final class CacheEventContextUtil {

    private CacheEventContextUtil() {
    }

    public static CacheEventContext createCacheCompleteEvent(int completionId) {
        CacheEventContext cacheEventContext = new CacheEventContext();
        cacheEventContext.setEventType(CacheEventType.COMPLETED);
        cacheEventContext.setCompletionId(completionId);
        return cacheEventContext;
    }

    public static CacheEventContext createCacheCompleteEvent(Data dataKey, int completionId) {
        CacheEventContext cacheEventContext = new CacheEventContext();
        cacheEventContext.setEventType(CacheEventType.COMPLETED);
        cacheEventContext.setDataKey(dataKey);
        cacheEventContext.setCompletionId(completionId);
        return cacheEventContext;
    }

    public static CacheEventContext createCacheCompleteEvent(Data dataKey, long expirationTime,
                                                             String origin, int completionId) {
        CacheEventContext cacheEventContext = new CacheEventContext();
        cacheEventContext.setEventType(CacheEventType.COMPLETED);
        cacheEventContext.setDataKey(dataKey);
        cacheEventContext.setCompletionId(completionId);
        cacheEventContext.setOrigin(origin);
        cacheEventContext.setExpirationTime(expirationTime);
        return cacheEventContext;
    }

    public static CacheEventContext createCacheExpiredEvent(Data dataKey, Data dataValue,
                                                            long expirationTime, String origin, int completionId) {
        CacheEventContext cacheEventContext =
                createBaseEventContext(CacheEventType.EXPIRED, dataKey, dataValue,
                                       expirationTime, origin, completionId);
        return cacheEventContext;
    }

    public static CacheEventContext createCacheCreatedEvent(Data dataKey, Data dataValue,
                                                            long expirationTime, String origin, int completionId) {
        CacheEventContext cacheEventContext =
                createBaseEventContext(CacheEventType.CREATED, dataKey, dataValue,
                                       expirationTime, origin, completionId);
        return cacheEventContext;
    }

    @SuppressWarnings("checkstyle:parameternumber")
    public static CacheEventContext createCacheUpdatedEvent(Data dataKey, Data dataValue, Data dataOldValue,
                                                            long creationTime, long expirationTime,
                                                            long lastAccessTime, long accessHit,
                                                            String origin, int completionId) {
        CacheEventContext cacheEventContext =
                createBaseEventContext(CacheEventType.UPDATED, dataKey, dataValue,
                                       expirationTime, origin, completionId);
        cacheEventContext.setDataOldValue(dataOldValue);
        cacheEventContext.setIsOldValueAvailable(true);
        cacheEventContext.setCreationTime(creationTime);
        cacheEventContext.setLastAccessTime(lastAccessTime);
        cacheEventContext.setAccessHit(accessHit);
        return cacheEventContext;
    }

    public static CacheEventContext createCacheUpdatedEvent(Data dataKey, Data dataValue, Data dataOldValue,
                                                            long creationTime, long expirationTime,
                                                            long lastAccessTime, long accessHit) {
        return createCacheUpdatedEvent(dataKey, dataValue, dataOldValue,
                                       creationTime, expirationTime, lastAccessTime, accessHit,
                                       null, MutableOperation.IGNORE_COMPLETION);
    }

    public static CacheEventContext createCacheRemovedEvent(Data dataKey, Data dataValue,
                                                            long expirationTime, String origin, int completionId) {
        CacheEventContext cacheEventContext =
                createBaseEventContext(CacheEventType.REMOVED, dataKey, dataValue,
                                       expirationTime, origin, completionId);
        return cacheEventContext;
    }

    public static CacheEventContext createCacheRemovedEvent(Data dataKey) {
        return createCacheRemovedEvent(dataKey, null, CacheRecord.TIME_NOT_AVAILABLE,
                                       null, MutableOperation.IGNORE_COMPLETION);
    }

    public static CacheEventContext createBaseEventContext(CacheEventType eventType, Data dataKey, Data dataValue,
                                                           long expirationTime, String origin, int completionId) {
        CacheEventContext cacheEventContext = new CacheEventContext();
        cacheEventContext.setEventType(eventType);
        cacheEventContext.setDataKey(dataKey);
        cacheEventContext.setDataValue(dataValue);
        cacheEventContext.setExpirationTime(expirationTime);
        cacheEventContext.setOrigin(origin);
        cacheEventContext.setCompletionId(completionId);
        return cacheEventContext;
    }

}
