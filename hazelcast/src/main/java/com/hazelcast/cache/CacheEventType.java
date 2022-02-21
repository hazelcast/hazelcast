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

package com.hazelcast.cache;

import javax.cache.event.EventType;

/**
 * Hazelcast JCache implementation's internal event types. This enum
 * type is an extension to {@link EventType} to define more event types.
 */
public enum CacheEventType {

    /**
     * An event type indicating that the cache entry was created.
     */
    CREATED(1),

    /**
     * An event type indicating that the cache entry was updated, i.e. a previous
     * mapping existed.
     */
    UPDATED(2),

    /**
     * An event type indicating that the cache entry was removed.
     */
    REMOVED(3),

    /**
     * An event type indicating that the cache entry has expired.
     */
    EXPIRED(4),

    /**
     * An event type indicating that the cache entry has evicted.
     */
    EVICTED(5),

    /**
     * An event type indicating that the cache entry has invalidated for Near Cache invalidation.
     */
    INVALIDATED(6),

    /**
     * An event type indicating that the cache operation has completed.
     */
    COMPLETED(7),

    /**
     * An event type indicating that the expiration time of cache record has been updated
     */
    EXPIRATION_TIME_UPDATED(8),

    /**
     * An event type indicating that partition loss is detected in given cache with name
     */
    PARTITION_LOST(9);

    private static final int MIN_TYPE_ID = CREATED.type;
    private static final int MAX_TYPE_ID = PARTITION_LOST.type;
    private static final CacheEventType[] CACHED_VALUES = values();

    private int type;

    CacheEventType(final int type) {
        this.type = type;
    }

    /**
     * @return unique ID of the event type.
     */
    public int getType() {
        return type;
    }

    public static CacheEventType getByType(final int eventType) {
        if (MIN_TYPE_ID <= eventType && eventType <= MAX_TYPE_ID) {
            return CACHED_VALUES[eventType - 1];
        }

        return null;
    }

    /**
     * Converts a {@link CacheEventType} into {@link EventType}.
     * Just an Enum type conversion takes place.
     *
     * @param cacheEventType a {@link CacheEventType}.
     * @return same event of {@link EventType} enum.
     */
    public static EventType convertToEventType(CacheEventType cacheEventType) {
        return EventType.valueOf(cacheEventType.name());
    }

}
