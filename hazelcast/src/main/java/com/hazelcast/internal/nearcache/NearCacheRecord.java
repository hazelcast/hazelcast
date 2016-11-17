/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.nearcache;

import com.hazelcast.internal.eviction.Evictable;
import com.hazelcast.internal.eviction.Expirable;

/**
 * An expirable and evictable data object which represents a Near Cache entry.
 *
 * Record of {@link NearCacheRecordStore}.
 *
 * @param <V> the type of the value stored by this {@link NearCacheRecord}
 * @see com.hazelcast.internal.eviction.Expirable
 * @see com.hazelcast.internal.eviction.Evictable
 */
public interface NearCacheRecord<V> extends Expirable, Evictable<V> {

    int TIME_NOT_SET = -1;

    /**
     * Sets the value of this {@link NearCacheRecord}.
     *
     * @param value the value for this {@link NearCacheRecord}
     */
    void setValue(V value);

    /**
     * Sets the creation time of this {@link Evictable} in milliseconds.
     *
     * @param time the creation time for this {@link Evictable} in milliseconds
     */
    void setCreationTime(long time);

    /**
     * Sets the access time of this {@link Evictable} in milliseconds.
     *
     * @param time the latest access time of this {@link Evictable} in milliseconds
     */
    void setAccessTime(long time);

    /**
     * Sets the access hit count of this {@link Evictable}.
     *
     * @param hit the access hit count for this {@link Evictable}
     */
    void setAccessHit(int hit);

    /**
     * Increases the access hit count of this {@link Evictable} by {@code 1}.
     */
    void incrementAccessHit();

    /**
     * Resets the access hit count of this {@link Evictable} to {@code 0}.
     */
    void resetAccessHit();

    /**
     * Checks whether the maximum idle time is passed with respect to the provided time
     * without any access during this time period as {@code maxIdleSeconds}.
     *
     * @param maxIdleMilliSeconds maximum idle time in milliseconds
     * @param now                 current time in milliseconds
     * @return {@code true} if exceeds max idle seconds, otherwise {@code false}
     */
    boolean isIdleAt(long maxIdleMilliSeconds, long now);
}
