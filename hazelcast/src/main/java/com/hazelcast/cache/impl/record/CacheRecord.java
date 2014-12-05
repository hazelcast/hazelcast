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

import com.hazelcast.cache.impl.eviction.Evictable;

/**
 * <p>
 * An expirable and evictable data object which represents a cache entry.
 * </p>
 * Record of {@link com.hazelcast.cache.impl.ICacheRecordStore}.
 *
 * @param <V> the type of the value stored by this {@link CacheRecord}
 */
public interface CacheRecord<V> extends Expirable, Evictable {

    /**
     * Gets the value of this {@link CacheRecord}.
     *
     * @return the value of this {@link CacheRecord}
     */
    V getValue();

    /**
     * Sets the value of this {@link CacheRecord}.
     *
     * @param value the value for this {@link CacheRecord}
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
     * Increases the access hit count of this {@link Evictable} as <code>1</code>.
     */
    void incrementAccessHit();

    /**
     * Resets the access hit count of this {@link Evictable} to <code>0</code>.
     */
    void resetAccessHit();

}
