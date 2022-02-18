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

package com.hazelcast.map.impl.querycache.subscriber.record;

import com.hazelcast.internal.eviction.Evictable;

/**
 * Represents a {@link com.hazelcast.map.QueryCache QueryCache} record.
 *
 * @param <V> the type of the value of this record.
 */
public interface QueryCacheRecord<V> extends Evictable {

    @Override
    V getValue();

    /**
     * @return stored value without any conversion
     */
    Object getRawValue();

    /**
     * Sets the access time of this {@link Evictable} in milliseconds.
     *
     * @param time the latest access time of this {@link Evictable} in milliseconds
     */
    void setAccessTime(long time);

    /**
     * Increases the access hit count of this {@link Evictable} as <code>1</code>.
     */
    void incrementAccessHit();
}
