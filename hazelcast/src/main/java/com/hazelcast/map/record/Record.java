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

package com.hazelcast.map.record;

import com.hazelcast.nio.serialization.Data;

/**
 * @param <V>
 */
public interface Record<V> {

    /**
     *  If not a {@link com.hazelcast.map.record.CachedDataRecord)}.
     */
    Object NOT_CACHED = new Object();

    Data getKey();

    V getValue();

    void setValue(V value);

    void invalidate();

    RecordStatistics getStatistics();

    void setStatistics(RecordStatistics stats);

    void onAccess();

    void onUpdate();

    void onStore();

    long getCost();

    long getVersion();

    void setVersion(long version);

    void setEvictionCriteriaNumber(long evictionCriteriaNumber);

    long getEvictionCriteriaNumber();

    Object getCachedValue();

    void setCachedValue(Object cachedValue);

    long getTtl();

    void setTtl(long ttl);

    long getLastAccessTime();

    void setLastAccessTime(long lastAccessTime);

    long getLastUpdateTime();

    void setLastUpdateTime(long lastUpdatedTime);

    long getCreationTime();

    void setCreationTime(long creationTime);

}
