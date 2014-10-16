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

package com.hazelcast.cache.impl;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

/**
 * Internal event data wrapper used during publishing and dispatching events.
 *
 * An event data is represented by
 * <ul>
 * <li>name</li>
 * <li>Event type</li>
 * <li>key</li>
 * <li>value</li>
 * <li>old value if available</li>
 * <li>availability of old data</li>
 * </ul>
 * All key value and oldValue are represented in serialized {@link Data} form.
 *
 * @see com.hazelcast.cache.impl.CacheService#publishEvent(String, CacheEventSet, int)
 * @see com.hazelcast.cache.impl.CacheService#dispatchEvent(Object, CacheEventListener)
 */
public interface CacheEventData
        extends IdentifiedDataSerializable {

    /**
     * Cache Event Type of this event data
     * @return Cache Event Type
     * @see com.hazelcast.cache.impl.CacheEventType
     */
    CacheEventType getCacheEventType();

    /**
     * get the name of the cache
     * @return the name of the cache
     */
    String getName();

    /**
     * Get Cache entry key as {@link Data}
     * @return key as {@link Data}
     */
    Data getDataKey();

    /**
     * Get Cache entry value as {@link Data}
     * @return value as {@link Data}
     * */
    Data getDataValue();

    /**
     * Get the old value of entry as {@link Data} if available
     * @return if available old value of entry as {@link Data} else null
     */
    Data getDataOldValue();

    /**
     * is old value available
     * @return is old value available
     */
    boolean isOldValueAvailable();
}
