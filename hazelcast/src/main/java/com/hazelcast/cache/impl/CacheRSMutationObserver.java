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

package com.hazelcast.cache.impl;

import com.hazelcast.internal.serialization.Data;

/**
 * Main contract to observe mutations on a {@link ICacheRecordStore}.
 * {@link CacheRSMutationObserver} is created per each partition
 * hence they are accessed by only one thread along its lifecycle.
 */
public interface CacheRSMutationObserver {

    /**
     * Called when a new entry is created.
     *
     * @param key   the key
     * @param value the value
     */
    void onCreate(Data key, Object value);

    /**
     * Called when an entry is removed.
     *
     * @param key   the key
     * @param value the old value
     */
    void onRemove(Data key, Object value);

    /**
     * Called when an entry is updated.
     *
     * @param key      the key
     * @param oldValue the old value
     * @param value    the new value
     */
    void onUpdate(Data key, Object oldValue, Object value);

    /**
     * Called when an entry is evicted.
     *
     * @param key   the key
     * @param value the old value
     */
    void onEvict(Data key, Object value);

    /**
     * Called when an entry is expired.
     *
     * @param key   the key
     * @param value the old value
     */
    void onExpire(Data key, Object value);

    /**
     * Called when the observed {@link ICacheRecordStore} is destroyed.
     */
    void onDestroy();
}
