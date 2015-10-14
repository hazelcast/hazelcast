/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.eviction;

import com.hazelcast.map.impl.recordstore.RecordStore;

/**
 * Evicts a {@link RecordStore}.
 *
 * When the {@link RecordStore} needs to be evicted according to {@link EvictionChecker#checkEvictionPossible},
 * {@link Evictor} removes records from {@link RecordStore}.
 */
public interface Evictor {

    /**
     * Find size to evict from a record-store.
     *
     * @param recordStore the recordStore
     * @return removal size.
     */
    int findRemovalSize(RecordStore recordStore);

    /**
     * Remove supplied number of elements from record-store.
     *
     * @param removalSize supplied size to remove.
     * @param recordStore the recordStore
     */
    void removeSize(int removalSize, RecordStore recordStore);

    /**
     * Get eviction checker for this {@link Evictor}
     *
     * @return the {@link EvictionChecker}
     */
    EvictionChecker getEvictionChecker();
}
