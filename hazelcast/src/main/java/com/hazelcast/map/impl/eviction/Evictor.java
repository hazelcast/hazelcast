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

package com.hazelcast.map.impl.eviction;

import com.hazelcast.map.impl.recordstore.RecordStore;

/**
 * Evicts a {@link RecordStore}.
 *
 * When the {@link RecordStore} needs to be evicted according to {@link Evictor#checkEvictable},
 * {@link Evictor} removes records from {@link RecordStore}.
 */
public interface Evictor {

    /**
     * Evict supplied record-store.
     *
     * @param recordStore the recordStore
     */
    void evict(RecordStore recordStore);

    /**
     * Check whether the supplied record-store needs eviction.
     *
     * @param recordStore the recordStore
     * @return {@code true} if eviction is required, {@code false} otherwise.
     */
    boolean checkEvictable(RecordStore recordStore);
}
