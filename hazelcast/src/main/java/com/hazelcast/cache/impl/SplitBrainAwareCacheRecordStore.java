/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cache.CacheEntryView;
import com.hazelcast.cache.CacheMergePolicy;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.nio.serialization.Data;

/**
 * Contract point for {@link com.hazelcast.cache.impl.ICacheRecordStore} implementations
 * which are handled in split-brain.
 *
 * @see CacheRecordStore
 * @see CacheEntryView
 * @see CacheMergePolicy
 */
public interface SplitBrainAwareCacheRecordStore
        extends ICacheRecordStore {

    /**
     * Merges given record (inside given {@link CacheEntryView}) with the existing record as given {@link CacheMergePolicy}.
     *
     * @param cacheEntryView    the {@link CacheEntryView} instance that wraps key/value for merging and existing entry
     * @param mergePolicy       the {@link CacheMergePolicy} instance for handling merge policy
     * @return the used {@link CacheRecord} if merge is applied, otherwise <code>null</code>
     */
    CacheRecord merge(CacheEntryView<Data, Data> cacheEntryView, CacheMergePolicy mergePolicy);

}
