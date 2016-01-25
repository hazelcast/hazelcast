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

package com.hazelcast.cache.merge;

import com.hazelcast.cache.CacheEntryView;
import com.hazelcast.cache.StorageTypeAwareCacheMergePolicy;

/**
 * `PassThroughCacheMergePolicy` policy merges cache entry from source to destination directly.
 */
public class PassThroughCacheMergePolicy
        implements StorageTypeAwareCacheMergePolicy {

    public PassThroughCacheMergePolicy() {

    }

    @Override
    public Object merge(String cacheName, CacheEntryView mergingEntry, CacheEntryView existingEntry) {
        return mergingEntry != null ? mergingEntry.getValue() : existingEntry.getValue();
    }

}
