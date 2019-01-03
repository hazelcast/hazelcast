/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache;

import com.hazelcast.nio.serialization.BinaryInterface;

import java.io.Serializable;

/**
 * Policy for merging cache entries after a split-brain has been healed.
 * <p>
 * Passed {@link CacheEntryView} instances wrap the key and value as their original types
 * with conversion to object from their storage types. If you don't need the original types
 * of key and value, you should use {@link StorageTypeAwareCacheMergePolicy} which is
 * a sub-type of this interface.
 *
 * @see com.hazelcast.cache.merge.HigherHitsCacheMergePolicy
 * @see com.hazelcast.cache.merge.LatestAccessCacheMergePolicy
 * @see com.hazelcast.cache.merge.PassThroughCacheMergePolicy
 * @see com.hazelcast.cache.merge.PutIfAbsentCacheMergePolicy
 */
@BinaryInterface
public interface CacheMergePolicy extends Serializable {

    /**
     * Selects one of the merging and existing cache entries to be merged.
     * <p>
     * Note that the {@code existingEntry} may be {@code null} if there
     * is no entry with the same key in the destination cache.
     * This happens, when the entry for that key was
     * <ul>
     * <li>only created in the smaller sub-cluster during the split-brain</li>
     * <li>removed in the larger sub-cluster during the split-brain</li>
     * </ul>
     *
     * @param cacheName     name of the cache
     * @param mergingEntry  {@link CacheEntryView} instance that has the cache entry to be merged
     * @param existingEntry {@link CacheEntryView} instance that has the existing cache entry
     *                      or {@code null} if there is no existing cache entry
     * @return the selected value for merging or {@code null} if the entry should be removed
     */
    Object merge(String cacheName, CacheEntryView mergingEntry, CacheEntryView existingEntry);
}
