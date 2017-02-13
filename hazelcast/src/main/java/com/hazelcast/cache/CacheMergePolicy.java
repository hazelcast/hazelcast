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

package com.hazelcast.cache;

import com.hazelcast.nio.serialization.BinaryInterface;

import java.io.Serializable;

/**
 * <p>
 * Policy for merging cache entries.
 * </p>
 *
 * <p>
 * Passed {@link CacheEntryView} instances wraps the key and value as their original types
 * with convertion to object from their storage types. If user doesn't need to original types of key and value,
 * (s)he should use {@link StorageTypeAwareCacheMergePolicy} which is sub-type of this interface.
 * </p>
 */
@BinaryInterface
public interface CacheMergePolicy extends Serializable {

    /**
     * <p>
     * Selects one of the merging and existing cache entries to be merged.
     * </p>
     *
     * <p>
     * Note that as mentioned also in arguments, the {@link CacheEntryView} instance that represents existing cache entry
     * may be null if there is no existing entry for the specified key in the the {@link CacheEntryView} instance
     * that represents merging cache entry.
     * </p>
     *
     * @param cacheName     name of the cache
     * @param mergingEntry  {@link CacheEntryView} instance that has cache entry to be merged
     * @param existingEntry {@link CacheEntryView} instance that has existing cache entry.
     *                      This entry may be <code>null</code> if there is no existing cache entry.
     * @return the selected value for merging
     */
    Object merge(String cacheName, CacheEntryView mergingEntry, CacheEntryView existingEntry);

}
