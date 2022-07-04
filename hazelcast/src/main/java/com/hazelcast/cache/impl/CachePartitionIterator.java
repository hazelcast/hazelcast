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

/**
 * Iterator for iterating cache entries in a single partition.
 * The values are fetched in batches.
 * <b>NOTE</b>
 * The iteration may be done when the map is being mutated or when there are
 * membership changes. The iterator does not reflect the state when it has
 * been constructed - it may return some entries that were added after the
 * iteration has started and may not return some entries that were removed
 * after iteration has started.
 * The iterator will not, however, skip an entry if it has not been changed
 * and will not return an entry twice.
 */
public class CachePartitionIterator<K, V> extends CacheIterator<K, V> {

    public CachePartitionIterator(CacheProxy<K, V> cache, int fetchSize, int partitionId, boolean prefetchValues) {
        super(cache, fetchSize, partitionId, prefetchValues);
    }

    @Override
    protected boolean advance() {
        if (pointers[pointers.length - 1].getIndex() < 0) {
            resetPointers();
            return false;
        }
        result = fetch();
        if (result != null && result.size() > 0) {
            index = 0;
            return true;
        }
        return false;
    }
}
