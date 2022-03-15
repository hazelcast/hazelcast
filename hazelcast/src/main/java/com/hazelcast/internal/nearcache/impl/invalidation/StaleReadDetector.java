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

package com.hazelcast.internal.nearcache.impl.invalidation;

import com.hazelcast.internal.nearcache.NearCacheRecord;

/**
 * Used to detect staleness of Near Cache data.
 *
 * During {@link com.hazelcast.internal.nearcache.NearCache#get(Object)}, if one or more invalidations are lost for a key,
 * we will make near cached data unreachable with the help of {@link StaleReadDetector} and next {@code get()} from the
 * Near Cache will return {@code null} to force fresh data fetching from underlying IMap/ICache.
 *
 * @see com.hazelcast.internal.nearcache.impl.store.AbstractNearCacheRecordStore#get
 */
public interface StaleReadDetector {

    /**
     * This instance will be used when Near Cache invalidations are disabled.
     * It behaves as if there is no stale data and everything is fresh.
     */
    StaleReadDetector ALWAYS_FRESH = new StaleReadDetector() {
        @Override
        public boolean isStaleRead(Object key, NearCacheRecord record) {
            return false;
        }

        @Override
        public int getPartitionId(Object key) {
            return 0;
        }

        @Override
        public MetaDataContainer getMetaDataContainer(int partitionId) {
            return null;
        }

        @Override
        public String toString() {
            return "ALWAYS_FRESH";
        }
    };

    /**
     * @param key    the key
     * @param record the Near Cache record
     * @return {@code true} if reading with the supplied invalidation metadata is stale,
     * otherwise returns {@code false}
     */
    boolean isStaleRead(Object key, NearCacheRecord record);

    int getPartitionId(Object key);

    /**
     * @param partitionId supplied partition ID to get value
     * @return {@link MetaDataContainer} for this key
     */
    MetaDataContainer getMetaDataContainer(int partitionId);
}
