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

package com.hazelcast.internal.nearcache.impl.invalidation;

import com.hazelcast.internal.nearcache.NearCacheRecord;

import java.util.UUID;

/**
 * Used to detect staleness of near-cached data. There are two cases that staleness can be seen:
 * <ul>
 * <li>
 * One is during near-cache-put. If an invalidation comes for an entry during start and end of put,
 * write staleness can be seen. As a result, latest write should be cancelled by removing the entry.
 * </li>
 * <li>
 * Other one can happen during near-cache-get, if one or more invalidations are lost for a key,
 * this will make near-cached-data stale and next near-cache-get should return null to force fresh data
 * fetching from underlying imap/icache.
 * </li>
 * </ul>
 *
 * @see com.hazelcast.internal.nearcache.impl.store.AbstractNearCacheRecordStore#get
 * @see com.hazelcast.internal.nearcache.impl.store.AbstractNearCacheRecordStore#putIdentified
 */
public interface StaleReadWriteDetector {

    /**
     * This instance will be used when near-cache invalidations are disabled.
     * It behaves as if there is no staleness and data always fresh.
     */
    StaleReadWriteDetector ALWAYS_FRESH = new StaleReadWriteDetector() {
        @Override
        public long takeSequence(Object key) {
            return 0;
        }

        @Override
        public UUID takeUuid(Object key) {
            return null;
        }

        @Override
        public boolean isStaleRead(Object key, NearCacheRecord nearCacheRecord) {
            return false;
        }

        @Override
        public boolean isStaleWrite(Object key, NearCacheRecord nearCacheRecord) {
            return false;
        }
    };

    /**
     * Returns associated sequence for the supplied key.
     *
     * @param key the key
     * @return sequence for the key
     */
    long takeSequence(Object key);

    /**
     * Returns associated uuid for the supplied key.
     *
     * @param key the key
     * @return uuid for the key
     */
    UUID takeUuid(Object key);

    /**
     * Returns {@code true} if reading with the supplied uuid and the sequence is stale,
     * otherwise returns {@code false}
     *
     * @param key    the key
     * @param record the near-cache record
     * @return {@code true} if reading with the supplied uuid and the sequence is stale,
     * otherwise returns {@code false}
     */
    boolean isStaleRead(Object key, NearCacheRecord record);

    /**
     * Returns {@code true} if writing with the supplied uuid and the sequence is stale,
     * otherwise returns {@code false}
     *
     * @param key    the key
     * @param record the near-cache record
     * @return {@code true} if writing with the supplied uuid and the sequence is stale,
     * otherwise returns {@code false}
     */
    boolean isStaleWrite(Object key, NearCacheRecord record);
}
