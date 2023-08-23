/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nearcache;

import com.hazelcast.instance.LocalInstanceStats;
import com.hazelcast.internal.monitor.MemberState;

/**
 * Near Cache statistics to be used by {@link MemberState} implementations.
 */
public interface NearCacheStats extends LocalInstanceStats {

    /**
     * @return creation time of this Near Cache on this member
     */
    @Override
    long getCreationTime();

    /**
     * @return number of Near Cache entries owned by this member
     */
    long getOwnedEntryCount();

    /**
     * @return memory cost (number of bytes) of Near Cache entries owned by this member
     */
    long getOwnedEntryMemoryCost();

    /**
     * @return number of hits (reads) of Near Cache entries owned by this member
     */
    long getHits();

    /**
     * @return number of misses of Near Cache entries owned by this member
     */
    long getMisses();

    /**
     * @return hit/miss ratio of Near Cache entries owned by this member
     */
    double getRatio();

    /**
     * @return number of evictions of Near Cache entries owned by this member
     */
    long getEvictions();

    /**
     * @return number of TTL and max-idle expirations of Near Cache entries owned by this member
     */
    long getExpirations();

    /**
     * @return number of successful invalidations of Near Cache entries owned by this member
     */
    long getInvalidations();

    /**
     * @return number of requested invalidations of Near Cache entries owned by this member.
     * <p>
     * One request may cover multiple keys (e.g. {@code clear}), includes failed invalidations (e.g. where they key
     * isn't contained in the Near Cache).
     */
    long getInvalidationRequests();

    /**
     * @return the number of Near Cache key persistences (when the pre-load feature is enabled)
     */
    long getPersistenceCount();

    /**
     * @return the timestamp of the last Near Cache key persistence (when the pre-load feature is enabled)
     */
    long getLastPersistenceTime();

    /**
     * @return the duration in milliseconds of the last Near Cache key persistence (when the pre-load feature is enabled)
     */
    long getLastPersistenceDuration();

    /**
     * @return the written bytes of the last Near Cache key persistence (when the pre-load feature is enabled)
     */
    long getLastPersistenceWrittenBytes();

    /**
     * @return the number of persisted keys of the last Near Cache key persistence (when the pre-load feature is enabled)
     */
    long getLastPersistenceKeyCount();

    /**
     * @return the failure reason of the last Near Cache persistence (when the pre-load feature is enabled)
     */
    String getLastPersistenceFailure();
}
