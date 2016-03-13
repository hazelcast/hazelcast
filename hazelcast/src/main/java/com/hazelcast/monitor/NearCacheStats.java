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

package com.hazelcast.monitor;

public interface NearCacheStats extends LocalInstanceStats {

    /**
     * Returns the creation time of this NearCache on this member
     *
     * @return creation time of this NearCache on this member
     */
    long getCreationTime();

    /**
     * Returns the number of entries owned by this member.
     *
     * @return number of entries owned by this member.
     */
    long getOwnedEntryCount();

    /**
     * Returns memory cost (number of bytes) of entries in this cache.
     *
     * @return memory cost (number of bytes) of entries in this cache.
     */
    long getOwnedEntryMemoryCost();

    /**
     * Returns the number of hits (reads) of the locally owned entries.
     *
     * @return number of hits (reads) of the locally owned entries.
     */
    long getHits();

    /**
     * Returns the number of misses of the locally owned entries.
     *
     * @return number of misses of the locally owned entries.
     */
    long getMisses();

    /**
     * Returns the hit/miss ratio of the locally owned entries.
     *
     * @return hit/miss ratio of the locally owned entries.
     */
    double getRatio();
}
