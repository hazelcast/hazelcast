/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.monitor;

/**
 * Local record store statistics to be used by {@link MemberState}
 * implementations.
 */
public interface LocalRecordStoreStats {

    /**
     * Returns the number of hits (reads) of the locally owned entries of this partition.
     *
     * @return number of hits (reads) of the locally owned entries of this partition.
     */
    long getHits();

    /**
     * Returns the last access (read) time of the locally owned entries of this partition.
     *
     * @return last access (read) time of the locally owned entries of this partition.
     */
    long getLastAccessTime();

    /**
     * Returns the last update time of the locally owned entries of this partition.
     *
     * @return last update time of the locally owned entries of this partition.
     */
    long getLastUpdateTime();

    long getEvictionCount();

    long getExpirationCount();

    void increaseEvictions();

    void increaseExpirations();

    /**
     * Increases the number of hits of the locally owned entries of this partition.
     */
    void increaseHits();

    /**
     * Sets the last access (read) time of the locally owned entries of this partition.
     */
    void setLastAccessTime(long time);

    /**
     * Sets the last update time of the locally owned entries of this partition.
     */
    void setLastUpdateTime(long time);

    /**
     * Returns the number of times an entry larger than the oversized entry
     * threshold was written to this partition.
     */
    long getOversizedEntriesSeen();

    /**
     * Returns the serialized size, in bytes, of the last oversized entry written to this partition.
     */
    long getLastOversizedEntrySize();

    /**
     * Returns the time the last oversized entry was written to this partition.
     */
    long getLastOversizedEntryTime();

    /**
     * Records an oversized entry written to this partition.
     *
     * @param entrySize the serialized entry size in bytes
     * @param time      the time the entry was written
     */
    void recordOversizedEntry(long entrySize, long time);
}
