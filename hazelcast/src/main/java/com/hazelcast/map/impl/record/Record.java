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

package com.hazelcast.map.impl.record;

import com.hazelcast.internal.util.Clock;

/**
 * @param <V> the type of value which is in the Record
 */
@SuppressWarnings("checkstyle:methodcount")
public interface Record<V> {

    /**
     * Represents an unset value. This is the default
     * value of ttl, max-idle or anything unavailable.
     */
    int UNSET = -1;

    /**
     * If not a {@link com.hazelcast.map.impl.record.CachedSimpleRecord}.
     */
    Object NOT_CACHED = new Object();

    V getValue();

    void setValue(V value);

    /**
     * Returns heap cost of this record in bytes.
     *
     * @return heap cost of this record in bytes.
     */
    long getCost();

    int getVersion();

    void setVersion(int version);

    /**
     * Get current cache value or null.
     * <p>
     * Warning: Do not use this method directly as it
     * might expose arbitrary objects acting as a lock.
     * Use {@link Records#getCachedValue(Record)} instead.
     *
     * @return current cached value or null or cached record mutex.
     */
    default Object getCachedValueUnsafe() {
        return Record.NOT_CACHED;
    }

    /**
     * Atomically sets the cached value to the given new value
     * if the current cached value {@code ==} the expected value.
     *
     * @param expectedValue the expected cached value
     * @param newValue      the new cached value
     * @return {@code true} if successful. False
     * return indicates that the actual cached value
     * was not equal to the expected cached value.
     */
    default boolean casCachedValue(Object expectedValue, Object newValue) {
        assert getCachedValueUnsafe() != Record.NOT_CACHED;
        return true;
    }

    default long getLastAccessTime() {
        return UNSET;
    }

    default void setLastAccessTime(long lastAccessTime) {
    }

    default long getLastUpdateTime() {
        return UNSET;
    }

    default void setLastUpdateTime(long lastUpdateTime) {
    }

    default long getCreationTime() {
        return UNSET;
    }

    default void setCreationTime(long creationTime) {
    }

    default int getHits() {
        return UNSET;
    }

    default void setHits(int hits) {
    }

    /**
     * Only used for Hot Restart, HDRecord
     *
     * @return current sequence number
     */
    default long getSequence() {
        return UNSET;
    }

    /**
     * Only used for Hot Restart, HDRecord
     */
    default void setSequence(long sequence) {
    }

    default long getLastStoredTime() {
        return UNSET;
    }

    default void setLastStoredTime(long lastStoredTime) {
    }

    /**
     * An implementation must be thread safe if the
     * record might be accessed from multiple threads.
     */
    default void onAccess(long now) {
        incrementHits();
        setLastAccessTime(now);
    }

    default void incrementHits() {
        int hits = getHits();
        if (hits < Integer.MAX_VALUE) {
            // protect against potential overflow
            setHits(hits + 1);
        }
    }

    default void onUpdate(long now) {
        // We allow version overflow, versions can also be negative value.
        setVersion(getVersion() + 1);
        setLastUpdateTime(now);
    }

    default void onStore() {
        setLastStoredTime(Clock.currentTimeMillis());
    }

    /**
     * @return record reader writer to be used when
     * serializing/de-serializing this record instance.
     */
    RecordReaderWriter getMatchingRecordReaderWriter();

    /* Below `raw` methods are used during serialization of a record. */
    default int getRawCreationTime() {
        return UNSET;
    }

    default void setRawCreationTime(int creationTime) {
    }

    default int getRawLastAccessTime() {
        return UNSET;
    }

    default void setRawLastAccessTime(int lastAccessTime) {
    }

    default int getRawLastUpdateTime() {
        return UNSET;
    }

    default void setRawLastUpdateTime(int lastUpdateTime) {
    }

    default int getRawLastStoredTime() {
        return UNSET;
    }

    default void setRawLastStoredTime(int lastStoredTime) {
    }
}
