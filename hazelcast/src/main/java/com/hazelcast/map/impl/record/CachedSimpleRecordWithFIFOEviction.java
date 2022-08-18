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

import com.hazelcast.config.MapConfig;
import com.hazelcast.internal.serialization.Data;

import static com.hazelcast.internal.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.internal.util.TimeStripUtil.stripBaseTime;
import static com.hazelcast.map.impl.record.RecordReaderWriter.SIMPLE_DATA_RECORD_WITH_FIFO_EVICTION_READER_WRITER;

/**
 * Used when {@link MapConfig#isPerEntryStatsEnabled()} is {@code
 * false}, {@link MapConfig#getCacheDeserializedValues()} is not
 * {@link com.hazelcast.config.CacheDeserializedValues#NEVER}
 * and eviction is configured.
 *
 * @see CachedSimpleRecordWithFIFOEviction
 */
class CachedSimpleRecordWithFIFOEviction extends CachedSimpleRecord {
    private volatile int creationTime;

    CachedSimpleRecordWithFIFOEviction() {
    }

    CachedSimpleRecordWithFIFOEviction(Data value) {
        super(value);
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    @Override
    public void setCreationTime(long creationTime) {
        this.creationTime = stripBaseTime(creationTime);
    }

    @Override
    public int getRawCreationTime() {
        return creationTime;
    }

    @Override
    public void setRawCreationTime(int creationTime) {
        this.creationTime = creationTime;
    }

    @Override
    public long getCost() {
        return super.getCost() + INT_SIZE_IN_BYTES;
    }

    @Override
    public RecordReaderWriter getMatchingRecordReaderWriter() {
        return SIMPLE_DATA_RECORD_WITH_FIFO_EVICTION_READER_WRITER;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        CachedSimpleRecordWithFIFOEviction that = (CachedSimpleRecordWithFIFOEviction) o;
        return creationTime == that.creationTime;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + creationTime;
        return result;
    }

    @Override
    public String toString() {
        return "CachedSimpleRecordWithFIFOEviction{"
                + "creationTime=" + creationTime
                + "} " + super.toString();
    }
}
