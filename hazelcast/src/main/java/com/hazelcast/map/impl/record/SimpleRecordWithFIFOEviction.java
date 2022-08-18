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
import static com.hazelcast.map.impl.record.RecordReaderWriter.SIMPLE_DATA_RECORD_WITH_FIFO_EVICTION_READER_WRITER;
import static com.hazelcast.internal.util.TimeStripUtil.stripBaseTime;

/**
 * Used when {@link MapConfig#isPerEntryStatsEnabled()} is {@code false}
 * and eviction is configured.
 *
 * @see SimpleRecordWithFIFOEviction
 */
class SimpleRecordWithFIFOEviction<V> extends SimpleRecord<V> {
    private volatile int creationTime;

    SimpleRecordWithFIFOEviction() {
    }

    SimpleRecordWithFIFOEviction(V value) {
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
    public RecordReaderWriter getMatchingRecordReaderWriter() {
        return SIMPLE_DATA_RECORD_WITH_FIFO_EVICTION_READER_WRITER;
    }

    @Override
    public long getCost() {
        return value instanceof Data
                ? super.getCost() + INT_SIZE_IN_BYTES : 0;
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

        SimpleRecordWithFIFOEviction that = (SimpleRecordWithFIFOEviction) o;
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
        return "SimpleRecordWithFIFOEviction{"
                + "creationTime=" + creationTime
                + "} " + super.toString();
    }
}
