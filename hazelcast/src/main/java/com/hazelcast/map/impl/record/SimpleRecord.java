/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import java.util.Objects;

import static com.hazelcast.internal.util.JVMUtil.REFERENCE_COST_IN_BYTES;
import static com.hazelcast.map.impl.record.RecordReaderWriter.SIMPLE_DATA_RECORD_READER_WRITER;

/**
 * Used when {@link MapConfig#isStatisticsEnabled()} is {@code false}
 */
@SuppressWarnings({"checkstyle:methodcount", "VolatileLongOrDoubleField"})
class SimpleRecord<V> implements Record<V> {
    protected volatile V value;

    SimpleRecord() {
    }

    SimpleRecord(V value) {
        this.value = value;
    }

    @Override
    public final long getVersion() {
        return 0;
    }

    @Override
    public final void setVersion(long version) {
    }

    @Override
    public long getLastAccessTime() {
        return 0;
    }

    @Override
    public void setLastAccessTime(long lastAccessTime) {
    }

    @Override
    public long getLastUpdateTime() {
        return 0;
    }

    @Override
    public void setLastUpdateTime(long lastUpdateTime) {
    }

    @Override
    public long getCreationTime() {
        return 0;
    }

    @Override
    public void setCreationTime(long creationTime) {
    }

    @Override
    public int getHits() {
        return 0;
    }

    @Override
    public void setHits(int hits) {
    }

    @Override
    public V getValue() {
        return value;
    }

    @Override
    public void setValue(V value) {
        this.value = value;
    }

    @Override
    public Object getCachedValueUnsafe() {
        return Record.NOT_CACHED;
    }

    @Override
    public boolean casCachedValue(Object expectedValue, Object newValue) {
        return true;
    }

    @Override
    public final long getSequence() {
        return UNSET;
    }

    @Override
    public final void setSequence(long sequence) {
    }

    @Override
    public long getLastStoredTime() {
        return UNSET;
    }

    @Override
    public void setLastStoredTime(long lastStoredTime) {
    }

    @Override
    public long getCost() {
        if (value instanceof Data) {
            return REFERENCE_COST_IN_BYTES + ((Data) value).getHeapCost();
        } else {
            return 0;
        }
    }

    @Override
    public int getRawCreationTime() {
        return 0;
    }

    @Override
    public int getRawLastAccessTime() {
        return 0;
    }

    @Override
    public int getRawLastUpdateTime() {
        return 0;
    }

    @Override
    public void setRawCreationTime(int creationTime) {
    }

    @Override
    public void setRawLastAccessTime(int lastAccessTime) {
    }

    @Override
    public void setRawLastUpdateTime(int lastUpdateTime) {
    }

    @Override
    public int getRawLastStoredTime() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setRawLastStoredTime(int time) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RecordReaderWriter getMatchingRecordReaderWriter() {
        return SIMPLE_DATA_RECORD_READER_WRITER;
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

        SimpleRecord that = (SimpleRecord) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + value.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "SimpleRecord{"
                + "value=" + value
                + ", " + super.toString()
                + "} ";
    }
}
