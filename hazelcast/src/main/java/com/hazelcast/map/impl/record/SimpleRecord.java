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

import static com.hazelcast.internal.util.JVMUtil.OBJECT_HEADER_SIZE;
import static com.hazelcast.internal.util.JVMUtil.REFERENCE_COST_IN_BYTES;
import static com.hazelcast.map.impl.record.RecordReaderWriter.SIMPLE_DATA_RECORD_READER_WRITER;

/**
 * Used when {@link MapConfig#isStatisticsEnabled()} is {@code false}
 */
@SuppressWarnings({"checkstyle:methodcount", "VolatileLongOrDoubleField"})
class SimpleRecord<V> implements Record<V> {
    protected volatile V value;
    private int version;

    SimpleRecord() {
    }

    SimpleRecord(V value) {
        setValue(value);
    }

    @Override
    public final int getVersion() {
        return version;
    }

    @Override
    public final void setVersion(int version) {
        this.version = version;
    }

    @Override
    public long getLastAccessTime() {
        return UNSET;
    }

    @Override
    public void setLastAccessTime(long lastAccessTime) {
    }

    @Override
    public long getLastUpdateTime() {
        return UNSET;
    }

    @Override
    public void setLastUpdateTime(long lastUpdateTime) {
    }

    @Override
    public long getCreationTime() {
        return UNSET;
    }

    @Override
    public void setCreationTime(long creationTime) {
    }

    @Override
    public int getHits() {
        return UNSET;
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
            return OBJECT_HEADER_SIZE
                    + REFERENCE_COST_IN_BYTES + ((Data) value).getHeapCost();
        } else {
            return 0;
        }
    }

    @Override
    public void onAccess(long now) {
        // NOP
    }

    @Override
    public void onAccessSafe(long now) {
        // NOP
    }

    @Override
    public void onStore() {
        // NOP
    }

    @Override
    public int getRawCreationTime() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setRawCreationTime(int creationTime) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getRawLastAccessTime() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setRawLastAccessTime(int lastAccessTime) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getRawLastUpdateTime() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setRawLastUpdateTime(int lastUpdateTime) {
        throw new UnsupportedOperationException();
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

        SimpleRecord<?> that = (SimpleRecord<?>) o;

        if (version != that.version) {
            return false;
        }
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        int result = value != null ? value.hashCode() : 0;
        result = 31 * result + version;
        return result;
    }

    @Override
    public String toString() {
        return "SimpleRecord{"
                + "value=" + value
                + ", version=" + version
                + '}';
    }
}
