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
import static com.hazelcast.map.impl.record.RecordReaderWriter.SIMPLE_DATA_RECORD_WITH_LFU_EVICTION_READER_WRITER;

/**
 * Used when {@link MapConfig#isPerEntryStatsEnabled()} is {@code false}
 * and eviction is configured.
 *
 * @see SimpleRecordWithLRUEviction
 */
class SimpleRecordWithLFUEviction<V> extends SimpleRecord<V> {
    private volatile int hits;

    SimpleRecordWithLFUEviction() {
    }

    SimpleRecordWithLFUEviction(V value) {
        super(value);
    }

    @Override
    public int getHits() {
        return hits;
    }

    @Override
    public void setHits(int hits) {
        this.hits = hits;
    }

    @Override
    public RecordReaderWriter getMatchingRecordReaderWriter() {
        return SIMPLE_DATA_RECORD_WITH_LFU_EVICTION_READER_WRITER;
    }

    @Override
    public long getCost() {
        if (value instanceof Data) {
            return super.getCost() + INT_SIZE_IN_BYTES;
        } else {
            return 0;
        }
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

        SimpleRecordWithLFUEviction that = (SimpleRecordWithLFUEviction) o;
        return hits == that.hits;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + hits;
        return result;
    }

    @Override
    public String toString() {
        return "SimpleRecordWithLFUEviction{"
                + "hits=" + hits
                + "} " + super.toString();
    }
}
