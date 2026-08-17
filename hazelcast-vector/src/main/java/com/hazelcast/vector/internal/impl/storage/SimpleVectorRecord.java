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

package com.hazelcast.vector.internal.impl.storage;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordReaderWriter;

import java.util.Objects;

import static com.hazelcast.internal.util.JVMUtil.OBJECT_HEADER_SIZE;
import static com.hazelcast.internal.util.JVMUtil.REFERENCE_COST_IN_BYTES;

/**
 * On-heap vector collection record.
 * Very similar to {@link com.hazelcast.map.impl.record.SimpleRecord} but has volatile version
 * to allow correct concurrent behavior.
 *
 * @see VectorCollectionStorage#logicalTime
 */
public class SimpleVectorRecord<V> implements Record<V> {
    // Both fields are volatile. Making only one of them volatile might cause some visibility problems (at least in theory).
    // Non-atomic updates to 2 fields are handled by increasing version twice and the rest of the protocol.
    // Note that full barriers (volatile) might not be necessary, using weaker barriers can be considered as an optimization.
    private volatile V value;
    private volatile int version;

    public SimpleVectorRecord(V value, int version) {
        this.value = value;
        this.version = version;
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
    public V getValue() {
        return value;
    }

    @Override
    public void setValue(V value) {
        this.value = value;
    }

    @Override
    public long getCost() {
        // OBJECT format is not supported yet
        return OBJECT_HEADER_SIZE
                + REFERENCE_COST_IN_BYTES + ((Data) value).getHeapCost()
                + Integer.BYTES;
    }

    @Override
    public RecordReaderWriter getMatchingRecordReaderWriter() {
        throw new UnsupportedOperationException("SimpleVectorRecord cannot be serialized");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SimpleVectorRecord<?> that = (SimpleVectorRecord<?>) o;

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
        return "SimpleVectorRecord{"
                + "value=" + value
                + ", version=" + version
                + '}';
    }
}
