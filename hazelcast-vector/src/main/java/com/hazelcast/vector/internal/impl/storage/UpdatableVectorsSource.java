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

import com.hazelcast.internal.memory.Measurable;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import org.jctools.maps.NonBlockingHashMapLong;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.internal.util.JVMUtil.OBJECT_HEADER_SIZE;
import static com.hazelcast.internal.util.JVMUtil.REFERENCE_COST_IN_BYTES;

/**
 * On-heap implementation of a vector source.
 * Expects ArrayVectorFloat as input.
 */

public class UpdatableVectorsSource implements RandomAccessVectorValues, Measurable {

    static final long FIXED_HEAP_BYTES_USED = (long) OBJECT_HEADER_SIZE + REFERENCE_COST_IN_BYTES + Integer.BYTES + Long.BYTES;

    private static final int INITIAL_CAPACITY = 1024;

    // heap bytes used by long key + float[] value
    final long heapBytesUsedPerEntry;

    // internal vector index int ID -> VectorFloat vector mapping
    private final NonBlockingHashMapLong<VectorFloat<?>> data;

    private final int dimension;

    public UpdatableVectorsSource(int dimension) {
        this.data = new NonBlockingHashMapLong<>(INITIAL_CAPACITY);
        this.dimension = dimension;

        // The costs of ArrayVectorFloat include the object header and the cost of the float[] field.
        // The float[] field cost includes the array header (object header + length as an int) and the cost of the float[].
        var vectorFloatHeapBytesUsed = OBJECT_HEADER_SIZE
                + REFERENCE_COST_IN_BYTES
                + OBJECT_HEADER_SIZE
                + Integer.BYTES
                + (long) dimension * Float.BYTES;

        this.heapBytesUsedPerEntry = vectorFloatHeapBytesUsed
                // add long key bytes used and cost of reference from data to the VectorFloat
                + Long.BYTES + REFERENCE_COST_IN_BYTES;
    }

    /**
     * @return {@code true} if previous value existed for the given id and was replaced,
     * {@code false} if no such id previously existed.
     */
    public boolean add(int id, VectorFloat<?> vector) {
        return data.put(id, vector) != null;
    }

    public boolean remove(int id) {
        return data.remove(id) != null;
    }

    @Override
    public int size() {
        return data.size();
    }

    @Override
    public int dimension() {
        return dimension;
    }

    @Override
    public VectorFloat<?> getVector(int i) {
        return data.get(i);
    }

    @Override
    public boolean isValueShared() {
        return false;
    }

    @Override
    public RandomAccessVectorValues copy() {
        return this;
    }

    @Override
    public long heapBytesUsed() {
        // Take into account only cost of this object + cost of long key & float[] mappings.
        // Internal fields of NonBlockingHashMapLong are not considered. For example,
        // NonBlockingHashMapLong has probably already allocated a larger long[] than the number
        // of elements used, but we don't account for that.
        return FIXED_HEAP_BYTES_USED + data.size() * heapBytesUsedPerEntry;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(data.size());
        for (var vectorEntry : data.entrySet()) {
            out.writeInt(vectorEntry.getKey().intValue());
            out.writeFloatArray((float[]) vectorEntry.getValue().get());
        }
    }

    public void readData(ObjectDataInput in) throws IOException {
        int vectorsCount = in.readInt();
        for (int k = 0; k < vectorsCount; k++) {
            int nodeId = in.readInt();
            var vectorArray = in.readFloatArray();
            add(nodeId, ArrayVectorProvider.getInstance().createFloatVector(vectorArray));
        }
    }

    public Set<Map.Entry<Long, VectorFloat<?>>> entries() {
        return Collections.unmodifiableSet(data.entrySet());
    }
}
