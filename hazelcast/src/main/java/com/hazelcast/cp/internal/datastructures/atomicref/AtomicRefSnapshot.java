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

package com.hazelcast.cp.internal.datastructures.atomicref;

import com.hazelcast.cp.internal.datastructures.snapshot.ValueDataChunk;
import com.hazelcast.cp.internal.datastructures.spi.atomic.RaftAtomicValueSnapshot;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Snapshot of a {@link AtomicRefService} state for a Raft group
 */
public class AtomicRefSnapshot extends RaftAtomicValueSnapshot<Data> implements IdentifiedDataSerializable {

    public AtomicRefSnapshot() {
    }

    public AtomicRefSnapshot(Map<String, Data> refs, Set<String> destroyed) {
        super(refs, destroyed);
    }

    /**
     * Converts the stored values into a list of data chunks.
     *
     * @return a list of ValueDataChunk objects containing the values and destroyed entries
     */
    @Nonnull
    public List<ValueDataChunk> toChunks() {
        List<ValueDataChunk> valueDataChunks = new ArrayList<>();

        for (Map.Entry<String, Data> entry : getValues()) {
            String name = entry.getKey();
            Data value = entry.getValue();

            valueDataChunks.add(new ValueDataChunk(name, value));
        }

        for (String destroyedName : destroyed) {
            valueDataChunks.add(new ValueDataChunk(destroyedName, true));
        }

        // Sort valueDataChunks to create a stable order
        valueDataChunks.sort(Comparator.comparing(ValueDataChunk::isDestroyed)
                .thenComparing(ValueDataChunk::getName));

        return valueDataChunks;
    }

    public void fromChunks(List<ValueDataChunk> chunks) {
        for (ValueDataChunk chunk : chunks) {
            String mapName = chunk.getName();

            if (chunk.isDestroyed()) {
                destroyed.add(mapName);
                continue;
            }

            String name = chunk.getName();
            Data value = chunk.getValue();

            values.put(name, value);
        }
    }

    @Override
    public int getFactoryId() {
        return AtomicRefDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return AtomicRefDataSerializerHook.SNAPSHOT;
    }

    @Override
    protected void writeValue(ObjectDataOutput out, Data value) throws IOException {
        IOUtil.writeData(out, value);
    }

    @Override
    protected Data readValue(ObjectDataInput in) throws IOException {
        return IOUtil.readData(in);
    }

    @Override
    public String toString() {
        return "AtomicRefSnapshot{" + "refs=" + values + ", destroyed=" + destroyed + '}';
    }
}
