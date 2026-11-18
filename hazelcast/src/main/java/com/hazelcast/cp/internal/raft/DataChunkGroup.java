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

package com.hazelcast.cp.internal.raft;

import com.hazelcast.cp.internal.raft.impl.RaftDataSerializerConstants;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A container designed to group multiple data chunks from a service.
 *
 * @param <T> the type of data chunks stored in this group
 */
public class DataChunkGroup<T> implements IdentifiedDataSerializable {

    private List<T> data;

    public DataChunkGroup() {
        this.data = new ArrayList<>();
    }

    /**
     * Adds an item to the chunk.
     *
     * @param item The item to add to the chunk.
     * @return This chunk instance for method chaining.
     * @throws NullPointerException If the provided item is null.
     */
    public DataChunkGroup<T> add(T item) {
        Objects.requireNonNull(item, "Item cannot be null");
        data.add(item);
        return this;
    }

    /**
     * @return the services data in this chunk.
     */
    public List<T> getServiceData() {
        return data;
    }

    /**
     * @return {@code true} if this chunk is empty, {@code false} otherwise.
     */
    public boolean isEmpty() {
        return data.isEmpty();
    }

    @Override
    public int getFactoryId() {
        return RaftDataSerializerConstants.F_ID;
    }

    @Override
    public int getClassId() {
        return RaftDataSerializerConstants.DATA_CHUNK_GROUP;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(data.size());
        for (T item : data) {
            out.writeObject(item);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        List<T> newData = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            T item = in.readObject();
            newData.add(item);
        }
        this.data = newData;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DataChunkGroup<?> that = (DataChunkGroup<?>) o;
        return Objects.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(data);
    }

    @Override
    public String toString() {
        return "Chunk{data=" + data + "}";
    }
}
