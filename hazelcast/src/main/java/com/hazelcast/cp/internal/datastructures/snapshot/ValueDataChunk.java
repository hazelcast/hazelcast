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

package com.hazelcast.cp.internal.datastructures.snapshot;

import com.hazelcast.cp.internal.raft.impl.RaftDataSerializerConstants;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.Objects;

/**
 * Represents the state of a single value data structure during
 * snapshotting. This class allows a data structure to be
 * divided into one or more {@link ValueDataChunk} instances.
 * <p>
 * It supports both active and destroyed states of the data structure.
 */
public class ValueDataChunk extends AbstractDataChunk {

    private Data value;

    public ValueDataChunk() {
    }

    /**
     * @see #ValueDataChunk(String, Data, boolean)
     */
    public ValueDataChunk(String name, Data value) {
        this(name, value, false);
    }

    /**
     * @see #ValueDataChunk(String, Data, boolean)
     */
    public ValueDataChunk(String name, boolean destroyed) {
        this(name, null, destroyed);
    }

    /**
     * Constructs a new {@link ValueDataChunk} instance.
     *
     * @param name      The name of the data structure. Must not be {@code null}.
     * @param value     The data value, or {@code null} if the data structure is destroyed
     *                  or if a {@code null} value is provided. Note that {@code null} can be
     *                  a valid value even when {@code destroyed} is {@code false}.
     * @param destroyed Indicates whether the data structure is destroyed.
     * @throws NullPointerException if {@code name} is {@code null}.
     */
    private ValueDataChunk(String name, Data value, boolean destroyed) {
        super(name, destroyed);
        this.value = destroyed ? null : value;

        addToChunkSize(value == null ? 0L : value.dataSize());
    }

    /**
     * @return the snapshot value for this data structure
     */
    public Data getValue() {
        return value;
    }

    @Override
    public int getClassId() {
        return RaftDataSerializerConstants.VALUE_DATA_CHUNK;
    }

    @Override
    void writeDataInternal(ObjectDataOutput out) throws IOException {
        IOUtil.writeData(out, value);
    }

    @Override
    void readDataInternal(ObjectDataInput in) throws IOException {
        value = IOUtil.readData(in);
    }


    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        if (!super.equals(o)) {
            return false;
        }

        ValueDataChunk that = (ValueDataChunk) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + Objects.hashCode(value);
        return result;
    }

    @Override
    public String toString() {
        return "ValueDataChunk{"
                + super.toString()
                + ", value=" + value
                + "} ";
    }
}

