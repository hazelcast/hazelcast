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
import com.hazelcast.internal.nio.Bits;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.Objects;

/**
 * Includes common functionality for {@link
 * ValueDataChunk} and {@link KeyValueDataChunk}
 *
 * @see ValueDataChunk
 * @see KeyValueDataChunk
 */
public abstract class AbstractDataChunk implements IdentifiedDataSerializable {

    private String name;
    private boolean destroyed;
    private long chunkSizeInBytes;

    protected AbstractDataChunk() {
    }

    protected AbstractDataChunk(String name, boolean destroyed) {
        this.name = Objects.requireNonNull(name, "Name cannot be null");
        this.destroyed = destroyed;
        this.chunkSizeInBytes = calculateSizeOfChunkMetadata(name);
    }

    private static long calculateSizeOfChunkMetadata(String name) {
        // size of field `name`
        return name.length()
                // size of field `destroyed`
                + Bits.BOOLEAN_SIZE_IN_BYTES
                // size of field `chunkSizeInBytes`
                + Bits.LONG_SIZE_IN_BYTES;
    }

    protected void addToChunkSize(long size) {
        chunkSizeInBytes += size;
    }

    /**
     * Returns the name of the data structure.
     *
     * @return the name of the data structure.
     */
    public final String getName() {
        return name;
    }

    public final long getChunkSizeInBytes() {
        return chunkSizeInBytes;
    }

    /**
     * Checks whether the data structure is destroyed.
     *
     * @return {@code true} if the data structure is destroyed, {@code false} otherwise.
     */
    public final boolean isDestroyed() {
        return destroyed;
    }

    @Override
    public int getFactoryId() {
        return RaftDataSerializerConstants.F_ID;
    }

    @Override
    public final void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(name);
        out.writeBoolean(destroyed);
        out.writeLong(chunkSizeInBytes);

        writeDataInternal(out);
    }

    abstract void writeDataInternal(ObjectDataOutput out) throws IOException;

    @Override
    public final void readData(ObjectDataInput in) throws IOException {
        name = in.readString();
        destroyed = in.readBoolean();
        chunkSizeInBytes = in.readLong();

        readDataInternal(in);
    }

    abstract void readDataInternal(ObjectDataInput in) throws IOException;

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AbstractDataChunk that = (AbstractDataChunk) o;
        return destroyed == that.destroyed
                && chunkSizeInBytes == that.chunkSizeInBytes
                && Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        int result = Objects.hashCode(name);
        result = 31 * result + Boolean.hashCode(destroyed);
        result = 31 * result + Long.hashCode(chunkSizeInBytes);
        return result;
    }

    @Override
    public String toString() {
        return "name='" + name + '\''
                + ", destroyed=" + destroyed
                + ", chunkSizeInBytes=" + chunkSizeInBytes;
    }
}
