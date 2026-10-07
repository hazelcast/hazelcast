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
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.impl.Versioned;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;


/**
 * Represents the state of a map during snapshotting.
 * A map's contents can be split across multiple chunks.
 * <p>
 * Key-value pairs are stored in a flattened, strided list where
 * index [i] is the key and [i+1] is the value to avoid Map.Entry allocation.
 */
public final class KeyValueDataChunk extends AbstractDataChunk implements Versioned {

    public static final int KEY_VALUE_STRIDE = 2;

    private List<Data> keyValuePairs;
    private List<Long> entryTimestamps;

    /**
     * @deprecated This constructor is deprecated only
     * as a notice to developers. It is used only for
     * deserialization and is not intended for removal.
     */
    @Deprecated
    public KeyValueDataChunk() {
        // Only used for deserialization
    }

    public KeyValueDataChunk(String name) {
        this(name, false);
    }

    public KeyValueDataChunk(String name, boolean destroyed) {
        super(name, destroyed);

        if (destroyed) {
            // Destroyed chunks never receive entries.
            this.keyValuePairs = Collections.emptyList();
            this.entryTimestamps = Collections.emptyList();
        }
    }

    public List<Data> getKeyValuePairs() {
        return keyValuePairs == null ? Collections.emptyList() : keyValuePairs;
    }

    public List<Long> getEntryTimestamps() {
        return entryTimestamps == null ? Collections.emptyList() : entryTimestamps;
    }

    public void add(Data key, Data value, long maxChunkSizeInBytes, int mapSize) {
        final int entrySize = entrySize(key, value);

        if (keyValuePairs == null) {
            initializeCapacity(
                    entrySize,
                    maxChunkSizeInBytes,
                    mapSize,
                    false
            );
        }

        keyValuePairs.add(key);
        keyValuePairs.add(value);

        addToChunkSize(entrySize);
    }

    public void add(Data key,
                    Data value,
                    long lastUpdateTime,
                    long maxChunkSizeInBytes,
                    int mapSize) {

        final int entrySize = entrySize(key, value) + Bits.LONG_SIZE_IN_BYTES;

        if (keyValuePairs == null) {
            initializeCapacity(
                    entrySize,
                    maxChunkSizeInBytes,
                    mapSize,
                    true
            );
        }

        keyValuePairs.add(key);
        keyValuePairs.add(value);
        entryTimestamps.add(lastUpdateTime);

        addToChunkSize(entrySize);
    }

    @Override
    public int getClassId() {
        return RaftDataSerializerConstants.KEY_VALUE_DATA_CHUNK;
    }

    @Override
    void writeDataInternal(ObjectDataOutput out) throws IOException {
        final int kvSize = keyValuePairs == null ? 0 : keyValuePairs.size();
        out.writeInt(kvSize);

        for (int i = 0; i < kvSize; i++) {
            IOUtil.writeData(out, keyValuePairs.get(i));
        }

        final int tsSize = entryTimestamps == null ? 0 : entryTimestamps.size();
        out.writeInt(tsSize);
        for (int i = 0; i < tsSize; i++) {
            out.writeLong(entryTimestamps.get(i));
        }
    }

    @Override
    void readDataInternal(ObjectDataInput in) throws IOException {
        final int keyValueCount = in.readInt();
        keyValuePairs = keyValueCount == 0 ? null : new ArrayList<>(keyValueCount);
        for (int i = 0; i < keyValueCount; i++) {
            keyValuePairs.add(IOUtil.readData(in));
        }

        final int timestampCount = in.readInt();
        entryTimestamps = timestampCount == 0 ? null : new ArrayList<>(timestampCount);
        for (int i = 0; i < timestampCount; i++) {
            entryTimestamps.add(in.readLong());
        }
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof KeyValueDataChunk that)) {
            return false;
        }

        if (!super.equals(o)) {
            return false;
        }

        return Objects.equals(getKeyValuePairs(), that.getKeyValuePairs())
                && Objects.equals(getEntryTimestamps(), that.getEntryTimestamps());
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + Objects.hashCode(getKeyValuePairs());
        result = 31 * result + Objects.hashCode(getEntryTimestamps());
        return result;
    }

    @Override
    public String toString() {
        return "KeyValueDataChunk{"
                + super.toString()
                + ", keyValuePairs.size()=" + getKeyValuePairs().size()
                + ", entryTimestamps.size()=" + getEntryTimestamps().size()
                + "} ";

    }

    private void initializeCapacity(int firstEntryBytes,
                                    long maxChunkSizeInBytes,
                                    int mapSize,
                                    boolean trackTimestamps) {

        assert maxChunkSizeInBytes > 0
                : "maxChunkSizeInBytes must be positive: " + maxChunkSizeInBytes;

        assert firstEntryBytes > 0
                : "firstEntryBytes must be positive: " + firstEntryBytes;

        assert mapSize >= 0
                : "mapSize cannot be negative: " + mapSize;

        int estimated =
                calculateEstimatedCapacity(
                        firstEntryBytes,
                        maxChunkSizeInBytes,
                        mapSize
                );

        this.keyValuePairs =
                new ArrayList<>(estimated * KEY_VALUE_STRIDE);

        this.entryTimestamps =
                trackTimestamps
                        ? new ArrayList<>(estimated)
                        : new ArrayList<>(0);
    }

    private static int calculateEstimatedCapacity(int firstEntryBytes,
                                                  long maxChunkSizeInBytes,
                                                  int mapSize) {
        if (mapSize == 0) {
            return 0;
        }

        long estimated = Math.max(1, maxChunkSizeInBytes / firstEntryBytes);
        estimated = Math.min(estimated, mapSize);

        return (int) Math.min(estimated, Integer.MAX_VALUE / KEY_VALUE_STRIDE);
    }

    private static int entrySize(Data key, Data value) {
        return key.dataSize() + value.dataSize();
    }
}
