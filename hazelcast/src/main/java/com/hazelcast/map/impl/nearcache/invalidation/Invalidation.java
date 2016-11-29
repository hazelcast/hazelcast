/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.nearcache.invalidation;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.IMapEvent;
import com.hazelcast.core.Member;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.UUID;

import static com.hazelcast.core.EntryEventType.INVALIDATION;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.checkPositive;

/**
 * Root interface for Near Cache invalidation data.
 */
public abstract class Invalidation implements IMapEvent, IdentifiedDataSerializable {

    public static final long NO_SEQUENCE = -1L;

    private String mapName;
    private String sourceUuid;
    private UUID partitionUuid;
    private long sequence = NO_SEQUENCE;

    public Invalidation() {
    }

    public Invalidation(String mapName) {
        this.mapName = checkNotNull(mapName, "mapName cannot be null");
    }

    public Invalidation(String mapName, String sourceUuid, UUID partitionUuid) {
        this.mapName = checkNotNull(mapName, "mapName cannot be null");
        this.sourceUuid = checkNotNull(sourceUuid, "sourceUuid cannot be null");
        this.partitionUuid = partitionUuid;
    }

    public Invalidation(String mapName, String sourceUuid, UUID partitionUuid, long sequence) {
        this.mapName = checkNotNull(mapName, "mapName cannot be null");
        this.partitionUuid = checkNotNull(partitionUuid, "partitionUuid cannot be null");
        this.sequence = checkPositive(sequence, "sequence should be positive");
        this.sourceUuid = checkNotNull(sourceUuid, "sourceUuid cannot be null");
    }

    public final UUID getPartitionUuid() {
        return partitionUuid;
    }

    public final String getSourceUuid() {
        return sourceUuid;
    }

    public final long getSequence() {
        return sequence;
    }

    public Data getKey() {
        throw new UnsupportedOperationException("getKey is not supported");
    }

    @Override
    public final String getName() {
        return mapName;
    }

    @Override
    public Member getMember() {
        throw new UnsupportedOperationException();
    }

    @Override
    public EntryEventType getEventType() {
        return INVALIDATION;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(mapName);
        out.writeUTF(sourceUuid);
        out.writeLong(sequence);

        boolean nullUuid = partitionUuid == null;
        out.writeBoolean(nullUuid);
        if (!nullUuid) {
            out.writeLong(partitionUuid.getMostSignificantBits());
            out.writeLong(partitionUuid.getLeastSignificantBits());
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        mapName = in.readUTF();
        sourceUuid = in.readUTF();
        sequence = in.readLong();
        boolean nullUuid = in.readBoolean();
        if (!nullUuid) {
            partitionUuid = new UUID(in.readLong(), in.readLong());
        }
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public String toString() {
        return "mapName='" + mapName + "', sourceUuid='" + sourceUuid
                + "', partitionUuid='" + partitionUuid + ", sequence=" + sequence;
    }
}
