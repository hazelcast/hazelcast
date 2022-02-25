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

package com.hazelcast.internal.nearcache.impl.invalidation;

import com.hazelcast.cluster.Member;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.map.IMapEvent;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.UUID;

import static com.hazelcast.core.EntryEventType.INVALIDATION;
import static com.hazelcast.internal.nearcache.impl.invalidation.InvalidationUtils.NO_SEQUENCE;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkPositive;

/**
 * Root interface for Near Cache invalidation data.
 */
public abstract class Invalidation implements IMapEvent, IdentifiedDataSerializable {

    private String dataStructureName;
    private UUID sourceUuid;
    private UUID partitionUuid;
    private long sequence = NO_SEQUENCE;

    public Invalidation() {
    }

    public Invalidation(String dataStructureName) {
        this.dataStructureName = checkNotNull(dataStructureName, "dataStructureName cannot be null");
    }

    public Invalidation(String dataStructureName, UUID sourceUuid, UUID partitionUuid, long sequence) {
        this.dataStructureName = checkNotNull(dataStructureName, "dataStructureName cannot be null");
        // sourceUuid can be null.
        this.sourceUuid = sourceUuid;
        this.partitionUuid = checkNotNull(partitionUuid, "partitionUuid cannot be null");
        this.sequence = checkPositive("sequence", sequence);
    }

    public final UUID getPartitionUuid() {
        return partitionUuid;
    }

    public final UUID getSourceUuid() {
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
        return dataStructureName;
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
        out.writeString(dataStructureName);
        UUIDSerializationUtil.writeUUID(out, sourceUuid);
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
        dataStructureName = in.readString();
        sourceUuid = UUIDSerializationUtil.readUUID(in);
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
        return "dataStructureName='" + dataStructureName + "', sourceUuid='" + sourceUuid
                + "', partitionUuid='" + partitionUuid + ", sequence=" + sequence;
    }
}
