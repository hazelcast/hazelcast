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

package com.hazelcast.cache.impl.client;

import com.hazelcast.cache.impl.CacheDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;

import java.io.IOException;
import java.util.UUID;

public class CacheSingleInvalidationMessage extends CacheInvalidationMessage {

    private Data key;
    private String sourceUuid;
    private UUID partitionUuid;
    private long sequence;

    public CacheSingleInvalidationMessage() {
    }

    public CacheSingleInvalidationMessage(String name, Data key, String sourceUuid, UUID partitionUuid, long sequence) {
        super(name);
        this.key = key;
        this.sourceUuid = sourceUuid;
        this.partitionUuid = partitionUuid;
        this.sequence = sequence;
    }

    @Override
    public Data getKey() {
        return key;
    }

    public long getSequence() {
        return sequence;
    }

    public String getSourceUuid() {
        return sourceUuid;
    }

    public UUID getPartitionUuid() {
        return partitionUuid;
    }

    @Override
    public int getId() {
        return CacheDataSerializerHook.INVALIDATION_MESSAGE;
    }


    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeUTF(sourceUuid);
        boolean hasKey = key != null;
        out.writeBoolean(hasKey);
        if (hasKey) {
            out.writeData(key);
        }
        out.writeLong(sequence);
        out.writeLong(partitionUuid.getMostSignificantBits());
        out.writeLong(partitionUuid.getLeastSignificantBits());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        sourceUuid = in.readUTF();
        if (in.readBoolean()) {
            key = in.readData();
        }
        sequence = in.readLong();
        partitionUuid = new UUID(in.readLong(), in.readLong());
    }


    @Override
    public String toString() {
        return "CacheSingleInvalidationMessage{"
                + "name='" + name + '\''
                + ", key=" + key
                + ", sourceUuid='" + sourceUuid + '\''
                + ", partitionUuid='" + partitionUuid + '\''
                + ", sequence=" + sequence
                + '}';
    }

}
