/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.operation;

import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.SqlDataSerializerHook;

import java.io.IOException;
import java.util.UUID;

/**
 * Base class for query operations.
 */
public abstract class QueryOperation implements IdentifiedDataSerializable {

    public static final int PARTITION_ANY = -1;

    private UUID callerId;

    protected QueryOperation() {
        // No-op.
    }

    public UUID getCallerId() {
        return callerId;
    }

    public void setCallerId(UUID callerId) {
        this.callerId = callerId;
    }

    public int getPartition() {
        return PARTITION_ANY;
    }

    /**
     * Map an arbitrary integer value to a positive integer, which is later used as a logical partition.
     *
     * @param hash Hash.
     * @return Logical partition.
     */
    protected static int getPartitionForHash(int hash) {
        if (hash == Integer.MIN_VALUE) {
            hash = Integer.MAX_VALUE;
        }

        return Math.abs(hash);
    }

    @Override
    public final int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public final void writeData(ObjectDataOutput out) throws IOException {
        UUIDSerializationUtil.writeUUID(out, callerId);

        writeInternal0(out);
    }

    @Override
    public final void readData(ObjectDataInput in) throws IOException {
        callerId = UUIDSerializationUtil.readUUID(in);

        readInternal0(in);
    }

    protected abstract void writeInternal0(ObjectDataOutput out) throws IOException;
    protected abstract void readInternal0(ObjectDataInput in) throws IOException;
}
