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

package com.hazelcast.sql.impl.partitioner;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.row.Row;

import java.io.IOException;

/**
 * Partitioner that is not making actual partitioning mapping all rows to the same partition.
 */
public final class ZeroPartitioner implements RowPartitioner, IdentifiedDataSerializable {

    public static final ZeroPartitioner INSTANCE = new ZeroPartitioner();

    public ZeroPartitioner() {
        // No-op.
    }

    @Override
    public int getPartition(Row row, int partitionCount, InternalSerializationService serializationService) {
        return 0;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        // No-op.
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        // No-op.
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof ZeroPartitioner;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{}";
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.ZERO_PARTITIONER;
    }
}
