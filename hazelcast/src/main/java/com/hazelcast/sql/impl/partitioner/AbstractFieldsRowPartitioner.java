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

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.HashUtil;
import com.hazelcast.sql.impl.row.Row;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.EMPTY_PARTITIONING_STRATEGY;

/**
 * Partitioner that calculates row partition based on row field values.
 */
public abstract class AbstractFieldsRowPartitioner implements RowPartitioner {
    @Override
    public final int getPartition(Row row, int partitionCount, InternalSerializationService serializationService) {
        int hash = getHash(row, serializationService);

        return HashUtil.hashToIndex(hash, partitionCount);
    }

    protected abstract int getHash(Row row, InternalSerializationService serializationService);

    protected int getFieldHash(Object val, InternalSerializationService serializationService) {
        // TODO 1: Optimize hashing of primitive types, to avoid serialization
        // TODO 2: Do not use Data for multi-field because in this case the other side is never "declarative"?
        // TODO 3: Make sure that the code is shared with AbstractSerialiazationService.calculatePartitionHash
        Data data = serializationService.toData(val, EMPTY_PARTITIONING_STRATEGY);

        return data != null ? data.getPartitionHash() : 0;
    }
}
