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

package com.hazelcast.sql.impl.exec.io;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.row.RowBatch;
import com.hazelcast.sql.impl.partitioner.RowPartitioner;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Send qualifier for unicast sender.
 */
public class UnicastOutboxSendQualifier implements OutboxSendQualifier {

    private final RowPartitioner partitioner;
    private final int[] partitionOutboxIndexes;
    private final InternalSerializationService serializationService;

    private RowBatch batch;
    private int[] cachedPartitions;
    private boolean[] cachePartitionFlags;

    private int outboxIndex;

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "This is an internal class")
    public UnicastOutboxSendQualifier(
        RowPartitioner partitioner,
        int[] partitionOutboxIndexes,
        InternalSerializationService serializationService
    ) {
        this.partitioner = partitioner;
        this.partitionOutboxIndexes = partitionOutboxIndexes;
        this.serializationService = serializationService;
    }

    public void setBatch(RowBatch batch) {
        this.batch = batch;

        int rowCount = batch.getRowCount();

        if (cachedPartitions == null || cachedPartitions.length < rowCount) {
            cachedPartitions = new int[rowCount];
        }

        cachePartitionFlags = new boolean[rowCount];
    }

    public void setOutboxIndex(int outboxIndex) {
        this.outboxIndex = outboxIndex;
    }

    @Override
    public boolean shouldSend(int rowIndex) {
        return getOutboxIndex(rowIndex) == outboxIndex;
    }

    private int getOutboxIndex(int rowIndex) {
        int partition = getPartition(rowIndex);

        return partitionOutboxIndexes[partition];
    }

    private int getPartition(int rowIndex) {
        assert rowIndex < batch.getRowCount();

        if (cachePartitionFlags[rowIndex]) {
            return cachedPartitions[rowIndex];
        }

        Row row = batch.getRow(rowIndex);

        int partition = partitioner.getPartition(row, partitionOutboxIndexes.length, serializationService);

        cachedPartitions[rowIndex] = partition;
        cachePartitionFlags[rowIndex] = true;

        return partition;
    }
}
