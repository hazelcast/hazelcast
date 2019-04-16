/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.datastream.impl;

import com.hazelcast.core.IMap;
import com.hazelcast.datastream.DataOutputStream;
import com.hazelcast.datastream.impl.operations.AppendOperation;
import com.hazelcast.datastream.impl.operations.FillOperation;
import com.hazelcast.datastream.impl.operations.PopulateOperationFactory;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.util.function.Supplier;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static com.hazelcast.util.Preconditions.checkNotNegative;
import static com.hazelcast.util.Preconditions.checkNotNull;

class DataOutputStreamImpl<R> implements DataOutputStream<R> {

    private final InternalSerializationService serializationService;
    private final OperationService operationService;
    private final String name;
    private final IPartitionService partitionService;

    DataOutputStreamImpl(InternalSerializationService serializationService,
                         OperationService operationService,
                         IPartitionService partitionService,
                         String name) {
        this.serializationService = serializationService;
        this.operationService = operationService;
        this.name = name;
        this.partitionService = partitionService;
    }

    @Override
    public void fill(long count, Supplier<R> supplier) {
        checkNotNull(supplier, "supplier can't be null");
        checkNotNegative(count, "count can't be smaller than 0");

        int partitionCount = partitionService.getPartitionCount();
        long countPerPartition = count / partitionCount;
        long remaining = count % partitionCount;

        List<InternalCompletableFuture> futures = new LinkedList<>();
        for (int k = 0; k < partitionCount; k++) {
            long c = k == partitionCount - 1
                    ? countPerPartition + remaining
                    : countPerPartition;

            // we need to clone the supplier
            Supplier s = serializationService.toObject(serializationService.toData(supplier));
            Operation op = new FillOperation(name, s, c)
                    .setPartitionId(k);
            InternalCompletableFuture<Object> f = operationService.invokeOnPartition(op);
            futures.add(f);
        }

        for (InternalCompletableFuture f : futures) {
            f.join();
        }
    }

    @Override
    public void populate(IMap src) {
        checkNotNull(src, "map can't be null");

        try {
            operationService.invokeOnAllPartitions(
                    DSService.SERVICE_NAME, new PopulateOperationFactory(name, src.getName()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public <P> long write(P partitionKey, R record) {
        return writeAsync(partitionKey, record).join();
    }

    @Override
    public long write(R record) {
       return writeAsync(record).join();
    }

    @Override
    public InternalCompletableFuture<Long> writeAsync(R record) {
        ThreadLocalRandom threadLocalRandom = ThreadLocalRandom.current();
        int partitionId = threadLocalRandom.nextInt(partitionService.getPartitionCount());
        return publishAsync(partitionId, record);
    }

    @Override
    public <P> InternalCompletableFuture<Long> writeAsync(P partitionKey, R record) {
        return publishAsync(partitionService.getPartitionId(partitionKey), record);
    }

    private InternalCompletableFuture<Long> publishAsync(int partitionId, R record) {
        checkNotNull(record, "record can't be null");

        Operation op = new AppendOperation(name, serializationService.toData(record))
                .setPartitionId(partitionId);

        return operationService.invokeOnPartition(op);
    }
}
