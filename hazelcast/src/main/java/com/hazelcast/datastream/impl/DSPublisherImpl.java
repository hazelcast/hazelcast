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

import com.hazelcast.datastream.DataStreamPublisher;
import com.hazelcast.datastream.impl.operations.AppendOperation;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.partition.IPartitionService;

import java.util.concurrent.ThreadLocalRandom;

import static com.hazelcast.util.Preconditions.checkNotNull;

public class DSPublisherImpl<R> implements DataStreamPublisher<R> {

    private final InternalSerializationService serializationService;
    private final OperationService operationService;
    private final String name;
    private final IPartitionService partitionService;

    DSPublisherImpl(InternalSerializationService serializationService,
                    OperationService operationService,
                    IPartitionService partitionService,
                    String name) {
        this.serializationService = serializationService;
        this.operationService = operationService;
        this.name = name;
        this.partitionService = partitionService;
    }

    public <P> void publish(P partitionKey, R record) {
        publishAsync(partitionKey, record);
    }

    @Override
    public void publish(R record) {
        ThreadLocalRandom threadLocalRandom = ThreadLocalRandom.current();
        int partitionId = threadLocalRandom.nextInt(partitionService.getPartitionCount());
        publishAsync(partitionId, record).join();
    }

    @Override
    public <P> InternalCompletableFuture<R> publishAsync(P partitionKey, R record) {
        return publishAsync(partitionService.getPartitionId(partitionKey), record);
    }

    private InternalCompletableFuture<R> publishAsync(int partitionId, R record) {
        checkNotNull(record, "record can't be null");

        Operation op = new AppendOperation(name, serializationService.toData(record))
                .setPartitionId(partitionId);

        return operationService.invokeOnPartition(op);
    }
}
