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

import com.hazelcast.datastream.DataFrame;
import com.hazelcast.datastream.DataInputStream;
import com.hazelcast.datastream.DataOutputStream;
import com.hazelcast.datastream.DataStream;
import com.hazelcast.datastream.impl.operations.HeadOperation;
import com.hazelcast.datastream.impl.operations.IteratorOperation;
import com.hazelcast.datastream.impl.operations.TailOperation;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.partition.IPartitionService;

import java.util.Iterator;
import java.util.List;

import static java.util.Collections.singletonList;

public class DSProxy<R> extends AbstractDistributedObject<DSService> implements DataStream<R> {

    protected final IPartitionService partitionService;
    protected final OperationService operationService;
    private final String name;
    private final InternalSerializationService serializationService;
    private final DataFrameImpl<R> frame;

    public DSProxy(String name, NodeEngine nodeEngine, DSService service) {
        super(nodeEngine, service);
        this.name = name;
        this.partitionService = nodeEngine.getPartitionService();
        this.operationService = nodeEngine.getOperationService();
        this.serializationService = (InternalSerializationService) nodeEngine.getSerializationService();
        this.frame = new DataFrameImpl<>(name, operationService, nodeEngine, service);
    }

    @Override
    public <P> long tail(P partitionKey) {
        int partitionId = partitionService.getPartitionId(partitionKey);
        InternalCompletableFuture<Object> future = operationService
                .invokeOnPartition(new TailOperation(name).setPartitionId(partitionId));
        return (Long) future.join();
    }

    @Override
    public <P> long head(P partitionKey) {
        int partitionId = partitionService.getPartitionId(partitionKey);
        InternalCompletableFuture<Object> future = operationService
                .invokeOnPartition(new HeadOperation(name).setPartitionId(partitionId));
        return (Long) future.join();
    }

    public Iterator<R> iterator(int partitionId) {
        Operation op = new IteratorOperation(name).setPartitionId(partitionId);
        return (Iterator) operationService.invokeOnPartition(op).join();
    }

    @Override
    public DataOutputStream<R> newOutputStream() {
        return new DataOutputStreamImpl<>(serializationService, operationService, partitionService, name);
    }

    @Override
    public DataInputStream<R> newInputStream(int partition, long offset) {
        return newInputStream(singletonList(partition), singletonList(offset));
    }

    @Override
    public DataInputStream<R> newInputStream(List<Integer> partitionIds, List<Long> offsets) {
        return new DataInputStreamImpl<>(serializationService, getService(), name, partitionIds, offsets);
    }

    @Override
    public <P> DataInputStream<R> newInputStream(List<Long> offsets) {
        return new DataInputStreamImpl<>(serializationService, getService(), name, null, offsets);
    }

    @Override
    public DataFrame<R> asFrame() {
        return frame;
    }

    @Override
    public String getServiceName() {
        return DSService.SERVICE_NAME;
    }

    @Override
    public String getName() {
        return name;
    }
}
