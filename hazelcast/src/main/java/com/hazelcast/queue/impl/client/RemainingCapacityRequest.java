/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.queue.impl.client;

import com.hazelcast.client.impl.client.CallableClientRequest;
import com.hazelcast.client.impl.client.RetryableRequest;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.queue.impl.QueueContainer;
import com.hazelcast.queue.impl.QueuePortableHook;
import com.hazelcast.queue.impl.QueueService;
import com.hazelcast.queue.impl.SizeOperation;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.QueuePermission;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.InvocationBuilder;

import java.io.IOException;
import java.security.Permission;

/**
 * Request for remaining capacity of Queue.
 */
public class RemainingCapacityRequest extends CallableClientRequest implements RetryableRequest {

    protected String name;

    public RemainingCapacityRequest() {
    }

    public RemainingCapacityRequest(String name) {
        this.name = name;
    }

    @Override
    public int getFactoryId() {
        return QueuePortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return QueuePortableHook.REMAINING_CAPACITY;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
    }

    @Override
    public Object call() throws Exception {
        String partitionKey = StringPartitioningStrategy.getPartitionKey(name);
        InternalPartitionService partitionService = clientEngine.getPartitionService();
        int partitionId = partitionService.getPartitionId(partitionKey);
        SizeOperation operation = new SizeOperation(name);
        InvocationBuilder builder = operationService.createInvocationBuilder(getServiceName(), operation, partitionId);
        InternalCompletableFuture<Integer> future = builder.invoke();
        Integer size = future.get();

        QueueService service = getService();
        QueueContainer container = service.getOrCreateContainer(name, true);
        return container.getConfig().getMaxSize() - size;
    }

    @Override
    public String getServiceName() {
        return QueueService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new QueuePermission(name, ActionConstants.ACTION_READ);
    }

    @Override
    public String getDistributedObjectName() {
        return name;
    }

    @Override
    public String getMethodName() {
        return "remainingCapacity";
    }
}
