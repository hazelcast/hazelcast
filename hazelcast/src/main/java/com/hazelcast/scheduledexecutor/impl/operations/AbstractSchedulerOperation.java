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

package com.hazelcast.scheduledexecutor.impl.operations;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.scheduledexecutor.impl.DistributedScheduledExecutorService;
import com.hazelcast.scheduledexecutor.impl.ScheduledExecutorContainer;
import com.hazelcast.scheduledexecutor.impl.ScheduledExecutorDataSerializerHook;
import com.hazelcast.spi.impl.operationservice.NamedOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;

import java.io.IOException;
import java.util.concurrent.RejectedExecutionException;

public abstract class AbstractSchedulerOperation
        extends Operation
        implements NamedOperation, PartitionAwareOperation, IdentifiedDataSerializable {

    protected String schedulerName;

    AbstractSchedulerOperation() {
    }

    AbstractSchedulerOperation(String schedulerName) {
        this.schedulerName = schedulerName;
    }

    public String getSchedulerName() {
        return schedulerName;
    }

    @Override
    public String getName() {
        return schedulerName;
    }

    @Override
    public String getServiceName() {
        return DistributedScheduledExecutorService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return ScheduledExecutorDataSerializerHook.F_ID;
    }

    public ScheduledExecutorContainer getContainer() {
        checkNotShutdown();
        DistributedScheduledExecutorService service = getService();
        return service.getPartitionOrMemberBin(getPartitionId()).getOrCreateContainer(schedulerName);
    }

    private void checkNotShutdown() {
        DistributedScheduledExecutorService service = getService();
        if (service.isShutdown(getSchedulerName())) {
            throw new RejectedExecutionException("Executor is shut down.");
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeString(schedulerName);
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        this.schedulerName = in.readString();
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);

        sb.append(", name=").append(schedulerName);
        sb.append(", partitionId=").append(getPartitionId());
    }
}

