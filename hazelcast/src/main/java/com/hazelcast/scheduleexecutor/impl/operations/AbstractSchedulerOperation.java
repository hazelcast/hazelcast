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

package com.hazelcast.scheduleexecutor.impl.operations;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.scheduleexecutor.impl.DistributedScheduledExecutorService;
import com.hazelcast.scheduleexecutor.impl.ScheduledExecutorContainer;
import com.hazelcast.scheduleexecutor.impl.ScheduledExecutorDataSerializerHook;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;

/**
 * Created by Thomas Kountis.
 */
public abstract class AbstractSchedulerOperation
        extends Operation
        implements PartitionAwareOperation, IdentifiedDataSerializable {

    protected String schedulerName;

    AbstractSchedulerOperation() { }

    AbstractSchedulerOperation(String schedulerName) {
        this.schedulerName = schedulerName;
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
        DistributedScheduledExecutorService service = getService();
        return service.getPartition(getPartitionId()).getOrCreateContainer(schedulerName);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        out.writeUTF(schedulerName);
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        this.schedulerName = in.readUTF();
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);

        sb.append(", name=").append(schedulerName);
    }
}

