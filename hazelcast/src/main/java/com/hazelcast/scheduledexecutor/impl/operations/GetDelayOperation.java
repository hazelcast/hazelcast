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
import com.hazelcast.scheduledexecutor.ScheduledTaskHandler;
import com.hazelcast.scheduledexecutor.impl.ScheduledExecutorDataSerializerHook;
import com.hazelcast.spi.impl.operationservice.ReadonlyOperation;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class GetDelayOperation
        extends AbstractSchedulerOperation
        implements ReadonlyOperation {

    private String taskName;

    private TimeUnit unit;

    private long response;

    public GetDelayOperation() {
    }

    public GetDelayOperation(ScheduledTaskHandler descriptor, TimeUnit unit) {
        super(descriptor.getSchedulerName());
        this.taskName = descriptor.getTaskName();
        this.unit = unit;
        setPartitionId(descriptor.getPartitionId());
    }

    @Override
    public void run()
            throws Exception {
        response = getContainer().getDelay(taskName, unit);
    }

    @Override
    public Long getResponse() {
        return response;
    }

    @Override
    public int getClassId() {
        return ScheduledExecutorDataSerializerHook.GET_DELAY_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeString(taskName);
        out.writeString(unit.name());
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        this.taskName = in.readString();
        this.unit = TimeUnit.valueOf(in.readString());
    }
}
