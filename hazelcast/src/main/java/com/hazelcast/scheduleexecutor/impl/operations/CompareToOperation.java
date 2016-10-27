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
import com.hazelcast.scheduleexecutor.ScheduledTaskHandler;
import com.hazelcast.scheduleexecutor.impl.ScheduledExecutorDataSerializerHook;

import java.io.IOException;
import java.util.concurrent.Delayed;

/**
 * Created by Thomas Kountis.
 */
public class CompareToOperation
        extends AbstractSchedulerOperation {

    private String taskName;

    private Delayed delayed;

    private long response;

    public CompareToOperation() {
    }

    public CompareToOperation(ScheduledTaskHandler descriptor, Delayed delayed) {
        super(descriptor.getSchedulerName());
        this.taskName = descriptor.getTaskName();
        this.delayed = delayed;
        setPartitionId(descriptor.getPartitionId());
    }

    @Override
    public void run()
            throws Exception {
        response = getContainer().compareTo(taskName, delayed);
    }

    @Override
    public Long getResponse() {
        return response;
    }

    @Override
    public int getId() {
        return ScheduledExecutorDataSerializerHook.COMPARE_TO_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeUTF(taskName);
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        this.taskName = in.readUTF();
    }
}
