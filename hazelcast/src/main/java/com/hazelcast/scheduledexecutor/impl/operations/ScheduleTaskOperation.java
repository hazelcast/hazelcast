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

package com.hazelcast.scheduledexecutor.impl.operations;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.scheduledexecutor.impl.ScheduledExecutorDataSerializerHook;
import com.hazelcast.scheduledexecutor.impl.TaskDefinition;
import com.hazelcast.spi.Operation;

import java.io.IOException;

/**
 * Created by Thomas Kountis.
 */
public class ScheduleTaskOperation
        extends AbstractBackupAwareSchedulerOperation {

    private TaskDefinition definition;

    private Data definitionData;

    public ScheduleTaskOperation() {
    }

    public ScheduleTaskOperation(String schedulerName, Data definitionData) {
        super(schedulerName);
        this.definitionData = definitionData;
    }

    public ScheduleTaskOperation(String schedulerName, TaskDefinition definition) {
        super(schedulerName);
        this.definitionData = getNodeEngine().getSerializationService().toData(definition);
    }

    @Override
    public void run()
            throws Exception {
        System.err.println("Run: " + toString());
        definition = getNodeEngine().getSerializationService().toObject(definitionData);
        getContainer().schedule(definition);
    }

    @Override
    public int getId() {
        return ScheduledExecutorDataSerializerHook.SCHEDULE_OP;
    }

    @Override
    public Operation getBackupOperation() {
        boolean isMemberOperation = getPartitionId() == -1;
        if (isMemberOperation) {
            return null;
        }

        return new ScheduleTaskBackupOperation(schedulerName, definition);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeData(definitionData);
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        this.definitionData = in.readData();
    }
}
