/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.scheduledexecutor.impl.ScheduledExecutorDataSerializerHook;
import com.hazelcast.scheduledexecutor.impl.TaskDefinition;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.impl.MutatingOperation;

import java.io.IOException;

public class ScheduleTaskBackupOperation
        extends AbstractSchedulerOperation implements BackupOperation, MutatingOperation {

    private TaskDefinition definition;

    public ScheduleTaskBackupOperation() {
    }

    public ScheduleTaskBackupOperation(String schedulerName, TaskDefinition definition) {
        super(schedulerName);
        this.definition = definition;
    }

    @Override
    public void run()
            throws Exception {
        getContainer().stash(definition);
    }

    @Override
    public int getId() {
        return ScheduledExecutorDataSerializerHook.SCHEDULE_BACKUP_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeObject(definition);
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        this.definition = in.readObject();
    }
}
