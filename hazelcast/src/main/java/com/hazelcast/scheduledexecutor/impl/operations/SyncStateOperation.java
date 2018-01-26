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

import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.scheduledexecutor.impl.ScheduledExecutorDataSerializerHook;
import com.hazelcast.scheduledexecutor.impl.ScheduledTaskResult;
import com.hazelcast.scheduledexecutor.impl.ScheduledTaskStatisticsImpl;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.util.Map;

import static com.hazelcast.scheduledexecutor.impl.DistributedScheduledExecutorService.MEMBER_BIN;
import static com.hazelcast.util.MapUtil.createHashMap;

public class SyncStateOperation
        extends AbstractBackupAwareSchedulerOperation {

    protected String taskName;

    protected Map<Object, Object> state;

    protected ScheduledTaskStatisticsImpl stats;

    protected ScheduledTaskResult result;

    private boolean shouldRun;

    public SyncStateOperation() {
    }

    public SyncStateOperation(String schedulerName, String taskName, Map state,
                              ScheduledTaskStatisticsImpl stats, ScheduledTaskResult result) {
        super(schedulerName);
        this.taskName = taskName;
        this.state = state;
        this.stats = stats;
        this.result = result;
    }

    @Override
    public void run()
            throws Exception {

        int partitionId = getPartitionId();
        shouldRun = partitionId == MEMBER_BIN;

        if (partitionId >= 0) {
            Address partitionOwner = getNodeEngine().getPartitionService().getPartitionOwner(partitionId);
            shouldRun = shouldRun || getCallerAddress().equals(partitionOwner);
        }

        if (shouldRun) {
            getContainer().syncState(taskName, state, stats, result);
        }
    }

    @Override
    public boolean shouldBackup() {
        return super.shouldBackup() && shouldRun;
    }

    @Override
    public Operation getBackupOperation() {
        return new SyncBackupStateOperation(schedulerName, taskName, state, stats, result);
    }

    @Override
    public int getId() {
        return ScheduledExecutorDataSerializerHook.SYNC_STATE_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeUTF(taskName);
        out.writeInt(state.size());
        for (Map.Entry entry : state.entrySet()) {
            out.writeObject(entry.getKey());
            out.writeObject(entry.getValue());
        }
        out.writeObject(stats);
        out.writeObject(result);
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        this.taskName = in.readUTF();
        int stateSize = in.readInt();
        this.state = createHashMap(stateSize);
        for (int i = 0; i < stateSize; i++) {
            this.state.put(in.readObject(), in.readObject());
        }
        this.stats = in.readObject();
        this.result = in.readObject();
    }
}
