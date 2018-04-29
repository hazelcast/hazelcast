/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.scheduledexecutor.impl.ScheduledExecutorContainer;
import com.hazelcast.scheduledexecutor.impl.ScheduledTaskDescriptor;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.ScheduledExecutorMergeTypes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.scheduledexecutor.impl.ScheduledExecutorDataSerializerHook.MERGE;

public class MergeOperation
        extends AbstractBackupAwareSchedulerOperation {

    private List<ScheduledExecutorMergeTypes> mergingEntries;
    private SplitBrainMergePolicy<ScheduledTaskDescriptor, ScheduledExecutorMergeTypes> mergePolicy;

    private transient List<ScheduledTaskDescriptor> mergedTasks;

    public MergeOperation() {
        super();
    }

    public MergeOperation(String name, List<ScheduledExecutorMergeTypes> mergingEntries,
                          SplitBrainMergePolicy<ScheduledTaskDescriptor, ScheduledExecutorMergeTypes> mergePolicy) {
        super(name);
        this.mergingEntries = mergingEntries;
        this.mergePolicy = mergePolicy;
    }

    @Override
    public boolean shouldBackup() {
        return super.shouldBackup() && mergedTasks != null && !mergedTasks.isEmpty();
    }

    @Override
    public void run()
            throws Exception {
        ScheduledExecutorContainer container = getContainer();
        mergedTasks = new ArrayList<ScheduledTaskDescriptor>();

        for (ScheduledExecutorMergeTypes mergingEntry : mergingEntries) {
            ScheduledTaskDescriptor merged = container.merge(mergingEntry, mergePolicy);
            if (merged != null) {
                mergedTasks.add(merged);
            }
        }

        container.promoteSuspended();
    }

    @Override
    public int getId() {
        return MERGE;
    }

    @Override
    public Operation getBackupOperation() {
        return new MergeBackupOperation(getSchedulerName(), mergedTasks);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeObject(mergePolicy);
        out.writeInt(mergingEntries.size());
        for (ScheduledExecutorMergeTypes mergingEntry : mergingEntries) {
            out.writeObject(mergingEntry);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        mergePolicy = in.readObject();
        int size = in.readInt();
        mergingEntries = new ArrayList<ScheduledExecutorMergeTypes>(size);
        for (int i = 0; i < size; i++) {
            ScheduledExecutorMergeTypes mergingEntry = in.readObject();
            mergingEntries.add(mergingEntry);
        }
    }
}
