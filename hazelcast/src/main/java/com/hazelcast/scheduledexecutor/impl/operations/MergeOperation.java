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
import com.hazelcast.spi.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.KeyMergeDataHolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.scheduledexecutor.impl.ScheduledExecutorDataSerializerHook.MERGE;

public class MergeOperation
        extends AbstractBackupAwareSchedulerOperation {

    private SplitBrainMergePolicy policy;

    private List<KeyMergeDataHolder<String, ScheduledTaskDescriptor>> mergingEntries;

    private transient List<ScheduledTaskDescriptor> mergedTasks;

    public MergeOperation() {
        super();
    }

    public MergeOperation(String name, SplitBrainMergePolicy mergePolicy,
                          List<KeyMergeDataHolder<String, ScheduledTaskDescriptor>> mergingEntries) {
        super(name);
        this.policy = mergePolicy;
        this.mergingEntries = mergingEntries;
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

        for (KeyMergeDataHolder<String, ScheduledTaskDescriptor> entry : mergingEntries) {
            ScheduledTaskDescriptor merged = container.merge(entry, policy);
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
        out.writeObject(policy);
        out.writeInt(mergingEntries.size());
        for (KeyMergeDataHolder<String, ScheduledTaskDescriptor> entry : mergingEntries) {
            out.writeObject(entry);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        policy = in.readObject();
        int size = in.readInt();
        mergingEntries = new ArrayList<KeyMergeDataHolder<String, ScheduledTaskDescriptor>>(size);
        for (int i = 0; i < size; i++) {
            KeyMergeDataHolder<String, ScheduledTaskDescriptor> entry = in.readObject();
            mergingEntries.add(entry);
        }
    }
}
