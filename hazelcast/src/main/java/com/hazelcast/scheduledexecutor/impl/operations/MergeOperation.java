/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.namespace.NamespaceUtil;
import com.hazelcast.internal.serialization.impl.SerializationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.scheduledexecutor.impl.DistributedScheduledExecutorService;
import com.hazelcast.scheduledexecutor.impl.ScheduledExecutorContainer;
import com.hazelcast.scheduledexecutor.impl.ScheduledTaskDescriptor;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.ScheduledExecutorMergeTypes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.scheduledexecutor.impl.ScheduledExecutorDataSerializerHook.MERGE;

public class MergeOperation
        extends AbstractBackupAwareSchedulerOperation {

    private List<ScheduledExecutorMergeTypes> mergingEntries;
    private SplitBrainMergePolicy<ScheduledTaskDescriptor, ScheduledExecutorMergeTypes, ScheduledTaskDescriptor> mergePolicy;

    private transient List<ScheduledTaskDescriptor> mergedTasks;

    public MergeOperation() {
        super();
    }

    public MergeOperation(String name, List<ScheduledExecutorMergeTypes> mergingEntries,
                          SplitBrainMergePolicy<ScheduledTaskDescriptor, ScheduledExecutorMergeTypes,
                                  ScheduledTaskDescriptor> mergePolicy) {
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
        mergedTasks = new ArrayList<>();

        for (ScheduledExecutorMergeTypes mergingEntry : mergingEntries) {
            ScheduledTaskDescriptor merged = container.merge(mergingEntry, mergePolicy);
            if (merged != null) {
                mergedTasks.add(merged);
            }
        }

        container.promoteSuspended();
    }

    @Override
    public int getClassId() {
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
        SerializationUtil.writeList(mergingEntries, out);
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        mergePolicy = NamespaceUtil.callWithNamespace(
                in::readObject,
                schedulerName,
                DistributedScheduledExecutorService::lookupNamespace
        );
        mergingEntries = SerializationUtil.readList(in);
    }
}
