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

package com.hazelcast.concurrent.atomicreference.operations;

import com.hazelcast.concurrent.atomicreference.AtomicReferenceService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.SplitBrainMergeEntryView;
import com.hazelcast.spi.SplitBrainMergePolicy;

import java.io.IOException;

import static com.hazelcast.concurrent.atomicreference.AtomicReferenceDataSerializerHook.MERGE;
import static com.hazelcast.spi.merge.SplitBrainEntryViews.createSplitBrainMergeEntryView;

/**
 * Contains a merge value for split-brain healing with a {@link SplitBrainMergePolicy}.
 *
 * @since 3.10
 */
public class MergeOperation extends AtomicReferenceBackupAwareOperation {

    private SplitBrainMergePolicy mergePolicy;
    private Data mergingValue;

    private transient Data backupValue;

    public MergeOperation() {
    }

    public MergeOperation(String name, SplitBrainMergePolicy mergePolicy, Data mergingValue) {
        super(name);
        this.mergePolicy = mergePolicy;
        this.mergingValue = mergingValue;
    }

    @Override
    public void run() throws Exception {
        AtomicReferenceService service = getService();
        boolean isExistingContainer = service.containsReferenceContainer(name);

        SplitBrainMergeEntryView<Boolean, Data> mergingEntry = createSplitBrainMergeEntryView(isExistingContainer, mergingValue);
        backupValue = getReferenceContainer().merge(mergingEntry, mergePolicy);
    }

    @Override
    public boolean shouldBackup() {
        return backupValue != null;
    }

    @Override
    public Operation getBackupOperation() {
        return new SetBackupOperation(name, backupValue);
    }

    @Override
    public int getId() {
        return MERGE;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(mergePolicy);
        out.writeData(mergingValue);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        mergePolicy = in.readObject();
        mergingValue = in.readData();
    }
}
