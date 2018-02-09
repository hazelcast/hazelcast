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

package com.hazelcast.concurrent.atomiclong.operations;

import com.hazelcast.concurrent.atomiclong.AtomicLongService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.merge.MergeDataHolder;
import com.hazelcast.spi.SplitBrainMergePolicy;

import java.io.IOException;

import static com.hazelcast.concurrent.atomiclong.AtomicLongDataSerializerHook.MERGE;
import static com.hazelcast.spi.merge.MergeDataHolders.createSplitBrainMergeEntryView;

/**
 * Contains a merge value for split-brain healing with a {@link SplitBrainMergePolicy}.
 *
 * @since 3.10
 */
public class MergeOperation extends AtomicLongBackupAwareOperation {

    private SplitBrainMergePolicy mergePolicy;
    private long mergingValue;

    private transient Long backupValue;

    public MergeOperation() {
    }

    public MergeOperation(String name, SplitBrainMergePolicy mergePolicy, long mergingValue) {
        super(name);
        this.mergePolicy = mergePolicy;
        this.mergingValue = mergingValue;
    }

    @Override
    public void run() throws Exception {
        AtomicLongService service = getService();
        boolean isExistingContainer = service.containsAtomicLong(name);

        MergeDataHolder<Long> mergeDataHolder = createSplitBrainMergeEntryView(mergingValue);
        backupValue = getLongContainer().merge(mergeDataHolder, mergePolicy, isExistingContainer);
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
        out.writeLong(mergingValue);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        mergePolicy = in.readObject();
        mergingValue = in.readLong();
    }
}
