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

package com.hazelcast.cardinality.impl.operations;

import com.hazelcast.cardinality.impl.hyperloglog.HyperLogLog;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.SplitBrainMergeEntryView;
import com.hazelcast.spi.SplitBrainMergePolicy;

import java.io.IOException;

import static com.hazelcast.cardinality.impl.CardinalityEstimatorDataSerializerHook.MERGE;
import static com.hazelcast.spi.merge.SplitBrainEntryViews.createSplitBrainMergeEntryView;

public class MergeOperation extends CardinalityEstimatorBackupAwareOperation {

    private SplitBrainMergePolicy mergePolicy;

    private HyperLogLog value;

    private transient HyperLogLog backupValue;

    public MergeOperation() {
    }

    public MergeOperation(String name, SplitBrainMergePolicy mergePolicy, HyperLogLog value) {
        super(name);
        this.mergePolicy = mergePolicy;
        this.value = value;
    }

    @Override
    public void run() throws Exception {
        SplitBrainMergeEntryView<String, HyperLogLog> mergingEntry =
                createSplitBrainMergeEntryView(name, value);
        backupValue = getCardinalityEstimatorContainer().merge(mergingEntry, mergePolicy);
    }

    @Override
    public int getId() {
        return MERGE;
    }

    @Override
    public Operation getBackupOperation() {
        return new SyncBackupOperation(name, backupValue);
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        mergePolicy = in.readObject();
        value = in.readObject();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeObject(mergePolicy);
        out.writeObject(value);
    }
}
