/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.replicatedmap.impl.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.ReplicatedMapMergeTypes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Contains multiple merging entries for split-brain healing with a {@link SplitBrainMergePolicy}.
 *
 * @since 3.10
 */
public class MergeOperation extends AbstractNamedSerializableOperation {

    private String name;
    private List<ReplicatedMapMergeTypes<Object, Object>> mergingEntries;
    private SplitBrainMergePolicy<Object, ReplicatedMapMergeTypes<Object, Object>, Object> mergePolicy;

    private transient boolean hasMergedValues;

    public MergeOperation() {
    }

    MergeOperation(String name, List<ReplicatedMapMergeTypes<Object, Object>> mergingEntries,
                   SplitBrainMergePolicy<Object, ReplicatedMapMergeTypes<Object, Object>, Object> mergePolicy) {
        this.name = name;
        this.mergingEntries = mergingEntries;
        this.mergePolicy = mergePolicy;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void run() {
        ReplicatedMapService service = getService();
        ReplicatedRecordStore recordStore = service.getReplicatedRecordStore(name, true, getPartitionId());

        for (ReplicatedMapMergeTypes<Object, Object> mergingEntry : mergingEntries) {
            if (recordStore.merge(mergingEntry, mergePolicy)) {
                hasMergedValues = true;
            }
        }
    }

    @Override
    public Object getResponse() {
        return hasMergedValues;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeString(name);
        out.writeInt(mergingEntries.size());
        for (ReplicatedMapMergeTypes mergingEntry : mergingEntries) {
            out.writeObject(mergingEntry);
        }
        out.writeObject(mergePolicy);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        name = in.readString();
        int size = in.readInt();
        mergingEntries = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            ReplicatedMapMergeTypes mergingEntry = in.readObject();
            mergingEntries.add(mergingEntry);
        }
        mergePolicy = in.readObject();
    }

    @Override
    public int getClassId() {
        return ReplicatedMapDataSerializerHook.MERGE;
    }
}
