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

package com.hazelcast.replicatedmap.impl.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.spi.SplitBrainMergeEntryView;
import com.hazelcast.spi.SplitBrainMergePolicy;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Contains multiple merging entries for split-brain healing with a {@link SplitBrainMergePolicy}.
 *
 * @since 3.10
 */
public class MergeOperation extends AbstractNamedSerializableOperation {

    private String name;
    private List<SplitBrainMergeEntryView<Object, Object>> mergeEntries;
    private SplitBrainMergePolicy mergePolicy;

    private transient boolean hasMergedValues;

    public MergeOperation() {
    }

    MergeOperation(String name, List<SplitBrainMergeEntryView<Object, Object>> mergeEntries, SplitBrainMergePolicy policy) {
        this.name = name;
        this.mergeEntries = mergeEntries;
        this.mergePolicy = policy;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void run() {
        ReplicatedMapService service = getService();
        ReplicatedRecordStore recordStore = service.getReplicatedRecordStore(name, true, getPartitionId());

        for (SplitBrainMergeEntryView<Object, Object> mergingEntry : mergeEntries) {
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
        out.writeUTF(name);
        out.writeInt(mergeEntries.size());
        for (SplitBrainMergeEntryView<Object, Object> mergeEntry : mergeEntries) {
            out.writeObject(mergeEntry);
        }
        out.writeObject(mergePolicy);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        name = in.readUTF();
        mergeEntries = new LinkedList<SplitBrainMergeEntryView<Object, Object>>();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            SplitBrainMergeEntryView<Object, Object> mergeEntry = in.readObject();
            mergeEntries.add(mergeEntry);
        }
        mergePolicy = in.readObject();
    }

    @Override
    public int getId() {
        return ReplicatedMapDataSerializerHook.MERGE;
    }
}
