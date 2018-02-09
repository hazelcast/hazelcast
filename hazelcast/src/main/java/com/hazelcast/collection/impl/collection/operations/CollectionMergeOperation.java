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

package com.hazelcast.collection.impl.collection.operations;

import com.hazelcast.collection.impl.collection.CollectionContainer;
import com.hazelcast.collection.impl.collection.CollectionDataSerializerHook;
import com.hazelcast.collection.impl.collection.CollectionItem;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.MergeDataHolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.hazelcast.util.MapUtil.createHashMap;

/**
 * Contains multiple merge entries for split-brain healing with a {@link SplitBrainMergePolicy}.
 *
 * @since 3.10
 */
public class CollectionMergeOperation extends CollectionBackupAwareOperation {

    private SplitBrainMergePolicy mergePolicy;
    private List<MergeDataHolder<Data>> mergeData;

    private transient Map<Long, Data> valueMap;

    public CollectionMergeOperation(String name, SplitBrainMergePolicy mergePolicy,
                                    List<MergeDataHolder<Data>> mergeData) {
        super(name);
        this.mergePolicy = mergePolicy;
        this.mergeData = mergeData;
    }

    public CollectionMergeOperation() {
    }

    @Override
    public int getId() {
        return CollectionDataSerializerHook.COLLECTION_MERGE;
    }

    @Override
    public boolean shouldBackup() {
        return valueMap != null && !valueMap.isEmpty();
    }

    @Override
    public Operation getBackupOperation() {
        return new CollectionAddAllBackupOperation(name, valueMap);
    }

    @Override
    public void run() throws Exception {
        CollectionContainer collectionContainer = getOrCreateContainer();
        valueMap = createHashMap(mergeData.size());
        for (MergeDataHolder<Data> mergingEntry : mergeData) {
            CollectionItem mergedItem = collectionContainer.merge(mergingEntry, mergePolicy);
            if (mergedItem != null) {
                valueMap.put(mergedItem.getItemId(), mergedItem.getValue());
            }
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(mergePolicy);
        out.writeInt(mergeData.size());
        for (MergeDataHolder<Data> mergingEntry : mergeData) {
            out.writeObject(mergingEntry);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        mergePolicy = in.readObject();
        int size = in.readInt();
        mergeData = new ArrayList<MergeDataHolder<Data>>(size);
        for (int i = 0; i < size; i++) {
            MergeDataHolder<Data> mergingEntry = in.readObject();
            mergeData.add(mergingEntry);
        }
    }
}
