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

package com.hazelcast.multimap.impl.operations;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.multimap.impl.MultiMapContainer;
import com.hazelcast.multimap.impl.MultiMapDataSerializerHook;
import com.hazelcast.multimap.impl.MultiMapMergeContainer;
import com.hazelcast.multimap.impl.MultiMapRecord;
import com.hazelcast.multimap.impl.MultiMapValue;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.SplitBrainMergeEntryView;
import com.hazelcast.spi.SplitBrainMergePolicy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.hazelcast.spi.impl.merge.SplitBrainEntryViews.createSplitBrainMergeEntryView;
import static com.hazelcast.util.MapUtil.createHashMap;

/**
 * Contains multiple merge entries for split-brain healing with a {@link SplitBrainMergePolicy}.
 *
 * @since 3.10
 */
public class MergeOperation extends MultiMapOperation implements BackupAwareOperation {

    private List<MultiMapMergeContainer> mergeEntries;
    private SplitBrainMergePolicy mergePolicy;

    private transient Map<Data, Collection<MultiMapRecord>> resultMap;

    public MergeOperation() {
    }

    public MergeOperation(String name, List<MultiMapMergeContainer> mergeEntries,
                          SplitBrainMergePolicy mergePolicy) {
        super(name);
        this.mergeEntries = mergeEntries;
        this.mergePolicy = mergePolicy;
    }

    @Override
    public void run() throws Exception {
        MultiMapContainer container = getOrCreateContainer();
        resultMap = createHashMap(mergeEntries.size());
        for (MultiMapMergeContainer mergeEntry : mergeEntries) {
            Data key = mergeEntry.getKey();
            if (!container.canAcquireLock(key, getCallerUuid(), -1)) {
                Object valueKey = getNodeEngine().getSerializationService().toObject(key);
                getLogger().info("Skipped merging of locked key '" + valueKey + "' on MultiMap '" + name + "'");
                continue;
            }

            SplitBrainMergeEntryView<Data, MultiMapMergeContainer> mergingEntry
                    = createSplitBrainMergeEntryView(key, mergeEntry);
            MultiMapValue result = container.merge(mergingEntry, mergePolicy);
            if (result != null) {
                resultMap.put(key, result.getCollection(false));
                publishEvent(EntryEventType.MERGED, key, result, null);
            }
        }
        response = !resultMap.isEmpty();
    }

    @Override
    public Operation getBackupOperation() {
        return new MergeBackupOperation(name, resultMap);
    }

    @Override
    public boolean shouldBackup() {
        return !resultMap.isEmpty();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(mergeEntries.size());
        for (MultiMapMergeContainer mergingEntry : mergeEntries) {
            out.writeObject(mergingEntry);
        }
        out.writeObject(mergePolicy);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        mergeEntries = new ArrayList<MultiMapMergeContainer>(size);
        for (int i = 0; i < size; i++) {
            MultiMapMergeContainer mergingEntry = in.readObject();
            mergeEntries.add(mergingEntry);
        }
        mergePolicy = in.readObject();
    }

    @Override
    public int getId() {
        return MultiMapDataSerializerHook.MERGE_OPERATION;
    }
}
