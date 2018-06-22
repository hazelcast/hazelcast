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

package com.hazelcast.cache.impl.operation;

import com.hazelcast.cache.impl.CacheDataSerializerHook;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.CacheMergeTypes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.hazelcast.util.MapUtil.createHashMap;

/**
 * Contains multiple merging entries for split-brain healing with a {@link SplitBrainMergePolicy}.
 *
 * @since 3.10
 */
public class CacheMergeOperation extends CacheOperation implements BackupAwareOperation {

    private List<CacheMergeTypes> mergingEntries;
    private SplitBrainMergePolicy<Data, CacheMergeTypes> mergePolicy;

    private transient boolean hasBackups;
    private transient Map<Data, CacheRecord> backupRecords;

    public CacheMergeOperation() {
    }

    public CacheMergeOperation(String name, List<CacheMergeTypes> mergingEntries,
                               SplitBrainMergePolicy<Data, CacheMergeTypes> mergePolicy) {
        super(name);
        this.mergingEntries = mergingEntries;
        this.mergePolicy = mergePolicy;
    }

    @Override
    protected void beforeRunInternal() {
        hasBackups = getSyncBackupCount() + getAsyncBackupCount() > 0;
        if (hasBackups) {
            backupRecords = createHashMap(mergingEntries.size());
        }
    }

    @Override
    public void run() {
        for (CacheMergeTypes mergingEntry : mergingEntries) {
            merge(mergingEntry);
        }
    }

    private void merge(CacheMergeTypes mergingEntry) {
        Data dataKey = mergingEntry.getKey();

        CacheRecord backupRecord = recordStore.merge(mergingEntry, mergePolicy);
        if (backupRecord != null) {
            backupRecords.put(dataKey, backupRecord);
        }
        if (recordStore.isWanReplicationEnabled()) {
            if (backupRecord != null) {
                publishWanUpdate(dataKey, backupRecord);
            } else {
                publishWanRemove(dataKey);
            }
        }
    }

    @Override
    public Object getResponse() {
        return !backupRecords.isEmpty();
    }

    @Override
    public boolean shouldBackup() {
        return hasBackups && !backupRecords.isEmpty();
    }

    @Override
    public Operation getBackupOperation() {
        return new CachePutAllBackupOperation(name, backupRecords);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(mergingEntries.size());
        for (CacheMergeTypes mergingEntry : mergingEntries) {
            out.writeObject(mergingEntry);
        }
        out.writeObject(mergePolicy);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        mergingEntries = new ArrayList<CacheMergeTypes>(size);
        for (int i = 0; i < size; i++) {
            CacheMergeTypes mergingEntry = in.readObject();
            mergingEntries.add(mergingEntry);
        }
        mergePolicy = in.readObject();
    }

    @Override
    public int getId() {
        return CacheDataSerializerHook.MERGE;
    }
}
