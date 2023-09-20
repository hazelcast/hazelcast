/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.cache.impl.CacheMergeResponse;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.CacheMergeTypes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import static com.hazelcast.wan.impl.CallerProvenance.NOT_WAN;

/**
 * Contains multiple merging entries for split-brain healing with a {@link SplitBrainMergePolicy}.
 *
 * @since 3.10
 */
public class CacheMergeOperation extends CacheOperation implements BackupAwareOperation {

    private List<CacheMergeTypes<Object, Object>> mergingEntries;
    private SplitBrainMergePolicy<Object, CacheMergeTypes<Object, Object>, Object> mergePolicy;

    private transient boolean hasBackups;
    private transient List backupPairs;
    private transient BitSet backupNonReplicatedKeys;

    public CacheMergeOperation() {
    }

    public CacheMergeOperation(String name, List<CacheMergeTypes<Object, Object>> mergingEntries,
                               SplitBrainMergePolicy<Object, CacheMergeTypes<Object, Object>, Object> mergePolicy) {
        super(name);
        this.mergingEntries = mergingEntries;
        this.mergePolicy = mergePolicy;
    }

    @Override
    protected void beforeRunInternal() {
        hasBackups = getSyncBackupCount() + getAsyncBackupCount() > 0;
        if (hasBackups) {
            backupPairs = new ArrayList(mergingEntries.size());
            backupNonReplicatedKeys = new BitSet(mergingEntries.size());
        }
    }

    @Override
    public void run() {
        for (CacheMergeTypes<Object, Object> mergingEntry : mergingEntries) {
            merge(mergingEntry);
        }
    }

    private void merge(CacheMergeTypes<Object, Object> mergingEntry) {
        Data dataKey = (Data) mergingEntry.getRawKey();

        CacheMergeResponse response = recordStore.merge(mergingEntry, mergePolicy, NOT_WAN);
        if (backupPairs != null && response.getResult().isMergeApplied()) {
            if (response.getResult() == CacheMergeResponse.MergeResult.RECORDS_ARE_EQUAL) {
                backupNonReplicatedKeys.set(backupPairs.size() / 2);
            }
            backupPairs.add(dataKey);
            backupPairs.add(response.getRecord());
        }
        if (recordStore.isWanReplicationEnabled()) {
            if (response.getResult().isMergeApplied()) {
                // Don't WAN replicate merge events where values don't change
                if (response.getResult() != CacheMergeResponse.MergeResult.RECORDS_ARE_EQUAL) {
                    publishWanUpdate(dataKey, response.getRecord());
                }
            } else {
                publishWanRemove(dataKey);
            }
        }
    }

    @Override
    public Object getResponse() {
        return hasBackups && !backupPairs.isEmpty();
    }

    @Override
    public boolean shouldBackup() {
        return hasBackups && !backupPairs.isEmpty();
    }

    @Override
    public Operation getBackupOperation() {
        return new CachePutAllBackupOperation(name, backupPairs, backupNonReplicatedKeys);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(mergingEntries.size());
        for (CacheMergeTypes<Object, Object> mergingEntry : mergingEntries) {
            out.writeObject(mergingEntry);
        }
        out.writeObject(mergePolicy);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        mergingEntries = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            CacheMergeTypes<Object, Object> mergingEntry = in.readObject();
            mergingEntries.add(mergingEntry);
        }
        mergePolicy = in.readObject();
    }

    @Override
    public int getClassId() {
        return CacheDataSerializerHook.MERGE;
    }
}
