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

import com.hazelcast.cache.CacheEntryView;
import com.hazelcast.cache.impl.CacheDataSerializerHook;
import com.hazelcast.cache.impl.CacheRecordStore;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.cache.impl.event.CacheWanEventPublisher;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.ServiceNamespace;
import com.hazelcast.spi.ServiceNamespaceAware;
import com.hazelcast.spi.SplitBrainMergeEntryView;
import com.hazelcast.spi.SplitBrainMergePolicy;
import com.hazelcast.spi.impl.AbstractNamedOperation;
import com.hazelcast.spi.serialization.SerializationService;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.hazelcast.cache.impl.CacheEntryViews.createDefaultEntryView;
import static com.hazelcast.util.MapUtil.createHashMap;

/**
 * Contains multiple merging entries for split-brain healing with a {@link SplitBrainMergePolicy}.
 */
public class CacheMergeOperation extends AbstractNamedOperation implements BackupAwareOperation, ServiceNamespaceAware {

    private List<SplitBrainMergeEntryView<Data, Data>> mergeEntries;
    private SplitBrainMergePolicy mergePolicy;

    private transient SerializationService serializationService;
    private transient ICacheRecordStore cache;
    private transient CacheWanEventPublisher wanEventPublisher;

    private transient boolean hasBackups;
    private transient Map<Data, CacheRecord> backupRecords;

    public CacheMergeOperation() {
    }

    CacheMergeOperation(String name, List<SplitBrainMergeEntryView<Data, Data>> mergeEntries, SplitBrainMergePolicy policy) {
        super(name);
        this.mergeEntries = mergeEntries;
        this.mergePolicy = policy;
    }

    @Override
    public void beforeRun() throws Exception {
        serializationService = getNodeEngine().getSerializationService();
        ICacheService cacheService = getService();
        cache = cacheService.getOrCreateRecordStore(name, getPartitionId());
        if (cache.isWanReplicationEnabled()) {
            wanEventPublisher = cacheService.getCacheWanEventPublisher();
        }
        hasBackups = getSyncBackupCount() + getAsyncBackupCount() > 0;
        if (hasBackups) {
            backupRecords = createHashMap(mergeEntries.size());
        }
    }

    @Override
    public void run() {
        for (SplitBrainMergeEntryView<Data, Data> mergingEntry : mergeEntries) {
            merge(mergingEntry);
        }
    }

    private void merge(SplitBrainMergeEntryView<Data, Data> mergingEntry) {
        Data dataKey = mergingEntry.getKey();

        CacheRecord backupRecord = ((CacheRecordStore) cache).merge(mergingEntry, mergePolicy);
        if (backupRecord != null) {
            backupRecords.put(dataKey, backupRecord);
        }
        if (cache.isWanReplicationEnabled()) {
            if (backupRecord != null) {
                CacheEntryView<Data, Data> entryView
                        = createDefaultEntryView(dataKey, serializationService.toData(backupRecord.getValue()), backupRecord);
                wanEventPublisher.publishWanReplicationUpdate(name, entryView);
            } else {
                wanEventPublisher.publishWanReplicationRemove(name, dataKey);
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
    public int getSyncBackupCount() {
        return cache != null ? cache.getConfig().getBackupCount() : 0;
    }

    @Override
    public int getAsyncBackupCount() {
        return cache != null ? cache.getConfig().getAsyncBackupCount() : 0;
    }

    @Override
    public Operation getBackupOperation() {
        return new CachePutAllBackupOperation(name, backupRecords);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(mergeEntries.size());
        for (SplitBrainMergeEntryView<Data, Data> mergeEntry : mergeEntries) {
            out.writeObject(mergeEntry);
        }
        out.writeObject(mergePolicy);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        mergeEntries = new LinkedList<SplitBrainMergeEntryView<Data, Data>>();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            SplitBrainMergeEntryView<Data, Data> mergeEntry = in.readObject();
            mergeEntries.add(mergeEntry);
        }
        mergePolicy = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return CacheDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return CacheDataSerializerHook.MERGE;
    }

    @Override
    public ServiceNamespace getServiceNamespace() {
        ICacheRecordStore recordStore = cache;
        if (recordStore == null) {
            ICacheService service = getService();
            recordStore = service.getOrCreateRecordStore(name, getPartitionId());
        }
        return recordStore.getObjectNamespace();
    }
}
