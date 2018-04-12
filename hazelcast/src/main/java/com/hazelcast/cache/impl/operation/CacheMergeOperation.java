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
import com.hazelcast.spi.impl.AbstractNamedOperation;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.CacheMergeTypes;
import com.hazelcast.spi.serialization.SerializationService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.hazelcast.cache.impl.CacheEntryViews.createDefaultEntryView;
import static com.hazelcast.util.MapUtil.createHashMap;

/**
 * Merges multiple {@link CacheMergeTypes} for split-brain healing with a {@link SplitBrainMergePolicy}.
 *
 * @since 3.10
 */
public class CacheMergeOperation extends AbstractNamedOperation implements BackupAwareOperation, ServiceNamespaceAware {

    private List<CacheMergeTypes> mergingEntries;
    private SplitBrainMergePolicy<Object, CacheMergeTypes> mergePolicy;

    private transient ICacheRecordStore cache;
    private transient Map<Data, CacheRecord> backupRecords;
    private transient CacheWanEventPublisher wanEventPublisher;

    public CacheMergeOperation() {
    }

    public CacheMergeOperation(String name, List<CacheMergeTypes> mergingEntries,
                               SplitBrainMergePolicy<Object, CacheMergeTypes> mergePolicy) {
        super(name);
        this.mergingEntries = mergingEntries;
        this.mergePolicy = mergePolicy;
    }

    @Override
    public void beforeRun() throws Exception {
        ICacheService cacheService = getService();
        cache = cacheService.getOrCreateRecordStore(name, getPartitionId());
        if (getSyncBackupCount() + getAsyncBackupCount() > 0) {
            backupRecords = createHashMap(mergingEntries.size());
        }
        if (cache.isWanReplicationEnabled()) {
            wanEventPublisher = cacheService.getCacheWanEventPublisher();
        }
    }

    @Override
    public void run() {
        SerializationService serializationService = getNodeEngine().getSerializationService();

        for (CacheMergeTypes mergingEntry : mergingEntries) {
            if (cache.merge(mergingEntry, mergePolicy)) {
                Data dataKey = mergingEntry.getKey();
                CacheRecord backupRecord = cache.getRecord(dataKey);
                if (backupRecords != null) {
                    backupRecords.put(dataKey, backupRecord);
                }
                if (wanEventPublisher != null) {
                    if (backupRecord == null) {
                        wanEventPublisher.publishWanReplicationRemove(name, dataKey);
                    } else {
                        Data dataValue = serializationService.toData(backupRecord.getValue());
                        CacheEntryView<Data, Data> entryView = createDefaultEntryView(dataKey, dataValue, backupRecord);
                        wanEventPublisher.publishWanReplicationUpdate(name, entryView);
                    }
                }
            }
        }
    }

    @Override
    public Object getResponse() {
        return !backupRecords.isEmpty();
    }

    @Override
    public boolean shouldBackup() {
        return backupRecords != null && !backupRecords.isEmpty();
    }

    @Override
    public Operation getBackupOperation() {
        return new CacheMergeBackupOperation(name, backupRecords);
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
