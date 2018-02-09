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
import com.hazelcast.spi.SplitBrainMergePolicy;
import com.hazelcast.spi.impl.AbstractNamedOperation;
import com.hazelcast.spi.merge.KeyMergeDataHolder;
import com.hazelcast.spi.serialization.SerializationService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.hazelcast.cache.impl.CacheEntryViews.createDefaultEntryView;
import static com.hazelcast.util.MapUtil.createHashMap;

/**
 * Contains multiple merging entries for split-brain healing with a {@link SplitBrainMergePolicy}.
 *
 * @since 3.10
 */
public class CacheMergeOperation extends AbstractNamedOperation implements BackupAwareOperation, ServiceNamespaceAware {

    private List<KeyMergeDataHolder<Data, Data>> mergeData;
    private SplitBrainMergePolicy mergePolicy;

    private transient SerializationService serializationService;
    private transient ICacheRecordStore cache;
    private transient CacheWanEventPublisher wanEventPublisher;

    private transient boolean hasBackups;
    private transient Map<Data, CacheRecord> backupRecords;

    public CacheMergeOperation() {
    }

    CacheMergeOperation(String name, List<KeyMergeDataHolder<Data, Data>> mergeData, SplitBrainMergePolicy policy) {
        super(name);
        this.mergeData = mergeData;
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
            backupRecords = createHashMap(mergeData.size());
        }
    }

    @Override
    public void run() {
        for (KeyMergeDataHolder<Data, Data> mergeDataHolder : mergeData) {
            merge(mergeDataHolder);
        }
    }

    private void merge(KeyMergeDataHolder<Data, Data> mergeDataHolder) {
        Data dataKey = mergeDataHolder.getKey();

        CacheRecord backupRecord = cache.merge(mergeDataHolder, mergePolicy);
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
        out.writeInt(mergeData.size());
        for (KeyMergeDataHolder<Data, Data> mergeEntry : mergeData) {
            out.writeObject(mergeEntry);
        }
        out.writeObject(mergePolicy);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        mergeData = new ArrayList<KeyMergeDataHolder<Data, Data>>(size);
        for (int i = 0; i < size; i++) {
            KeyMergeDataHolder<Data, Data> mergeEntry = in.readObject();
            mergeData.add(mergeEntry);
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
