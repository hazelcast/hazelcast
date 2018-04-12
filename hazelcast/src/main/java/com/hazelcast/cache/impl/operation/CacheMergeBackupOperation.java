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
import com.hazelcast.cache.CacheNotExistsException;
import com.hazelcast.cache.impl.CacheDataSerializerHook;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.cache.impl.event.CacheWanEventPublisher;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.ServiceNamespaceAware;
import com.hazelcast.spi.impl.AbstractNamedOperation;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.serialization.SerializationService;

import java.io.IOException;
import java.util.Map;

import static com.hazelcast.cache.impl.CacheEntryViews.createDefaultEntryView;
import static com.hazelcast.util.MapUtil.createHashMap;

/**
 * Creates backups for merged {@link CacheRecord} after split-brain healing with a {@link SplitBrainMergePolicy}.
 *
 * @since 3.10
 */
public class CacheMergeBackupOperation extends AbstractNamedOperation implements BackupOperation, ServiceNamespaceAware {

    private Map<Data, CacheRecord> cacheRecords;

    private transient ICacheRecordStore cache;

    public CacheMergeBackupOperation() {
    }

    public CacheMergeBackupOperation(String cacheNameWithPrefix, Map<Data, CacheRecord> cacheRecords) {
        super(cacheNameWithPrefix);
        this.cacheRecords = cacheRecords;
    }

    @Override
    public void beforeRun() throws Exception {
        ICacheService service = getService();
        try {
            cache = service.getOrCreateRecordStore(name, getPartitionId());
        } catch (CacheNotExistsException e) {
            getLogger().finest("Error while getting a cache", e);
        }
    }

    @Override
    public void run() throws Exception {
        if (cache == null) {
            return;
        }

        SerializationService serializationService = getNodeEngine().getSerializationService();

        CacheWanEventPublisher publisher = null;
        if (cache.isWanReplicationEnabled()) {
            ICacheService service = getService();
            publisher = service.getCacheWanEventPublisher();
        }

        for (Map.Entry<Data, CacheRecord> entry : cacheRecords.entrySet()) {
            Data key = entry.getKey();
            CacheRecord record = entry.getValue();
            if (record == null) {
                cache.removeRecord(key);
                if (publisher != null) {
                    publisher.publishWanReplicationRemoveBackup(name, key);
                }
            } else {
                cache.putRecord(key, record, true);
                if (publisher != null) {
                    Data dataValue = serializationService.toData(record.getValue());
                    CacheEntryView<Data, Data> view = createDefaultEntryView(key, dataValue, record);
                    publisher.publishWanReplicationUpdateBackup(name, view);
                }
            }
        }
    }

    @Override
    public ObjectNamespace getServiceNamespace() {
        ICacheRecordStore recordStore = cache;
        if (recordStore == null) {
            ICacheService service = getService();
            recordStore = service.getOrCreateRecordStore(name, getPartitionId());
        }
        return recordStore.getObjectNamespace();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(cacheRecords.size());
        for (Map.Entry<Data, CacheRecord> entry : cacheRecords.entrySet()) {
            out.writeData(entry.getKey());
            out.writeObject(entry.getValue());
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        cacheRecords = createHashMap(size);
        for (int i = 0; i < size; i++) {
            Data key = in.readData();
            CacheRecord record = in.readObject();
            cacheRecords.put(key, record);
        }
    }

    @Override
    public int getId() {
        return CacheDataSerializerHook.MERGE_BACKUP;
    }

    @Override
    public int getFactoryId() {
        return CacheDataSerializerHook.F_ID;
    }
}
