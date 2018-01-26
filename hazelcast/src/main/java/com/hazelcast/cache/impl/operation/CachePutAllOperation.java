/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.cache.impl.CacheEntryViews;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.cache.impl.event.CacheWanEventPublisher;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.ServiceNamespaceAware;
import com.hazelcast.spi.impl.AbstractNamedOperation;
import com.hazelcast.spi.impl.MutatingOperation;

import javax.cache.expiry.ExpiryPolicy;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.hazelcast.util.MapUtil.createHashMap;

public class CachePutAllOperation
        extends AbstractNamedOperation
        implements PartitionAwareOperation, IdentifiedDataSerializable, BackupAwareOperation, ServiceNamespaceAware,
                   MutableOperation, MutatingOperation {

    private List<Map.Entry<Data, Data>> entries;
    private ExpiryPolicy expiryPolicy;
    private int completionId;

    private transient ICacheRecordStore cache;
    private transient Map<Data, CacheRecord> backupRecords;

    public CachePutAllOperation() {
    }

    public CachePutAllOperation(String cacheNameWithPrefix, List<Map.Entry<Data, Data>> entries,
                                ExpiryPolicy expiryPolicy, int completionId) {
        super(cacheNameWithPrefix);
        this.entries = entries;
        this.expiryPolicy = expiryPolicy;
        this.completionId = completionId;
    }

    @Override
    public int getCompletionId() {
        return completionId;
    }

    @Override
    public void setCompletionId(int completionId) {
        this.completionId = completionId;
    }

    @Override
    public void run()
            throws Exception {
        int partitionId = getPartitionId();
        String callerUuid = getCallerUuid();
        ICacheService service = getService();
        cache = service.getOrCreateRecordStore(name, partitionId);
        backupRecords = createHashMap(entries.size());
        for (Map.Entry<Data, Data> entry : entries) {
            Data key = entry.getKey();
            Data value = entry.getValue();
            CacheRecord backupRecord = cache.put(key, value, expiryPolicy, callerUuid, completionId);
            // backupRecord may be null (eg expired on put)
            if (backupRecord != null) {
                backupRecords.put(key, backupRecord);
            }

            publishWanEvent(key, value, backupRecord);
        }
    }

    private void publishWanEvent(Data key, Data value, CacheRecord backupRecord) {
        if (cache.isWanReplicationEnabled()) {
            ICacheService service = getService();
            CacheWanEventPublisher publisher = service.getCacheWanEventPublisher();
            publisher.publishWanReplicationUpdate(name, CacheEntryViews.createDefaultEntryView(key, value, backupRecord));
        }
    }

    @Override
    public boolean shouldBackup() {
        return !backupRecords.isEmpty();
    }

    @Override
    public Operation getBackupOperation() {
        return new CachePutAllBackupOperation(name, backupRecords);
    }

    @Override
    public int getId() {
        return CacheDataSerializerHook.PUT_ALL;
    }

    @Override
    public int getFactoryId() {
        return CacheDataSerializerHook.F_ID;
    }

    @Override
    public final int getSyncBackupCount() {
        return cache != null ? cache.getConfig().getBackupCount() : 0;
    }

    @Override
    public final int getAsyncBackupCount() {
        return cache != null ? cache.getConfig().getAsyncBackupCount() : 0;
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
        out.writeObject(expiryPolicy);
        out.writeInt(completionId);
        out.writeInt(entries.size());
        for (Map.Entry<Data, Data> entry : entries) {
            out.writeData(entry.getKey());
            out.writeData(entry.getValue());
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        expiryPolicy = in.readObject();
        completionId = in.readInt();
        int size = in.readInt();
        entries = new ArrayList<Map.Entry<Data, Data>>(size);
        for (int i = 0; i < size; i++) {
            Data key = in.readData();
            Data value = in.readData();
            entries.add(new AbstractMap.SimpleImmutableEntry<Data, Data>(key, value));
        }
    }

}
