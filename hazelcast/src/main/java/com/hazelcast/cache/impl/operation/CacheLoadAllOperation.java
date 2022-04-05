/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cache.impl.CacheClearResponse;
import com.hazelcast.cache.impl.CacheDataSerializerHook;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;
import com.hazelcast.internal.services.ServiceNamespaceAware;
import com.hazelcast.spi.impl.operationservice.AbstractNamedOperation;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;
import com.hazelcast.internal.partition.IPartitionService;

import javax.cache.CacheException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.internal.util.MapUtil.createHashMap;
import static com.hazelcast.internal.util.SetUtil.createHashSet;

/**
 * Loads all entries of the keys to partition record store {@link com.hazelcast.cache.impl.ICacheRecordStore}.
 * <p>{@link com.hazelcast.cache.impl.operation.CacheLoadAllOperationFactory} creates this operation.</p>
 * <p>Functionality: Filters out the partition keys and calls
 * {@link com.hazelcast.cache.impl.ICacheRecordStore#loadAll(java.util.Set keys, boolean replaceExistingValues)}.</p>
 */
public class CacheLoadAllOperation
        extends AbstractNamedOperation
        implements PartitionAwareOperation, IdentifiedDataSerializable, BackupAwareOperation,
        MutatingOperation, ServiceNamespaceAware {

    private Set<Data> keys;
    private boolean replaceExistingValues;
    private boolean shouldBackup;

    private transient Map<Data, CacheRecord> backupRecords;
    private transient ICacheRecordStore cache;

    private Object response;

    public CacheLoadAllOperation(String name, Set<Data> keys, boolean replaceExistingValues) {
        super(name);
        this.keys = keys;
        this.replaceExistingValues = replaceExistingValues;
    }

    public CacheLoadAllOperation() {
    }

    @Override
    public void run()
            throws Exception {
        int partitionId = getPartitionId();
        IPartitionService partitionService = getNodeEngine().getPartitionService();

        Set<Data> filteredKeys = null;
        if (keys != null) {
            filteredKeys = new HashSet<Data>();
            for (Data k : keys) {
                if (partitionService.getPartitionId(k) == partitionId) {
                    filteredKeys.add(k);
                }
            }
        }

        if (filteredKeys == null || filteredKeys.isEmpty()) {
            return;
        }

        try {
            ICacheService service = getService();
            cache = service.getOrCreateRecordStore(name, partitionId);
            Set<Data> keysLoaded = cache.loadAll(filteredKeys, replaceExistingValues);
            int loadedKeyCount = keysLoaded.size();
            if (loadedKeyCount > 0) {
                backupRecords = createHashMap(loadedKeyCount);
                for (Data key : keysLoaded) {
                    CacheRecord record = cache.getRecord(key);
                    // Loaded keys may have been evicted, then record will be null.
                    // So if the loaded key is evicted, don't send it to backup.
                    if (record != null) {
                        backupRecords.put(key, record);
                    }
                }
                shouldBackup = !backupRecords.isEmpty();
            }
        } catch (CacheException e) {
            response = new CacheClearResponse(e);
        }
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    public boolean shouldBackup() {
        return shouldBackup;
    }

    @Override
    public Operation getBackupOperation() {
        return new CachePutAllBackupOperation(name, backupRecords);
    }

    @Override
    public int getClassId() {
        return CacheDataSerializerHook.LOAD_ALL;
    }

    @Override
    public int getFactoryId() {
        return CacheDataSerializerHook.F_ID;
    }

    @Override
    public final int getSyncBackupCount() {
        return cache.getConfig().getBackupCount();
    }

    @Override
    public final int getAsyncBackupCount() {
        return cache.getConfig().getAsyncBackupCount();
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
        out.writeBoolean(replaceExistingValues);
        out.writeBoolean(keys != null);
        if (keys != null) {
            out.writeInt(keys.size());
            for (Data key : keys) {
                IOUtil.writeData(out, key);
            }
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        replaceExistingValues = in.readBoolean();
        if (in.readBoolean()) {
            int size = in.readInt();
            keys = createHashSet(size);
            for (int i = 0; i < size; i++) {
                keys.add(IOUtil.readData(in));
            }
        }
    }

    @Override
    public boolean requiresTenantContext() {
        return true;
    }
}
