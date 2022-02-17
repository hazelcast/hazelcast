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
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.internal.services.ServiceNamespaceAware;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;
import com.hazelcast.internal.partition.IPartitionService;

import javax.cache.CacheException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.cache.impl.CacheEventContextUtil.createCacheCompleteEvent;
import static com.hazelcast.internal.util.SetUtil.createHashSet;

/**
 * TODO add a proper JavaDoc
 */
public class CacheRemoveAllOperation
        extends PartitionWideCacheOperation
        implements BackupAwareOperation, MutatingOperation, ServiceNamespaceAware {

    private Set<Data> keys;
    private int completionId;

    private transient Set<Data> filteredKeys = new HashSet<Data>();
    private transient ICacheService service;
    private transient ICacheRecordStore cache;

    public CacheRemoveAllOperation() {
    }

    public CacheRemoveAllOperation(String name, Set<Data> keys, int completionId) {
        super(name);
        this.keys = keys;
        this.completionId = completionId;
    }

    @Override
    public void beforeRun() throws Exception {
        service = getService();
        cache = service.getRecordStore(name, getPartitionId());
    }

    @Override
    public void run() throws Exception {
        if (cache == null) {
            service.publishEvent(createCacheCompleteEvent(completionId).setCacheName(name));
            return;
        }
        filterKeys();
        try {
            if (keys == null) {
                // Here the filteredKeys is empty, this means we will remove all data
                // filteredKeys will get filled with removed keys
                cache.removeAll(filteredKeys, completionId);
            } else if (!filteredKeys.isEmpty()) {
                cache.removeAll(filteredKeys, completionId);
            } else {
                service.publishEvent(createCacheCompleteEvent(completionId).setCacheName(name));
            }
            response = new CacheClearResponse(Boolean.TRUE);
        } catch (CacheException e) {
            response = new CacheClearResponse(e);
        }
    }

    private void filterKeys() {
        if (keys == null) {
            return;
        }
        IPartitionService partitionService = getNodeEngine().getPartitionService();
        for (Data k : keys) {
            if (partitionService.getPartitionId(k) == getPartitionId()) {
                filteredKeys.add(k);
            }
        }
    }

    @Override
    public boolean shouldBackup() {
        return !filteredKeys.isEmpty();
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
    public Operation getBackupOperation() {
        return new CacheRemoveAllBackupOperation(name, filteredKeys);
    }

    @Override
    public ObjectNamespace getServiceNamespace() {
        return cache != null ? cache.getObjectNamespace() : CacheService.getObjectNamespace(name);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(completionId);
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
        completionId = in.readInt();
        boolean isKeysNotNull = in.readBoolean();
        if (isKeysNotNull) {
            int size = in.readInt();
            keys = createHashSet(size);
            for (int i = 0; i < size; i++) {
                Data key = IOUtil.readData(in);
                keys.add(key);
            }
        }
    }

    @Override
    public int getClassId() {
        return CacheDataSerializerHook.REMOVE_ALL;
    }

}
