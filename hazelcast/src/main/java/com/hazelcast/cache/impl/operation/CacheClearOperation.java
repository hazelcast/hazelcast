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

import com.hazelcast.cache.impl.CacheClearResponse;
import com.hazelcast.cache.impl.CacheDataSerializerHook;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.ServiceNamespaceAware;
import com.hazelcast.spi.impl.MutatingOperation;
import com.hazelcast.spi.partition.IPartitionService;

import javax.cache.CacheException;

import static com.hazelcast.cache.impl.AbstractCacheRecordStore.SOURCE_NOT_AVAILABLE;

/**
 * Cache Clear will clear all internal cache data without sending any event
 */
public class CacheClearOperation
        extends PartitionWideCacheOperation
        implements BackupAwareOperation, ServiceNamespaceAware, MutatingOperation {

    private transient ICacheRecordStore cache;

    public CacheClearOperation() {
    }

    public CacheClearOperation(String name) {
        super(name);
    }

    @Override
    public void beforeRun() throws Exception {
        ICacheService service = getService();
        cache = service.getRecordStore(name, getPartitionId());
    }

    @Override
    public void run() {
        if (cache == null) {
            return;
        }
        try {
            cache.clear();
            response = new CacheClearResponse(Boolean.TRUE);
        } catch (CacheException e) {
            response = new CacheClearResponse(e);
        }
    }

    @Override
    public void afterRun() throws Exception {
        super.afterRun();

        IPartitionService partitionService = getNodeEngine().getPartitionService();
        if (partitionService.getPartitionId(name) == getPartitionId()) {
            CacheService cacheService = getService();
            cacheService.sendInvalidationEvent(name, null, SOURCE_NOT_AVAILABLE);
        }

    }

    @Override
    public int getId() {
        return CacheDataSerializerHook.CLEAR;
    }

    @Override
    public boolean shouldBackup() {
        return true;
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
        return new CacheClearBackupOperation(name);
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
}
