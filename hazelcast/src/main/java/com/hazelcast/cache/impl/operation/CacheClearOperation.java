/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.HeapData;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;

import javax.cache.CacheException;
import java.util.HashSet;
import java.util.Set;

/**
 * Cache Clear is a "remove all" operation with or without a set of keys provided.
 * <p><code>isRemoveAll</code> will delete all internal cache data without doing anything but deletion.</p>
 */
public class CacheClearOperation
        extends PartitionWideCacheOperation
        implements BackupAwareOperation {

    private boolean isClear;
    private Set<Data> keys;
    private int completionId;

    private transient Set<Data> filteredKeys = new HashSet<Data>();

    private transient ICacheRecordStore cache;

    private transient boolean shouldBackup;

    public CacheClearOperation() {
    }

    public CacheClearOperation(String name, Set<Data> keys, boolean isClear, int completionId) {
        super(name);
        this.keys = keys;
        this.isClear = isClear;
        this.completionId = completionId;
    }

    @Override
    public void run() {
        CacheService service = getService();

        cache = service.getCacheRecordStore(name, getPartitionId());
        if (cache == null) {
            return;
        }
        filterKeys();
        try {
            shouldBackup = true;
            if (isClear) {
                cache.clear();
            } else if (keys == null) {
                cache.removeAll(new HashSet<Data>());
            } else if (!filteredKeys.isEmpty()) {
                cache.removeAll(filteredKeys);
            } else {
                shouldBackup = false;
            }
            response = new CacheClearResponse(Boolean.TRUE);
            int orderKey = keys != null ? keys.hashCode() : 1;
            cache.publishCompletedEvent(name, completionId, new HeapData(), orderKey);
        } catch (CacheException e) {
            response = new CacheClearResponse(e);
        }

    }

    private void filterKeys() {
        if (keys == null) {
            return;
        }
        InternalPartitionService partitionService = getNodeEngine().getPartitionService();
        for (Data k : keys) {
            if (partitionService.getPartitionId(k) == getPartitionId()) {
                filteredKeys.add(k);
            }
        }
    }

    @Override
    public int getId() {
        return CacheDataSerializerHook.CLEAR;
    }

    @Override
    public boolean shouldBackup() {
        return shouldBackup;
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
        return new CacheClearBackupOperation(name, filteredKeys, isClear || keys == null);
    }

}
