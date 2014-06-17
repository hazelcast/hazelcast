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

package com.hazelcast.cache.operation;

import com.hazelcast.cache.CacheClearResponse;
import com.hazelcast.cache.CacheDataSerializerHook;
import com.hazelcast.cache.CacheService;
import com.hazelcast.cache.ICacheRecordStore;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.NormalResponse;

import javax.cache.CacheException;
import java.util.HashSet;
import java.util.Set;

/**
 * @author mdogan 06/02/14
 */
public class CacheClearOperation extends PartitionWideCacheOperation implements BackupAwareOperation {

    private boolean isRemoveAll;
    private Set<Data> keys;

    public CacheClearOperation() {
    }

    public CacheClearOperation(String name, Set<Data> keys, boolean isRemoveAll) {
        super(name);
        this.keys = keys;
        this.isRemoveAll = isRemoveAll;
    }

    @Override
    public void run() {
        CacheService service = getService();
        final InternalPartitionService partitionService = getNodeEngine().getPartitionService();

        ICacheRecordStore cache = service.getCache(name, getPartitionId());
        if (cache != null) {
            Set<Data> filteredKeys = null;
            if (keys != null) {
                filteredKeys = new HashSet<Data>();
                for (Data k : keys) {
                    if (partitionService.getPartitionId(k) == getPartitionId()) {
                        filteredKeys.add(k);
                    }
                }
            }
            try {
                cache.clear(filteredKeys, isRemoveAll);
            } catch (CacheException e) {
                response = new CacheClearResponse(e);
            }
        }
    }

    @Override
    public int getId() {
        return CacheDataSerializerHook.CLEAR;
    }

    @Override
    public boolean shouldBackup() {
        return false;
    }

    @Override
    public int getSyncBackupCount() {
        return 0;
    }

    @Override
    public int getAsyncBackupCount() {
        return 0;
    }

    @Override
    public Operation getBackupOperation() {
        return null;
    }

}
