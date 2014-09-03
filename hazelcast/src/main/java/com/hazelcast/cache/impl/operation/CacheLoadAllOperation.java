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
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.impl.AbstractNamedOperation;

import javax.cache.CacheException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Triggers cache store load of all given keys.
 */
public class CacheLoadAllOperation
        extends AbstractNamedOperation
        implements PartitionAwareOperation, IdentifiedDataSerializable, BackupAwareOperation {

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
        final int partitionId = getPartitionId();
        final InternalPartitionService partitionService = getNodeEngine().getPartitionService();

        Set<Data> filteredKeys = new HashSet<Data>();
        if (keys != null) {
            for (Data k : keys) {
                if (partitionService.getPartitionId(k) == partitionId) {
                    filteredKeys.add(k);
                }
            }
        }
        if (filteredKeys.isEmpty()) {
            return;
        }
        try {
            final CacheService service = getService();
            cache = service.getOrCreateCache(name, partitionId);
            final Set<Data> keysLoaded = cache.loadAll(filteredKeys, replaceExistingValues);
            shouldBackup = !keysLoaded.isEmpty();
            if (shouldBackup) {
                backupRecords = new HashMap<Data, CacheRecord>();
                for (Data key : keysLoaded) {
                    backupRecords.put(key, cache.getRecord(key));
                }
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
    public int getId() {
        return CacheDataSerializerHook.LOAD_ALL;
    }

    @Override
    public int getFactoryId() {
        return CacheDataSerializerHook.F_ID;
    }

    public final int getSyncBackupCount() {
        return cache != null ? cache.getConfig().getBackupCount() : 0;
    }

    public final int getAsyncBackupCount() {
        return cache != null ? cache.getConfig().getAsyncBackupCount() : 0;
    }
}
