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

import com.hazelcast.cache.CacheNotExistsException;
import com.hazelcast.cache.impl.CacheDataSerializerHook;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.BackupOperation;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.internal.services.ServiceNamespaceAware;
import com.hazelcast.spi.impl.operationservice.AbstractNamedOperation;

/**
 * Backup operation of {@link com.hazelcast.cache.impl.operation.CacheClearOperation}.
 * <p>It simply clears the records.</p>
 */
public class CacheClearBackupOperation extends AbstractNamedOperation
        implements BackupOperation, ServiceNamespaceAware, IdentifiedDataSerializable {

    private transient ICacheRecordStore cache;

    public CacheClearBackupOperation() {
    }

    public CacheClearBackupOperation(String name) {
        super(name);
    }

    @Override
    public void beforeRun()
            throws Exception {
        ICacheService service = getService();
        try {
            cache = service.getOrCreateRecordStore(name, getPartitionId());
        } catch (CacheNotExistsException e) {
            getLogger().finest("Error while getting a cache", e);
        }

    }

    @Override
    public String getServiceName() {
        return ICacheService.SERVICE_NAME;
    }

    @Override
    public void run() throws Exception {
        if (cache != null) {
            cache.clear();
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
    public int getFactoryId() {
        return CacheDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return CacheDataSerializerHook.CLEAR_BACKUP;
    }

}
