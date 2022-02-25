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

import com.hazelcast.cache.impl.CacheDataSerializerHook;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.Operation;

/**
 * Cache GetAndRemove Operation.
 * <p>Operation to call the record store's functionality. Backup is also triggered by this operation
 * if a record is removed.</p>
 *
 * @see com.hazelcast.cache.impl.ICacheRecordStore#getAndRemove(Data, String, int)
 */
public class CacheGetAndRemoveOperation extends MutatingCacheOperation {

    public CacheGetAndRemoveOperation() {
    }

    public CacheGetAndRemoveOperation(String name, Data key, int completionId) {
        super(name, key, completionId);
    }

    @Override
    public void run()
            throws Exception {
        response = recordStore.getAndRemove(key, getCallerUuid(), completionId);
    }

    @Override
    public void afterRun() throws Exception {
        if (response != null) {
            publishWanRemove(key);
        }
        super.afterRun();
    }

    @Override
    public boolean shouldBackup() {
        return response != null;
    }

    @Override
    public Operation getBackupOperation() {
        return new CacheRemoveBackupOperation(name, key);
    }

    @Override
    public int getClassId() {
        return CacheDataSerializerHook.GET_AND_REMOVE;
    }
}
