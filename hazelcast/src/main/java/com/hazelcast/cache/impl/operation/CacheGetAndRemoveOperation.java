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

import com.hazelcast.cache.impl.CacheDataSerializerHook;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

/**
 * Cache GetAndRemove Operation.
 * <p>Operation to call the record store's functionality. Backup is also triggered by this operation
 * if a record is removed.</p>
 * @see com.hazelcast.cache.impl.ICacheRecordStore#getAndRemove(com.hazelcast.nio.serialization.Data, String)
 */
public class CacheGetAndRemoveOperation
        extends AbstractMutatingCacheOperation {

    public CacheGetAndRemoveOperation() {
    }

    public CacheGetAndRemoveOperation(String name, Data key) {
        this(name, key, IGNORE_COMPLETION);
    }

    public CacheGetAndRemoveOperation(String name, Data key, int completionId) {
        super(name, key, completionId);
    }

    @Override
    public void run()
            throws Exception {
        response = cache.getAndRemove(key, getCallerUuid());
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
    public int getId() {
        return CacheDataSerializerHook.GET_AND_REMOVE;
    }
}
