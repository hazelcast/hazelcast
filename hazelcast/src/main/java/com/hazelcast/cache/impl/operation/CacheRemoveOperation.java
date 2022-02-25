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
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;

/**
 * Operation implementation for cache remove functionality.
 *
 * @see com.hazelcast.cache.impl.ICacheRecordStore#remove(Data, String, String, int)
 * @see com.hazelcast.cache.impl.ICacheRecordStore#remove(Data, Object, String, String, int)
 */
public class CacheRemoveOperation extends MutatingCacheOperation {

    // if same
    private Data oldValue;

    public CacheRemoveOperation() {
    }

    public CacheRemoveOperation(String cacheNameWithPrefix, Data key, Data oldValue, int completionId) {
        super(cacheNameWithPrefix, key, completionId);
        this.oldValue = oldValue;
    }

    @Override
    public void run()
            throws Exception {
        if (oldValue == null) {
            response = recordStore.remove(key, getCallerUuid(), null, completionId);
        } else {
            response = recordStore.remove(key, oldValue, getCallerUuid(), null, completionId);
        }
    }

    @Override
    public void afterRun() throws Exception {
        if (Boolean.TRUE.equals(response)) {
            publishWanRemove(key);
        }
        super.afterRun();
    }

    @Override
    public boolean shouldBackup() {
        return Boolean.TRUE.equals(response);
    }

    @Override
    public Operation getBackupOperation() {
        return new CacheRemoveBackupOperation(name, key);
    }

    @Override
    public int getClassId() {
        return CacheDataSerializerHook.REMOVE;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        IOUtil.writeData(out, oldValue);
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        oldValue = IOUtil.readData(in);
    }

    @Override
    public boolean requiresTenantContext() {
        return true;
    }
}
