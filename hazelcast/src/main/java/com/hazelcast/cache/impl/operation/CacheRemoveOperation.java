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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

import java.io.IOException;

/**
 * Operation implementation for cache remove functionality.
 * @see com.hazelcast.cache.impl.ICacheRecordStore#remove(Data, Object, String)
 * @see com.hazelcast.cache.impl.ICacheRecordStore#remove(Data, String)
 */
public class CacheRemoveOperation
        extends AbstractMutatingCacheOperation {

    // if same
    private Data oldValue;

    public CacheRemoveOperation() {
    }

    public CacheRemoveOperation(String name, Data key, Data oldValue) {
        this(name, key, oldValue, IGNORE_COMPLETION);
    }

    public CacheRemoveOperation(String name, Data key, Data oldValue, int completionId) {
        super(name, key, completionId);
        this.oldValue = oldValue;
    }

    @Override
    public void run()
            throws Exception {
        if (oldValue == null) {
            response = cache.remove(key, getCallerUuid());
        } else {
            response = cache.remove(key, oldValue, getCallerUuid());
        }
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
    public int getId() {
        return CacheDataSerializerHook.REMOVE;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeData(oldValue);
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        oldValue = in.readData();
    }
}
