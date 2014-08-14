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

import com.hazelcast.cache.CacheDataSerializerHook;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;

import java.io.IOException;

/**
 * @author mdogan 05/02/14
 */
public class CacheRemoveOperation extends AbstractCacheOperation implements BackupAwareOperation {

    private Data currentValue; // if same

    public CacheRemoveOperation() {
    }

    public CacheRemoveOperation(String name, Data key) {
        super(name, key);
    }

    public CacheRemoveOperation(String name, Data key, Data currentValue) {
        super(name, key);
        this.currentValue = currentValue;
    }

    @Override
    public void run() throws Exception {
        if (cache != null) {
            if (currentValue == null) {
                response = cache.remove(key, getCallerUuid());
            } else {
                response = cache.remove(key, currentValue, getCallerUuid());
            }
        } else {
            response = Boolean.FALSE;
        }
    }

    @Override
    public boolean shouldBackup() {
        return response == Boolean.TRUE;
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
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        IOUtil.writeNullableData(out, currentValue);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        currentValue = IOUtil.readNullableData(in);
    }
}
