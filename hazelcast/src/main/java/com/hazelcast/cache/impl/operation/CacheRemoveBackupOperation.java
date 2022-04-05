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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.BackupOperation;

import java.io.IOException;

/**
 * Backup operation used by remove operations.
 */
public class CacheRemoveBackupOperation
        extends KeyBasedCacheOperation implements BackupOperation {

    private boolean wanOriginated;

    public CacheRemoveBackupOperation() {
    }

    public CacheRemoveBackupOperation(String name, Data key) {
        this(name, key, false);
    }

    public CacheRemoveBackupOperation(String name, Data key, boolean wanOriginated) {
        super(name, key, true);
        this.wanOriginated = wanOriginated;
    }

    @Override
    public void run() {
        if (recordStore != null) {
            recordStore.removeRecord(key);
        }
    }

    @Override
    public void afterRun() {
        if (recordStore != null && !wanOriginated) {
            publishWanRemove(key);
        }
    }

    @Override
    public int getClassId() {
        return CacheDataSerializerHook.REMOVE_BACKUP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeBoolean(wanOriginated);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        wanOriginated = in.readBoolean();
    }
}
