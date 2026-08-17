/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.internal.impl.ops;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.vector.VectorValues;

/**
 * Common logic for all 1-entry update operations executed on the owner
 */
public abstract class BasePutOwnerOperation extends BasePutOperation
        implements BackupAwareOperation, MutatingOperation {

    protected BasePutOwnerOperation() {
    }

    protected BasePutOwnerOperation(String vectorCollectionName, Data key, Data userValue, VectorValues vectorValues) {
        super(vectorCollectionName, key, userValue, vectorValues);
    }


    @Override
    public boolean shouldBackup() {
        return storage.getConfig().getTotalBackupCount() > 0;
    }

    @Override
    public int getSyncBackupCount() {
        return storage.getConfig().getBackupCount();
    }

    @Override
    public int getAsyncBackupCount() {
        return storage.getConfig().getAsyncBackupCount();
    }

    @Override
    public Operation getBackupOperation() {
        return new SetBackupOperation(getName(), key, userValue, vectorValues);
    }
}
