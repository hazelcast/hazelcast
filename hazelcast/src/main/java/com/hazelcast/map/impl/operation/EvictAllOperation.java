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

package com.hazelcast.map.impl.operation;

import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;

import java.io.IOException;

import static com.hazelcast.core.EntryEventType.EVICT_ALL;

/**
 * Operation which evicts all keys except locked ones.
 */
public class EvictAllOperation extends MapOperation
        implements BackupAwareOperation, MutatingOperation, PartitionAwareOperation {

    private boolean shouldRunOnBackup;
    private int numberOfEvictedEntries;

    public EvictAllOperation() {
        this(null);
    }

    public EvictAllOperation(String name) {
        super(name);
        createRecordStoreOnDemand = false;
    }

    @Override
    protected void runInternal() {
        if (recordStore == null) {
            return;
        }
        numberOfEvictedEntries = recordStore.evictAll(false);
        shouldRunOnBackup = true;
    }

    @Override
    protected void afterRunInternal() {
        hintMapEvent();
        invalidateAllKeysInNearCaches();
    }

    private void hintMapEvent() {
        mapEventPublisher.hintMapEvent(getCallerAddress(), name,
                EVICT_ALL, numberOfEvictedEntries, getPartitionId());
    }

    @Override
    public boolean shouldBackup() {
        return shouldRunOnBackup;
    }

    @Override
    public Object getResponse() {
        return numberOfEvictedEntries;
    }

    @Override
    public int getSyncBackupCount() {
        return mapServiceContext.getMapContainer(name).getBackupCount();
    }

    @Override
    public int getAsyncBackupCount() {
        return mapServiceContext.getMapContainer(name).getAsyncBackupCount();
    }

    @Override
    public Operation getBackupOperation() {
        return new EvictAllBackupOperation(name);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(numberOfEvictedEntries);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        numberOfEvictedEntries = in.readInt();
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);

        sb.append(", shouldRunOnBackup=").append(shouldRunOnBackup);
        sb.append(", numberOfEvictedEntries=").append(numberOfEvictedEntries);
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.EVICT_ALL;
    }
}
