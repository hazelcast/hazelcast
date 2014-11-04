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

package com.hazelcast.map.operation;

import com.hazelcast.map.MapDataSerializerHook;
import com.hazelcast.map.MapService;
import com.hazelcast.map.MapServiceContext;
import com.hazelcast.map.RecordStore;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.BackupOperation;

import java.io.IOException;

public final class RemoveBackupOperation extends KeyBasedMapOperation implements BackupOperation, IdentifiedDataSerializable {

    private boolean unlockKey;

    public RemoveBackupOperation(String name, Data dataKey) {
        super(name, dataKey);
    }

    public RemoveBackupOperation(String name, Data dataKey, boolean unlockKey) {
        super(name, dataKey);
        this.unlockKey = unlockKey;
    }

    public RemoveBackupOperation() {
    }

    public void run() {
        MapService mapService = getService();
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        int partitionId = getPartitionId();
        RecordStore recordStore = mapServiceContext.getRecordStore(partitionId, name);
        recordStore.removeBackup(dataKey);
        if (unlockKey) {
            recordStore.forceUnlock(dataKey);
        }
    }

    @Override
    public void afterRun() throws Exception {
        evict(true);
    }

    @Override
    public Object getResponse() {
        return Boolean.TRUE;
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeBoolean(unlockKey);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        unlockKey = in.readBoolean();
    }

    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    public int getId() {
        return MapDataSerializerHook.REMOVE_BACKUP;
    }

}
