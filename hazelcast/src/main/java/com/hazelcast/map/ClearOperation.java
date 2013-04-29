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

package com.hazelcast.map;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.map.MapService.SERVICE_NAME;

public class ClearOperation extends AbstractMapOperation implements BackupAwareOperation, PartitionAwareOperation {

    Set<Data> keys;
    boolean shouldBackup = true;

    public ClearOperation(String name) {
        super(name);
    }

    public ClearOperation(String name, Set<Data> keys) {
        super(name);
        this.keys = keys;
    }

    public ClearOperation() {
    }

    public void run() {
        RecordStore recordStore = mapService.getRecordStore(getPartitionId(), name);

        if (keys == null) {
            recordStore.removeAll();
            return;
        }
        if(keys.isEmpty()) {
            shouldBackup = false;
        }
        for (Data key : keys) {
            if (!recordStore.isLocked(key)) {
                recordStore.evict(key);
            }
        }
    }

    public boolean shouldBackup() {
        return shouldBackup;
    }

    public int getSyncBackupCount() {
          return mapService.getMapContainer(name).getBackupCount();
    }

    public int getAsyncBackupCount() {
        return mapService.getMapContainer(name).getAsyncBackupCount();
    }

    @Override
    public boolean returnsResponse() {
        // keys is not null when this operation is used in eviction, and no need a response while eviction
        return keys == null;
    }

    public Operation getBackupOperation() {
        ClearBackupOperation clearBackupOperation = new ClearBackupOperation(name, keys);
        clearBackupOperation.setServiceName(SERVICE_NAME);
        return clearBackupOperation;
    }

    @Override
    public String toString() {
        return "ClearOperation{" +
                '}';
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        if (keys == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(keys.size());
            for (Data key : keys) {
                key.writeData(out);
            }
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        if (size > -1) {
            keys = new HashSet<Data>(size);
            for (int i = 0; i < size; i++) {
                Data data = new Data();
                data.readData(in);
                keys.add(data);
            }
        }
    }
}
