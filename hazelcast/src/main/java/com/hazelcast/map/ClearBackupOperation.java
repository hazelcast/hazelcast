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
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.impl.AbstractNamedOperation;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class ClearBackupOperation extends AbstractNamedOperation implements BackupOperation, DataSerializable {

    Set<Data> keys;
    MapService mapService;
    RecordStore recordStore;

    public ClearBackupOperation(String name, Set<Data> keys) {
        super(name);
        this.keys = keys;
    }

    public ClearBackupOperation() {
    }

    @Override
    public void beforeRun() throws Exception {
        mapService = getService();
        recordStore = mapService.getRecordStore(getPartitionId(), name);
    }

    public void run() {
        if (keys == null) {
            recordStore.removeAll();
            return;
        }
        for (Data key : keys) {
            if (!recordStore.isLocked(key))
                recordStore.evict(key);
        }
    }

    public Set<Data> getKeys() {
        return keys;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        if (keys == null)
            out.writeInt(-1);
        else {
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
        if(size > -1) {
            keys = new HashSet<Data>(size);
            for (int i = 0; i < size; i++) {
                Data data = new Data();
                data.readData(in);
                keys.add(data);
            }
        }
    }


    @Override
    public String toString() {
        return "ClearBackupOperation{" +
                '}';
    }

}
