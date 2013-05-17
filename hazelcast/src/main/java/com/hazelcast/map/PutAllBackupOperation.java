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

import com.hazelcast.core.EntryEvent;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class PutAllBackupOperation extends AbstractMapOperation implements PartitionAwareOperation, BackupOperation {

    MapEntrySet entrySet;

    public PutAllBackupOperation(String name, MapEntrySet entrySet) {
        super(name);
        this.entrySet = entrySet;
    }

    public PutAllBackupOperation() {
    }

    public void run() {
        int partitionId = getPartitionId();
        RecordStore recordStore = mapService.getRecordStore(partitionId, name);
        Set<Map.Entry<Data, Data>> entries = entrySet.getEntrySet();
        for (Map.Entry<Data, Data> entry : entries) {
            recordStore.set(entry.getKey(), entry.getValue(), -1);
        }
    }

    @Override
    public Object getResponse() {
        return entrySet;
    }

    @Override
    public String toString() {
        return "PutAllBackupOperation{" +
                '}';
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(entrySet);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        entrySet = in.readObject();
    }

}
