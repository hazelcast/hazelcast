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

import com.hazelcast.map.MapKeySet;
import com.hazelcast.map.MapService;
import com.hazelcast.map.RecordStore;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.impl.AbstractNamedOperation;

import java.io.IOException;
import java.util.Set;

/**
 * User: ahmetmircik
 * Date: 11/1/13
 */
public class EvictKeysBackupOperation extends AbstractNamedOperation implements BackupOperation, DataSerializable {
    MapKeySet mapKeySet;
    MapService mapService;
    RecordStore recordStore;

    public EvictKeysBackupOperation() {
    }

    public EvictKeysBackupOperation(String name, Set<Data> keys) {
        super(name);
        this.mapKeySet = new MapKeySet(keys);
    }

    @Override
    public void beforeRun() throws Exception {
        mapService = getService();
        recordStore = mapService.getRecordStore(getPartitionId(), name);
    }

    public void run() {
        final Set<Data> keys = mapKeySet.getKeySet();
        for (Data key : keys) {
            if (!recordStore.isLocked(key))
            {
                recordStore.evict(key);
            }
        }
    }

    public Set<Data> getKeys() {
        return mapKeySet.getKeySet();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        mapKeySet.writeData(out);

    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        mapKeySet = new MapKeySet();
        mapKeySet.readData(in);

    }


    @Override
    public String toString() {
        return "EvictKeysBackupOperation{" +
                '}';
    }

}