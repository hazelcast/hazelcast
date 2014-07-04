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

import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.MapEntrySimple;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupOperation;

import java.io.IOException;
import java.util.Map;

public class EntryBackupOperation extends KeyBasedMapOperation implements BackupOperation {

    private EntryBackupProcessor entryProcessor;

    public EntryBackupOperation(String name, Data dataKey, EntryBackupProcessor entryProcessor) {
        super(name, dataKey);
        this.entryProcessor = entryProcessor;
    }

    public EntryBackupOperation() {
    }

    public void innerBeforeRun() {
        if (entryProcessor instanceof HazelcastInstanceAware) {
            ((HazelcastInstanceAware) entryProcessor).setHazelcastInstance(getNodeEngine().getHazelcastInstance());
        }
    }

    public void run() {
        Map.Entry<Data, Object> mapEntry = recordStore.getMapEntryForBackup(dataKey);
        Object objectKey = mapService.getMapServiceContext().toObject(dataKey);
        final Object valueBeforeProcess = mapService.getMapServiceContext().toObject(mapEntry.getValue());
        MapEntrySimple<Object, Object> entry = new MapEntrySimple<Object, Object>(objectKey, valueBeforeProcess);
        entryProcessor.processBackup(entry);
        if (!entry.isModified()) {
            return;
        }
        if (entry.getValue() == null) {
            recordStore.removeBackup(dataKey);
        } else {
            recordStore.putBackup(dataKey, entry.getValue());
        }

    }


    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        entryProcessor = in.readObject();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(entryProcessor);
    }

    @Override
    public Object getResponse() {
        return true;
    }

    @Override
    public String toString() {
        return "EntryBackupOperation{}";
    }

}
