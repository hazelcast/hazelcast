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
import com.hazelcast.map.MapServiceContext;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupOperation;

import java.io.IOException;
import java.util.Map;

public class EntryBackupOperation extends KeyBasedMapOperation implements BackupOperation {

    private EntryBackupProcessor entryProcessor;

    public EntryBackupOperation() {
    }

    public EntryBackupOperation(String name, Data dataKey, EntryBackupProcessor entryProcessor) {
        super(name, dataKey);
        this.entryProcessor = entryProcessor;
    }

    @Override
    public void innerBeforeRun() {
        if (entryProcessor instanceof HazelcastInstanceAware) {
            ((HazelcastInstanceAware) entryProcessor).setHazelcastInstance(getNodeEngine().getHazelcastInstance());
        }
    }

    @Override
    public void run() {
        Map.Entry<Data, Object> entry = getEntry(dataKey);

        final Object key = toObject(entry.getKey());
        final Object value = toObject(entry.getValue());

        entry = createMapEntry(key, value);

        process(entry);

        if (entryRemoved(entry)) {
            return;
        }

        if (entryAddedOrUpdated(entry)) {
            return;
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

    private void process(Map.Entry entry) {
        entryProcessor.processBackup(entry);
    }

    private boolean entryRemoved(Map.Entry entry) {
        final Object value = entry.getValue();
        if (value == null) {
            recordStore.removeBackup(dataKey);
            return true;
        }
        return false;
    }

    private boolean entryAddedOrUpdated(Map.Entry entry) {
        final Object value = entry.getValue();
        if (value != null) {
            recordStore.putBackup(dataKey, value);
            return true;
        }
        return false;
    }


    private Map.Entry createMapEntry(Object key, Object value) {
        return new MapEntrySimple(key, value);
    }

    private Map.Entry<Data, Object> getEntry(Data key) {
        return recordStore.getMapEntryForBackup(key);
    }

    private Object toObject(Object data) {
        final MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        return mapServiceContext.toObject(data);
    }

}
