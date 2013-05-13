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

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Map;

public class EntryOperation extends LockAwareOperation implements BackupAwareOperation {

    EntryProcessor entryProcessor;

    transient Object response;
    transient Map.Entry entry;

    public EntryOperation(String name, Data dataKey, EntryProcessor entryProcessor) {
        super(name, dataKey);
        this.entryProcessor = entryProcessor;
    }

    public EntryOperation() {
    }

    public void run() {
        Map.Entry<Data, Object> mapEntry = recordStore.getMapEntryObject(dataKey);
        entry = new AbstractMap.SimpleEntry(mapService.toObject(dataKey), mapService.toObject(mapEntry.getValue()));
        response = mapService.toData(entryProcessor.process(entry));
        recordStore.put(new AbstractMap.SimpleImmutableEntry<Data, Object>(dataKey, entry.getValue()));
    }

    public void afterRun() throws Exception {
        super.afterRun();
        invalidateNearCaches();
    }

    @Override
    public void onWaitExpire() {
        getResponseHandler().sendResponse(null);
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
        return response;
    }

    @Override
    public String toString() {
        return "EntryOperation{}";
    }

    public Operation getBackupOperation() {
        return new EntryBackupOperation(name, dataKey, entryProcessor.getBackupProcessor());
    }

    public boolean shouldBackup() {
        return entryProcessor.getBackupProcessor() != null;
    }

    public int getAsyncBackupCount() {
        return mapContainer.getAsyncBackupCount();
    }

    public int getSyncBackupCount() {
        return mapContainer.getBackupCount();
    }

}
