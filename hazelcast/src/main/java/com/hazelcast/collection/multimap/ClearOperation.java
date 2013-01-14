/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection.multimap;

import com.hazelcast.collection.CollectionContainer;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionLevelOperation;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * @ali 1/9/13
 */
public class ClearOperation extends MultiMapOperation implements BackupAwareOperation, PartitionLevelOperation {

    int syncBackupCount;

    int asyncBackupCount;

    transient Map<Data, Collection> entries;

    public ClearOperation() {
    }

    public ClearOperation(String name, int syncBackupCount, int asyncBackupCount) {
        super(name);
        this.syncBackupCount = syncBackupCount;
        this.asyncBackupCount = asyncBackupCount;
    }

    public void beforeRun() throws Exception {
        if (hasListener()){
            CollectionContainer container = getContainer();
            if (container != null){
                entries = container.entrySet();
            }
        }
    }

    public void run() throws Exception {
        CollectionContainer container = getContainer();
        if (container != null){
            container.clear();
        }
        response = true;
    }

    public void afterRun() throws Exception {
        if (entries != null){
            for (Map.Entry<Data, Collection> entry: entries.entrySet()){
                Data key = entry.getKey();
                Collection coll = entry.getValue();
                for (Object obj: coll){
                    publishEvent(EntryEventType.REMOVED, key, obj);
                }
            }
            entries.clear();
            entries = null;
        }
    }

    public boolean shouldBackup() {
        return true;
    }

    public int getSyncBackupCount() {
        return syncBackupCount;
    }

    public int getAsyncBackupCount() {
        return asyncBackupCount;
    }

    public Operation getBackupOperation() {
        return new ClearBackupOperation(name);
    }

    public void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(syncBackupCount);
        out.writeInt(asyncBackupCount);
    }

    public void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        syncBackupCount = in.readInt();
        asyncBackupCount = in.readInt();
    }
}
