/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.RecordStore;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;
import java.io.IOException;

/**
 * Operation which evicts all keys except locked ones.
 */
public class EvictAllOperation extends AbstractMapOperation implements BackupAwareOperation, PartitionAwareOperation {

    private boolean shouldRunOnBackup;

    private int numberOfEvictedEntries;

    public EvictAllOperation() {
    }

    public EvictAllOperation(String name) {
        super(name);
    }

    @Override
    public void run() throws Exception {

        // TODO this also clears locked keys from near cache which should be preserved.
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        mapServiceContext.getNearCacheProvider().clearNearCache(name);

        final RecordStore recordStore = mapServiceContext.getExistingRecordStore(getPartitionId(), name);
        if (recordStore == null) {
            return;
        }
        numberOfEvictedEntries = recordStore.evictAll(false);
        shouldRunOnBackup = true;
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
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        return mapServiceContext.getMapContainer(name).getBackupCount();
    }

    @Override
    public int getAsyncBackupCount() {
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
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
    public String toString() {
        return "EvictAllOperation{"
                + "shouldRunOnBackup=" + shouldRunOnBackup
                + ", numberOfEvictedEntries=" + numberOfEvictedEntries
                + '}';
    }
}
