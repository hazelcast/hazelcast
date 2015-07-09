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

import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.RecordStore;
import com.hazelcast.map.impl.mapstore.MapDataStore;
import com.hazelcast.map.impl.mapstore.writebehind.DefaultSequencer;
import com.hazelcast.map.impl.mapstore.writebehind.Sequencer;
import com.hazelcast.map.impl.mapstore.writebehind.WriteBehindQueue;
import com.hazelcast.map.impl.mapstore.writebehind.WriteBehindStore;
import com.hazelcast.map.impl.mapstore.writebehind.entry.DelayedEntry;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * This operation is sent from the owner-node to backup-nodes upon store of entries to the map-store.
 * Main goal is to remove processed entries from backup nodes only if the owner has already stored them.
 */
public class WriteBehindCleanBackupOperation extends AbstractMapOperation implements PartitionAwareOperation {

    private Sequencer sequencer;

    public WriteBehindCleanBackupOperation() {
    }

    public WriteBehindCleanBackupOperation(String mapName, Sequencer sequencer) {
        super(checkNotNull(mapName, "mapName cannot be null"));
        this.sequencer = checkNotNull(sequencer, "sequencer cannot be null");
    }

    @Override
    public void run() throws Exception {
        WriteBehindStore store = getWriteBehindStoreOrNull();
        if (store == null) {
            ILogger logger = getNodeEngine().getLogger(getClass());
            logger.severe("Normally this should not be happen. All nodes should have the same map-store configuration.");
            return;
        }

        WriteBehindQueue<DelayedEntry> writeBehindQueue = store.getWriteBehindQueue();

        Deque<DelayedEntry> entries = new ArrayDeque<DelayedEntry>();

        writeBehindQueue.getFrontBySequence(sequencer.headSequence(), entries);
        removeEntries(entries);

        writeBehindQueue.getEndBySequence(sequencer.tailSequence(), entries);
        removeEntries(entries);

    }

    private void removeEntries(Deque<DelayedEntry> entries) {
        if (entries.isEmpty()) {
            return;
        }
        WriteBehindStore store = getWriteBehindStoreOrNull();
        WriteBehindQueue<DelayedEntry> writeBehindQueue = store.getWriteBehindQueue();

        do {
            DelayedEntry next = entries.poll();
            if (next == null) {
                break;
            }
            writeBehindQueue.removeFirstOccurrence(next);
            store.removeFromStagingArea(next);
        } while (true);
    }

    private WriteBehindStore getWriteBehindStoreOrNull() {
        int partitionId = getPartitionId();
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        PartitionContainer partitionContainer = mapServiceContext.getPartitionContainer(partitionId);
        RecordStore recordStore = partitionContainer.getExistingRecordStore(name);
        if (recordStore == null) {
            return null;
        }

        MapDataStore<Data, Object> mapDataStore = recordStore.getMapDataStore();
        if (!(mapDataStore instanceof WriteBehindStore)) {
            return null;
        }

        return (WriteBehindStore) mapDataStore;
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);

        sequencer.writeData(out);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);

        Sequencer sequencer = new DefaultSequencer();
        sequencer.readData(in);
    }
}
