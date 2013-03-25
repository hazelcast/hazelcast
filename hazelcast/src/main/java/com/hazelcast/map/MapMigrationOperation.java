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

import com.hazelcast.config.MapConfig;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractOperation;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

/**
 * @mdogan 7/24/12
 */
public class MapMigrationOperation extends AbstractOperation {

    // TODO: @mm - simplify please...
    private Map<String, Set<Record>> data;

    public MapMigrationOperation() {
    }

    public MapMigrationOperation(PartitionContainer container, int partitionId, int replicaIndex) {
        this.setPartitionId(partitionId).setReplicaIndex(replicaIndex);
        data = new HashMap<String, Set<Record>>(container.maps.size());
        for (Entry<String, PartitionRecordStore> entry : container.maps.entrySet()) {
            String name = entry.getKey();
            final MapConfig mapConfig = entry.getValue().getMapContainer().getMapConfig();
            if (mapConfig.getTotalBackupCount() < replicaIndex) {
                continue;
            }
            RecordStore recordStore = entry.getValue();
            Set<Record> recordSet = new HashSet<Record>(recordStore.getRecords().size());
            for (Entry<Data, Record> recordEntry : recordStore.getRecords().entrySet()) {
                recordSet.add(recordEntry.getValue());
            }
            data.put(name, recordSet);
        }
    }

    public void run() {
        MapService mapService = (MapService) getService();
        if (data != null) {
            for (Entry<String, Set<Record>> dataEntry : data.entrySet()) {
                Set<Record> recordSet = dataEntry.getValue();
                final String mapName = dataEntry.getKey();
                RecordStore recordStore = mapService.getRecordStore(getPartitionId(), mapName);
                for (Record recordEntry: recordSet) {
                    Record record = mapService.createRecord(mapName, recordEntry.getKey(), recordEntry.getValue(), -1);
                    record.setState(recordEntry.getState());
                    record.setStatistics(recordEntry.getStatistics());
                    recordStore.getRecords().put(recordEntry.getKey(), record);
                }
            }
        }
    }

    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    protected void readInternal(final ObjectDataInput in) throws IOException {
        int size = in.readInt();
        data = new HashMap<String, Set<Record>>(size);
        for (int i = 0; i < size; i++) {
            String name = in.readUTF();
            int mapSize = in.readInt();
            Set<Record> recordSet = new HashSet<Record>(mapSize);
            for (int j = 0; j < mapSize; j++) {
                Data key = new Data();
                key.readData(in);
                Record recordEntry = in.readObject();
                recordSet.add(recordEntry);
            }
            data.put(name, recordSet);
        }
    }

    protected void writeInternal(final ObjectDataOutput out) throws IOException {
        out.writeInt(data.size());
        for (Entry<String, Set<Record>> mapEntry : data.entrySet()) {
            out.writeUTF(mapEntry.getKey());
            Set<Record> recordSet = mapEntry.getValue();
            out.writeInt(recordSet.size());
            for (Record record : recordSet) {
                record.getKey().writeData(out);
                out.writeObject(record);
            }
        }
    }

    public boolean isEmpty() {
        return data == null || data.isEmpty();
    }
}