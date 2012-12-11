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

package com.hazelcast.map;

import com.hazelcast.config.MapConfig;
import com.hazelcast.impl.DefaultRecord;
import com.hazelcast.impl.Record;
import com.hazelcast.impl.base.DataRecordEntry;
import com.hazelcast.nio.Data;
import com.hazelcast.spi.AbstractOperation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * @mdogan 7/24/12
 */
public class MapMigrationOperation extends AbstractOperation {

    private Map<String, Map<Data, DataRecordEntry>> data;
    private boolean diff;

    public MapMigrationOperation() {
    }

    public MapMigrationOperation(PartitionContainer container, int partitionId, int replicaIndex, boolean diff) {
        this.setPartitionId(partitionId).setReplicaIndex(replicaIndex);
        this.diff = diff;
        data = new HashMap<String, Map<Data, DataRecordEntry>>(container.maps.size());
        for (Entry<String, MapPartition> entry : container.maps.entrySet()) {
            String name = entry.getKey();
            final MapConfig mapConfig = container.getMapConfig(name);
            if (mapConfig.getTotalBackupCount() < replicaIndex) {
                continue;
            }

            MapPartition mapPartition = entry.getValue();
            Map<Data, DataRecordEntry> map = new HashMap<Data, DataRecordEntry>(mapPartition.records.size());
            for (Entry<Data, Record> recordEntry : mapPartition.records.entrySet()) {
                map.put(recordEntry.getKey(), new DataRecordEntry(recordEntry.getValue(), true));
            }
            data.put(name, map);
        }

        // TODO: migrate locks?
    }

    public void run() {
        if (data == null) {
            return;
        }
        MapService mapService = (MapService) getService();
        for (Entry<String, Map<Data, DataRecordEntry>> dataEntry : data.entrySet()) {
            Map<Data, DataRecordEntry> dataMap = dataEntry.getValue();
            final String mapName = dataEntry.getKey();
            MapPartition partition = mapService.getMapPartition(getPartitionId(), mapName);
            for (Entry<Data, DataRecordEntry> entry : dataMap.entrySet()) {
                final DataRecordEntry recordEntry = entry.getValue();
                Record record = new DefaultRecord(getPartitionId(),
                        recordEntry.getKeyData(), recordEntry.getValueData(), -1, -1, mapService.nextId());
                partition.records.put(entry.getKey(), record);
            }
        }
        // TODO: migrate locks?
    }

    public String getServiceName() {
        return MapService.MAP_SERVICE_NAME;
    }

    public void readInternal(final DataInput in) throws IOException {
        diff = in.readBoolean();
        int size = in.readInt();
        data = new HashMap<String, Map<Data, DataRecordEntry>>(size);
        for (int i = 0; i < size; i++) {
            String name = in.readUTF();
            int mapSize = in.readInt();
            Map<Data, DataRecordEntry> map = new HashMap<Data, DataRecordEntry>(mapSize);
            for (int j = 0; j < mapSize; j++) {
                Data data = new Data();
                data.readData(in);
                DataRecordEntry recordEntry = new DataRecordEntry();
                recordEntry.readData(in);
                map.put(data, recordEntry);
            }
            data.put(name, map);
        }
    }

    public void writeInternal(final DataOutput out) throws IOException {
        out.writeBoolean(diff);
        out.writeInt(data.size());
        for (Entry<String, Map<Data, DataRecordEntry>> mapEntry : data.entrySet()) {
            out.writeUTF(mapEntry.getKey());
            Map<Data, DataRecordEntry> map = mapEntry.getValue();
            out.writeInt(map.size());
            for (Entry<Data, DataRecordEntry> entry : map.entrySet()) {
                entry.getKey().writeData(out);
                entry.getValue().writeData(out);
            }
        }
    }
}
