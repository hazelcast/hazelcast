/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.impl.map;

import com.hazelcast.impl.DefaultRecord;
import com.hazelcast.impl.Record;
import com.hazelcast.impl.base.DataRecordEntry;
import com.hazelcast.impl.spi.AbstractOperation;
import com.hazelcast.impl.spi.NodeService;
import com.hazelcast.impl.spi.ServiceMigrationOperation;
import com.hazelcast.nio.Data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
* @mdogan 7/24/12
*/
public class MapMigrationOperation extends AbstractOperation implements ServiceMigrationOperation {

    private Map<String, Map<Data, DataRecordEntry>> data ;
    private Map<String, Map<Data, Record>> buffer ;
    private int partitionId;
    private int replicaIndex;
    private boolean diff;

    public MapMigrationOperation() {
    }

    public MapMigrationOperation(PartitionContainer container, int partitionId, int replicaIndex, boolean diff) {
        this.partitionId = partitionId;
        this.replicaIndex = replicaIndex;
        this.diff = diff;
        data = new HashMap<String, Map<Data, DataRecordEntry>>(container.maps.size());
        for (Entry<String, MapPartition> entry : container.maps.entrySet()) {
            String name = entry.getKey();
            MapPartition partition = entry.getValue();
            Map<Data, DataRecordEntry> map = new HashMap<Data, DataRecordEntry>(partition.records.size());
            for (Entry<Data, Record> recordEntry : partition.records.entrySet()) {
                map.put(recordEntry.getKey(), new DataRecordEntry(recordEntry.getValue(), true));
            }
            data.put(name, map);
        }
    }

    public void run() {
        if (data == null) {
            getOperationContext().getResponseHandler().sendResponse(Boolean.FALSE);
            return;
        }
        NodeService nodeService = getOperationContext().getNodeService();
        MapService mapService = (MapService) getOperationContext().getService();
        buffer = new HashMap<String, Map<Data, Record>>(data.size());
        for (Entry<String, Map<Data, DataRecordEntry>> dataEntry : data.entrySet()) {
            Map<Data, DataRecordEntry> dataMap = dataEntry.getValue();
            Map<Data, Record> map = new HashMap<Data, Record>(dataMap.size());
            for (Entry<Data, DataRecordEntry> entry : dataMap.entrySet()) {
                final DataRecordEntry recordEntry = entry.getValue();
                Record record = new DefaultRecord(null, nodeService.getPartitionId(recordEntry.getKeyData()),
                        recordEntry.getKeyData(), recordEntry.getValueData(), -1, -1, mapService.nextId());
                map.put(entry.getKey(), record);
            }
            buffer.put(dataEntry.getKey(), map);
        }
        getOperationContext().getResponseHandler().sendResponse(Boolean.TRUE);
    }

    public void onSuccess() {
        MapService mapService = (MapService) getOperationContext().getService();
        PartitionContainer container = mapService.getPartitionContainer(partitionId);
        for (Entry<String, Map<Data, Record>> entry : buffer.entrySet()) {
            MapPartition partition = container.getMapPartition(entry.getKey());
            partition.records.putAll(entry.getValue());
            System.err.println(entry.getKey() + "  HEYOOOOO !!!!! " + entry.getValue().size());
        }
        clear();
    }

    public void onError() {
        clear();
    }

    private void clear() {
        if (buffer != null) {
            buffer.clear();
        }
        if (data != null) {
            data.clear();
        }
    }

    public String getServiceName() {
        return MapService.MAP_SERVICE_NAME;
    }

    public void readData(final DataInput in) throws IOException {
        super.readData(in);
        partitionId = in.readInt();
        replicaIndex = in.readInt();
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

    public void writeData(final DataOutput out) throws IOException {
        super.writeData(out);
        out.writeInt(partitionId);
        out.writeInt(replicaIndex);
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
