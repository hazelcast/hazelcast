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
import com.hazelcast.lock.LockInfo;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractOperation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * @mdogan 7/24/12
 */
public class MapMigrationOperation extends AbstractOperation {

    private Map<String, Map<Data, Record>> data;
    private Map<String, Map<Data, LockInfo>> locks;

    public MapMigrationOperation() {
    }

    public MapMigrationOperation(PartitionContainer container, int partitionId, int replicaIndex) {
        this.setPartitionId(partitionId).setReplicaIndex(replicaIndex);
        data = new HashMap<String, Map<Data, Record>>(container.maps.size());
        locks = new HashMap<String, Map<Data, LockInfo>>(container.maps.size());
        for (Entry<String, DefaultRecordStore> entry : container.maps.entrySet()) {
            String name = entry.getKey();
            final MapConfig mapConfig = entry.getValue().getMapContainer().getMapConfig();
            if (mapConfig.getTotalBackupCount() < replicaIndex) {
                continue;
            }
            RecordStore recordStore = entry.getValue();
            Map<Data, Record> map = new HashMap<Data, Record>(recordStore.getRecords().size());
            for (Entry<Data, Record> recordEntry : recordStore.getRecords().entrySet()) {
                map.put(recordEntry.getKey(), recordEntry.getValue());
            }
            data.put(name, map);
            Map<Data, LockInfo> lockmap = new HashMap<Data, LockInfo>();
            for (Entry<Data, LockInfo> lockEntry : recordStore.getLocks().entrySet()) {
                lockmap.put(lockEntry.getKey(), lockEntry.getValue());
            }
            locks.put(name, lockmap);
        }
    }

    public void run() {
        MapService mapService = (MapService) getService();
        if (data != null) {
            for (Entry<String, Map<Data, Record>> dataEntry : data.entrySet()) {
                Map<Data, Record> dataMap = dataEntry.getValue();
                final String mapName = dataEntry.getKey();
                RecordStore recordStore = mapService.getRecordStore(getPartitionId(), mapName);
                for (Entry<Data, Record> entry : dataMap.entrySet()) {
                    final Record recordEntry = entry.getValue();
                    Record record = mapService.createRecord(mapName, recordEntry.getKey(), recordEntry.getValue(), -1);
                    record.setState(recordEntry.getState());
                    record.setStats(recordEntry.getStats());
                    recordStore.getRecords().put(entry.getKey(), record);
                }
            }
        }
        if (locks != null) {
            for (Entry<String, Map<Data, LockInfo>> entry : locks.entrySet()) {
                String mapName = entry.getKey();
                RecordStore recordStore = mapService.getRecordStore(getPartitionId(), mapName);
                for (Entry<Data, LockInfo> lockEntry : entry.getValue().entrySet()) {
                    recordStore.putLock(lockEntry.getKey(), lockEntry.getValue());
                }
            }
        }
    }

    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    protected void readInternal(final ObjectDataInput in) throws IOException {
        int size = in.readInt();
        data = new HashMap<String, Map<Data, Record>>(size);
        for (int i = 0; i < size; i++) {
            String name = in.readUTF();
            int mapSize = in.readInt();
            Map<Data, Record> map = new HashMap<Data, Record>(mapSize);
            for (int j = 0; j < mapSize; j++) {
                Data key = new Data();
                key.readData(in);
                Record recordEntry = in.readObject();
                map.put(key, recordEntry);
            }
            data.put(name, map);
        }
        int lsize = in.readInt();
        locks = new HashMap<String, Map<Data, LockInfo>>(lsize);
        for (int i = 0; i < lsize; i++) {
            String name = in.readUTF();
            int mapSize = in.readInt();
            Map<Data, LockInfo> map = new HashMap<Data, LockInfo>(lsize);
            for (int j = 0; j < mapSize; j++) {
                Data key = new Data();
                key.readData(in);
                LockInfo lockInfo = IOUtil.readNullableObject(in);
                map.put(key, lockInfo);
            }
            locks.put(name, map);
        }
    }

    protected void writeInternal(final ObjectDataOutput out) throws IOException {
        out.writeInt(data.size());
        for (Entry<String, Map<Data, Record>> mapEntry : data.entrySet()) {
            out.writeUTF(mapEntry.getKey());
            Map<Data, Record> map = mapEntry.getValue();
            out.writeInt(map.size());
            for (Entry<Data, Record> entry : map.entrySet()) {
                entry.getKey().writeData(out);
                out.writeObject(entry.getValue());
            }
        }
        out.writeInt(locks.size());
        for (Entry<String, Map<Data, LockInfo>> mapEntry : locks.entrySet()) {
            out.writeUTF(mapEntry.getKey());
            Map<Data, LockInfo> map = mapEntry.getValue();
            out.writeInt(map.size());
            for (Entry<Data, LockInfo> entry : map.entrySet()) {
                entry.getKey().writeData(out);
                IOUtil.writeNullableObject(out, entry.getValue());
            }
        }
    }

    public boolean isEmpty() {
        return (data == null || data.isEmpty()) && (locks == null || locks.isEmpty());
    }
}