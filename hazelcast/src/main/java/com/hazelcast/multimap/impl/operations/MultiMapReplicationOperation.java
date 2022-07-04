/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.multimap.impl.operations;

import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.multimap.impl.MultiMapDataSerializerHook;
import com.hazelcast.multimap.impl.MultiMapRecord;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.multimap.impl.MultiMapValue;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.hazelcast.internal.util.MapUtil.createHashMap;
import static com.hazelcast.internal.util.SetUtil.createHashSet;

public class MultiMapReplicationOperation extends Operation implements IdentifiedDataSerializable {

    private Map<String, Map<Data, MultiMapValue>> map;

    public MultiMapReplicationOperation() {
    }

    public MultiMapReplicationOperation(Map<String, Map<Data, MultiMapValue>> map) {
        this.map = map;
    }

    @Override
    public void run() throws Exception {
        MultiMapService service = getService();
        service.insertMigratedData(getPartitionId(), map);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeInt(map.size());
        for (Map.Entry<String, Map<Data, MultiMapValue>> entry : map.entrySet()) {
            String name = entry.getKey();
            out.writeString(name);

            Map<Data, MultiMapValue> collections = entry.getValue();
            out.writeInt(collections.size());
            for (Map.Entry<Data, MultiMapValue> collectionEntry : collections.entrySet()) {
                Data key = collectionEntry.getKey();
                IOUtil.writeData(out, key);
                MultiMapValue multiMapValue = collectionEntry.getValue();
                Collection<MultiMapRecord> coll = multiMapValue.getCollection(false);
                out.writeInt(coll.size());
                String collectionType = MultiMapConfig.ValueCollectionType.SET.name();
                if (coll instanceof List) {
                    collectionType = MultiMapConfig.ValueCollectionType.LIST.name();
                }
                out.writeString(collectionType);
                for (MultiMapRecord record : coll) {
                    record.writeData(out);
                }
            }
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        int mapSize = in.readInt();
        map = createHashMap(mapSize);
        for (int i = 0; i < mapSize; i++) {
            String name = in.readString();
            int collectionSize = in.readInt();
            Map<Data, MultiMapValue> collections = createHashMap(collectionSize);
            for (int j = 0; j < collectionSize; j++) {
                Data key = IOUtil.readData(in);
                int collSize = in.readInt();
                String collectionType = in.readString();
                Collection<MultiMapRecord> coll;
                if (collectionType.equals(MultiMapConfig.ValueCollectionType.SET.name())) {
                    coll = createHashSet(collSize);
                } else {
                    coll = new LinkedList<>();
                }
                for (int k = 0; k < collSize; k++) {
                    MultiMapRecord record = new MultiMapRecord();
                    record.readData(in);
                    coll.add(record);
                }
                collections.put(key, new MultiMapValue(coll));

            }
            map.put(name, collections);
        }
    }

    @Override
    public int getFactoryId() {
        return MultiMapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MultiMapDataSerializerHook.MULTIMAP_REPLICATION_OPERATION;
    }
}
