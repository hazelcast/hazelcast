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

package com.hazelcast.multimap.impl.operations;

import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.multimap.impl.MultiMapRecord;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.multimap.impl.MultiMapWrapper;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractOperation;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class MultiMapMigrationOperation extends AbstractOperation {

    Map<String, Map> map;

    public MultiMapMigrationOperation() {
    }

    public MultiMapMigrationOperation(Map<String, Map> map) {
        this.map = map;
    }

    public void run() throws Exception {
        MultiMapService service = getService();
        service.insertMigratedData(getPartitionId(), map);
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeInt(map.size());
        for (Map.Entry<String, Map> entry : map.entrySet()) {
            String name = entry.getKey();
            out.writeUTF(name);

            Map<Data, MultiMapWrapper> collections = entry.getValue();
            out.writeInt(collections.size());
            for (Map.Entry<Data, MultiMapWrapper> collectionEntry : collections.entrySet()) {
                Data key = collectionEntry.getKey();
                out.writeData(key);
                MultiMapWrapper wrapper = collectionEntry.getValue();
                Collection<MultiMapRecord> coll = wrapper.getCollection(false);
                out.writeInt(coll.size());
                String collectionType = MultiMapConfig.ValueCollectionType.SET.name();
                if (coll instanceof List) {
                    collectionType = MultiMapConfig.ValueCollectionType.LIST.name();
                }
                out.writeUTF(collectionType);
                for (MultiMapRecord record : coll) {
                    record.writeData(out);
                }
            }
        }
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        int mapSize = in.readInt();
        map = new HashMap<String, Map>(mapSize);
        for (int i = 0; i < mapSize; i++) {
            String name = in.readUTF();
            int collectionSize = in.readInt();
            Map<Data, MultiMapWrapper> collections = new HashMap<Data, MultiMapWrapper>();
            for (int j = 0; j < collectionSize; j++) {
                Data key = in.readData();
                int collSize = in.readInt();
                String collectionType = in.readUTF();
                Collection<MultiMapRecord> coll;
                if (collectionType.equals(MultiMapConfig.ValueCollectionType.SET.name())) {
                    coll = new HashSet<MultiMapRecord>();
                } else {
                    coll = new LinkedList<MultiMapRecord>();
                }
                for (int k = 0; k < collSize; k++) {
                    MultiMapRecord record = new MultiMapRecord();
                    record.readData(in);
                    coll.add(record);
                }
                collections.put(key, new MultiMapWrapper(coll));

            }
            map.put(name, collections);
        }
    }


}
