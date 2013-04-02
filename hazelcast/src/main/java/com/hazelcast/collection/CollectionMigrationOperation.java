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

package com.hazelcast.collection;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * @ali 1/18/13
 */
public class CollectionMigrationOperation extends AbstractOperation {

    Map<CollectionProxyId, Map> map;

    public CollectionMigrationOperation() {
    }

    public CollectionMigrationOperation(Map<CollectionProxyId, Map> map) {
        this.map = map;
    }

    public void run() throws Exception {
        CollectionService service = getService();
        service.insertMigratedData(getPartitionId(), map);
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeInt(map.size());
        for (Map.Entry<CollectionProxyId, Map> entry : map.entrySet()) {
            CollectionProxyId proxyId = entry.getKey();
            proxyId.writeData(out);

            Map<Data, CollectionWrapper> collections = entry.getValue();
            out.writeInt(collections.size());
            for (Map.Entry<Data, CollectionWrapper> collectionEntry : collections.entrySet()) {
                Data key = collectionEntry.getKey();
                key.writeData(out);
                CollectionWrapper wrapper = collectionEntry.getValue();
                Collection<CollectionRecord> coll = wrapper.getCollection();
                out.writeInt(coll.size());
                for (CollectionRecord record : coll) {
                    record.writeData(out);
                }
            }
        }
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        int mapSize = in.readInt();
        map = new HashMap<CollectionProxyId, Map>(mapSize);
        for (int i = 0; i < mapSize; i++) {
            CollectionProxyId proxyId = new CollectionProxyId();
            proxyId.readData(in);
            int collectionSize = in.readInt();
            Map<Data, CollectionWrapper> collections = new HashMap<Data, CollectionWrapper>();
            for (int j = 0; j < collectionSize; j++) {
                Data key = new Data();
                key.readData(in);
                int collSize = in.readInt();
                Collection<CollectionRecord> coll = new ArrayList<CollectionRecord>(collSize);
                for (int k = 0; k < collSize; k++) {
                    CollectionRecord record = new CollectionRecord();
                    record.readData(in);
                    coll.add(record);
                }
                collections.put(key, new CollectionWrapper(coll));

            }
            map.put(proxyId, collections);
        }
    }


}
