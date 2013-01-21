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

package com.hazelcast.collection;

import com.hazelcast.map.LockInfo;
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

    Map<CollectionProxyId, Map[]> map;

    public CollectionMigrationOperation() {
    }

    public CollectionMigrationOperation(Map<CollectionProxyId, Map[]> map) {
        this.map = map;
    }

    public void run() throws Exception {
        CollectionService service = getService();
        service.insertMigratedData(getPartitionId(), map);
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeInt(map.size());
        for (Map.Entry<CollectionProxyId, Map[]> entry : map.entrySet()) {
            CollectionProxyId proxyId = entry.getKey();
            proxyId.writeData(out);

            Map<Data, Object> objects = entry.getValue()[0];
            out.writeInt(objects.size());
            for (Map.Entry<Data, Object> objectEntry : objects.entrySet()) {
                Data key = objectEntry.getKey();
                key.writeData(out);
                Object object = objectEntry.getValue();
                if (object instanceof Collection) {
                    out.writeBoolean(true);
                    Collection coll = (Collection) object;
                    out.writeInt(coll.size());
                    for (Object obj : coll) {
                        out.writeObject(obj);
                    }
                } else {
                    out.writeBoolean(false);
                    out.writeObject(object);
                }
            }


            Map<Data, LockInfo> locks = entry.getValue()[1];
            out.writeInt(locks.size());
            for (Map.Entry<Data, LockInfo> lockEntry : locks.entrySet()) {
                Data key = lockEntry.getKey();
                key.writeData(out);
                LockInfo lockInfo = lockEntry.getValue();
                lockInfo.writeData(out);
            }
        }
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        int mapSize = in.readInt();
        map = new HashMap<CollectionProxyId, Map[]>(mapSize);
        for (int i = 0; i < mapSize; i++) {
            CollectionProxyId proxyId = new CollectionProxyId();
            proxyId.readData(in);
            int objectSize = in.readInt();
            Map<Data, Object> objects = new HashMap<Data, Object>();
            for (int j = 0; j < objectSize; j++) {
                Data key = new Data();
                key.readData(in);
                boolean isCollection = in.readBoolean();
                if (isCollection){
                    int collSize = in.readInt();
                    Collection coll = new ArrayList(collSize);
                    for (int k = 0; k < collSize; k++) {
                        Object obj = in.readObject();
                        coll.add(obj);
                    }
                    objects.put(key, coll);
                }
                else {
                    objects.put(key, in.readObject());
                }
            }
            int lockSize = in.readInt();
            Map<Data, LockInfo> locks = new HashMap<Data, LockInfo>(lockSize);
            for (int j = 0; j < lockSize; j++) {
                Data key = new Data();
                key.readData(in);
                LockInfo lockInfo = new LockInfo();
                lockInfo.readData(in);
                locks.put(key, lockInfo);
            }
            map.put(proxyId, new Map[]{objects, locks});
        }
    }


}
