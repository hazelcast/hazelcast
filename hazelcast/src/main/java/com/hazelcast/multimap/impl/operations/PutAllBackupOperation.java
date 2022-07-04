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

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.impl.DataCollection;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.multimap.impl.MultiMapContainer;
import com.hazelcast.multimap.impl.MultiMapDataSerializerHook;
import com.hazelcast.multimap.impl.MultiMapRecord;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.BackupOperation;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

public class PutAllBackupOperation extends AbstractMultiMapOperation implements BackupOperation {
    private transient int currentIndex;
    private MapEntries mapEntries;

    public PutAllBackupOperation() {
    }

    public PutAllBackupOperation(String name, MapEntries mapEntries) {
        super(name);
        this.mapEntries = mapEntries;
    }

    @Override
    public void run() throws Exception {
        int size = mapEntries != null ? mapEntries.size() : 0;
        while (currentIndex < size) {
            Data dataKey = mapEntries.getKey(currentIndex);
            Data value = mapEntries.getValue(currentIndex);
            put(dataKey, value);
            currentIndex++;
        }
    }

    protected void put(Data dataKey, Data dataValue) {
        MultiMapContainer container = getOrCreateContainer();
        Collection<Data> c = ((DataCollection) toObject(dataValue)).getCollection();
        Collection<MultiMapRecord> coll = container.getOrCreateMultiMapValue(dataKey).getCollection(false);
        Iterator<Data> it = c.iterator();

        while (it.hasNext()) {
            Data o = it.next();
            MultiMapRecord record = new MultiMapRecord(container.nextId(), isBinary() ? o : toObject(o));
            if (coll.add(record)) {
                response = true;
            }
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(mapEntries);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        mapEntries = in.readObject();
    }

    @Override
    public int getClassId() {
        return MultiMapDataSerializerHook.PUT_ALL_BACKUP;
    }
}
