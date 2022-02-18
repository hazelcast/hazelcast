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

package com.hazelcast.collection.impl.collection.operations;

import com.hazelcast.collection.impl.collection.CollectionContainer;
import com.hazelcast.collection.impl.collection.CollectionDataSerializerHook;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.BackupOperation;

import java.io.IOException;
import java.util.Map;

import static com.hazelcast.internal.util.MapUtil.createHashMap;

public class CollectionAddAllBackupOperation extends CollectionOperation implements BackupOperation {

    protected Map<Long, Data> valueMap;

    public CollectionAddAllBackupOperation() {
    }

    public CollectionAddAllBackupOperation(String name, Map<Long, Data> valueMap) {
        super(name);
        this.valueMap = valueMap;
    }

    @Override
    public void run() throws Exception {
        CollectionContainer collectionContainer = getOrCreateContainer();
        collectionContainer.addAllBackup(valueMap);
    }

    @Override
    public int getClassId() {
        return CollectionDataSerializerHook.COLLECTION_ADD_ALL_BACKUP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(valueMap.size());
        for (Map.Entry<Long, Data> entry : valueMap.entrySet()) {
            out.writeLong(entry.getKey());
            IOUtil.writeData(out, entry.getValue());
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        final int size = in.readInt();
        valueMap = createHashMap(size);
        for (int i = 0; i < size; i++) {
            final long itemId = in.readLong();
            final Data value = IOUtil.readData(in);
            valueMap.put(itemId, value);
        }
    }
}
