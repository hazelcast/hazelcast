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
import com.hazelcast.spi.BackupOperation;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CollectionAddAllBackupOperation extends CollectionOperation implements BackupOperation {

    protected Map<Long, Data> valueMap;

    public CollectionAddAllBackupOperation() {
    }

    public CollectionAddAllBackupOperation(String name, Map<Long, Data> valueMap) {
        super(name);
        this.valueMap = valueMap;
    }

    @Override
    public int getId() {
        return CollectionDataSerializerHook.COLLECTION_ADD_ALL_BACKUP;
    }

    @Override
    public void beforeRun() throws Exception {

    }

    @Override
    public void run() throws Exception {
        getOrCreateContainer().addAllBackup(valueMap);
    }

    @Override
    public void afterRun() throws Exception {

    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(valueMap.size());
        for (Map.Entry<Long, Data> entry : valueMap.entrySet()) {
            out.writeLong(entry.getKey());
            entry.getValue().writeData(out);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        final int size = in.readInt();
        valueMap = new HashMap<Long, Data>(size);
        for (int i = 0; i < size; i++) {
            final long itemId = in.readLong();
            final Data value = new Data();
            value.readData(in);
            valueMap.put(itemId, value);
        }
    }
}
