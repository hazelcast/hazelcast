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

package com.hazelcast.queue.impl.operations;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.queue.impl.QueueDataSerializerHook;
import com.hazelcast.spi.BackupOperation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Provides backup functionality for {@link AddAllOperation}
 */
public class AddAllBackupOperation extends QueueOperation implements BackupOperation {

    private Map<Long, Data> dataMap;

    public AddAllBackupOperation() {
    }

    public AddAllBackupOperation(String name, Map<Long, Data> dataMap) {
        super(name);
        this.dataMap = dataMap;
    }

    @Override
    public void run() throws Exception {
        getOrCreateContainer().addAllBackup(dataMap);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(dataMap.size());
        for (Map.Entry<Long, Data> entry : dataMap.entrySet()) {
            long itemId = entry.getKey();
            Data value = entry.getValue();
            out.writeLong(itemId);
            value.writeData(out);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        dataMap = new HashMap<Long, Data>(size);
        for (int i = 0; i < size; i++) {
            long itemId = in.readLong();
            Data value = new Data();
            value.readData(in);
            dataMap.put(itemId, value);
        }
    }

    @Override
    public int getId() {
        return QueueDataSerializerHook.ADD_ALL_BACKUP;
    }
}
