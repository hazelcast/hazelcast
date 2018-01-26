/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection.impl.queue.operations;

import com.hazelcast.collection.impl.queue.QueueContainer;
import com.hazelcast.collection.impl.queue.QueueDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.impl.MutatingOperation;

import java.io.IOException;
import java.util.Map;

import static com.hazelcast.util.MapUtil.createHashMap;

/**
 * Provides backup functionality for {@link AddAllOperation}
 */
public class AddAllBackupOperation extends QueueOperation implements BackupOperation, MutatingOperation {

    private Map<Long, Data> dataMap;

    public AddAllBackupOperation() {
    }

    public AddAllBackupOperation(String name, Map<Long, Data> dataMap) {
        super(name);
        this.dataMap = dataMap;
    }

    @Override
    public void run() throws Exception {
        QueueContainer queueContainer = getContainer();
        queueContainer.addAllBackup(dataMap);
    }

    @Override
    public int getId() {
        return QueueDataSerializerHook.ADD_ALL_BACKUP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(dataMap.size());
        for (Map.Entry<Long, Data> entry : dataMap.entrySet()) {
            long itemId = entry.getKey();
            Data value = entry.getValue();
            out.writeLong(itemId);
            out.writeData(value);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        dataMap = createHashMap(size);
        for (int i = 0; i < size; i++) {
            long itemId = in.readLong();
            Data value = in.readData();
            dataMap.put(itemId, value);
        }
    }
}
